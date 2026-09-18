// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "adbc_scan.hpp"
#include "adbc_util.hpp"
#include "duckdb/main/config.hpp"

namespace duckdb {
namespace adbc {

using namespace Private;

AdbcArrowStreamFactory::AdbcArrowStreamFactory(const string &uri, const string &query_text)

    : connection(AdbcConnectionPool::GetEphemeralConnection(uri)), query_text(query_text), table(), delimiter(),
      statement(connection->GetConnection().MakeStatement(query_text)) {
}

AdbcArrowStreamFactory::AdbcArrowStreamFactory(unique_ptr<AdbcPooledConnection> conn,
                                               const string &table,
                                               const string &delimiter)
    : connection(std::move(conn)), query_text("SELECT * FROM " + table), table(table), delimiter(delimiter),
      projection_pushdown(true), statement(connection->GetConnection().MakeStatement(query_text)) {
}

void AdbcArrowStreamFactory::ApplyProjectionPushdown(const vector<string> &columns) {
    if (projection_pushdown) {
        // Append quoted columns
        string new_query_text = "SELECT ";

        // Constant if there are no columns
        if (columns.empty()) {
            new_query_text += "1 ";
        }
        // Otherwise append the column names
        else {
            bool first = true;
            for (auto &col : columns) {
                if (first) {
                    first = false;
                } else {
                    new_query_text += ",";
                }
                new_query_text += (delimiter[0] + col + delimiter[1] + ' ');
            }
        }
        new_query_text += ("FROM " + table);

        // Assign new query text
        query_text = new_query_text;
        ResetStatement();
    }
}

AdbcStatement *AdbcArrowStreamFactory::GetStatement() {
    return statement.get();
}

void AdbcArrowStreamFactory::ResetStatement() {
    statement = connection->GetConnection().MakeStatement(query_text);
}

void AdbcArrowStreamFactory::GetSchema(ArrowSchema &schema) {

    // Retrieve and register the schema information from ADBC with DuckDB
    Handle<Private::AdbcError> error = {};

    // Try running ExecuteSchema(...)
    auto schema_status = AdbcStatementExecuteSchema(statement.get(), &schema, error.get());

    // If it's not available, then execute the query, get the schema, and cancel the query
    if (schema_status == ADBC_STATUS_NOT_IMPLEMENTED) {
        error.reset();
        Handle<ArrowArrayStream> stream = {};
        int64_t rows_affected = 0;
        CHECK_ADBC(AdbcStatementExecuteQuery(statement.get(), stream.get(), &rows_affected, error.get()),
                   BinderException);
        if (stream->get_schema(stream.get(), &schema) != 0) {
            throw BinderException("Failed to get schema from ADBC stream");
        }
        stream.reset();
        ResetStatement();
    } else {
        CHECK_ADBC(schema_status, BinderException);
    }
}

unique_ptr<ArrowArrayStreamWrapper> AdbcArrowStreamFactory::ProduceStream(ArrowStreamParameters &parameters) {

    // Apply projection pushdown
    auto &columns = parameters.projected_columns.columns;
    ApplyProjectionPushdown(columns);

    // Create the stream for the query result
    Handle<Private::AdbcError> error = {};
    ArrowArrayStream adbc_stream = {};
    int64_t rows_affected;
    CHECK_ADBC(AdbcStatementExecuteQuery(statement.get(), &adbc_stream, &rows_affected, error.get()), IOException);

    // Create and return the wrapper owning the stream for DuckDB
    auto wrapper = make_uniq<ArrowArrayStreamWrapper>();
    std::memcpy(&wrapper->arrow_array_stream, &adbc_stream, sizeof(ArrowArrayStream));
    return wrapper;
}

AdbcArrowScanFunctionData::AdbcArrowScanFunctionData(ClientContext &context, shared_ptr<AdbcArrowStreamFactory> factory)
    : ArrowScanFunctionData(factory), adbc_arrow_stream_factory(factory) {
    factory->GetSchema(schema_root.arrow_schema);
    ArrowTableFunction::PopulateArrowTableSchema(context, arrow_table, schema_root.arrow_schema);
}

void AdbcScanFunction(ClientContext &context, TableFunctionInput &input, DataChunk &output) {

    // We closely follow the DuckDB Arrow extension's scan function from:
    // https://github.com/duckdb/arrow/
    if (!input.local_state) {
        return;
    }

    auto &function_data = input.bind_data->CastNoConst<AdbcArrowScanFunctionData>();
    auto &global_state = input.global_state->Cast<ArrowScanGlobalState>();
    auto &local_state = input.local_state->Cast<ArrowScanLocalState>();

    // Need more tuples in the current chunk
    if (local_state.chunk_offset >= static_cast<idx_t>(local_state.chunk->arrow_array.length)) {
        // Fetch them and exit if there are no more tuples left
        if (!ArrowTableFunction::ArrowScanParallelStateNext(context,
                                                            input.bind_data.get(),
                                                            local_state,
                                                            global_state)) {
            return;
        }
    }

    // Compute the number of tuples read (and therefore output size)
    idx_t output_size =
        MinValue<idx_t>(STANDARD_VECTOR_SIZE, local_state.chunk->arrow_array.length - local_state.chunk_offset);
    function_data.lines_read += output_size;

    // Handle the case where we don't need all of the columns
    auto is_projected = function_data.adbc_arrow_stream_factory->CanPushdownProjections();
    if (global_state.CanRemoveFilterColumns() && !is_projected) {
        local_state.all_columns.Reset();
        local_state.all_columns.SetChildCardinality(output_size);

        ArrowTableFunction::ArrowToDuckDB(local_state,
                                          function_data.arrow_table.GetColumns(),
                                          local_state.all_columns,
                                          false);
        output.ReferenceColumns(local_state.all_columns, global_state.projection_ids);
    } else {
        output.SetChildCardinality(output_size);
        ArrowTableFunction::ArrowToDuckDB(local_state, function_data.arrow_table.GetColumns(), output, is_projected);
    }

    output.Verify();
    local_state.chunk_offset += output.size();
}

unique_ptr<FunctionData> AdbcScanBindFunction(ClientContext &context,
                                              TableFunctionBindInput &input,
                                              vector<LogicalType> &return_types,
                                              vector<Identifier> &names) {

    // Validate that the function was provided exactly two input parameters
    if (input.inputs.size() != 2) {
        throw BinderException("read_adbc(...) requires two parameters: (1) the "
                              "adbc URI (2) the SQL query string");
    }

    // Get the input parameters
    auto uri = input.inputs[0].GetValue<string>();
    auto query_text = input.inputs[1].GetValue<string>();

    // Create the factory object which holds the ADBC state for the lifetime of
    // the scan
    auto adbc_arrow_stream_factory = make_uniq<AdbcArrowStreamFactory>(uri, query_text);

    // Create a function data object which registers the ADBC schema with DuckDB
    // and owns the factory for the scan
    auto function_data = make_uniq<AdbcArrowScanFunctionData>(context, std::move(adbc_arrow_stream_factory));

    // Assign the column names and types
    for (auto &name : function_data->arrow_table.GetNames()) {
        names.emplace_back(name);
    }
    return_types = function_data->arrow_table.GetTypes();
    function_data->all_types = return_types;
    return function_data;
}

} // namespace adbc
} // namespace duckdb
