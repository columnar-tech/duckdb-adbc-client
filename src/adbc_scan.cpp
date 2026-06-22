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
    : connection(AdbcConnectionPool::GetEphemeralConnection(uri)), query_text(query_text),
      statement(connection->GetConnection().MakeStatement(query_text)) {
}

AdbcArrowStreamFactory::AdbcArrowStreamFactory(unique_ptr<AdbcPooledConnection> conn, const string &query_text)
    : connection(std::move(conn)), query_text(query_text),
      statement(connection->GetConnection().MakeStatement(query_text)) {
}

AdbcStatement *AdbcArrowStreamFactory::GetStatement() {
    return statement.get();
}

void AdbcArrowStreamFactory::ResetStatement() {
    statement = connection->GetConnection().MakeStatement(query_text);
}

unique_ptr<ArrowArrayStreamWrapper> AdbcProduceArrowScan(uintptr_t factory_ptr, ArrowStreamParameters &parameters) {
    // Reinterpret the factory pointer to the correct class
    auto factory = reinterpret_cast<AdbcArrowStreamFactory *>(factory_ptr);

    // Create the stream for the query result
    Handle<Private::AdbcError> error = {};
    ArrowArrayStream adbc_stream = {};
    int64_t rows_affected;
    CHECK_ADBC(AdbcStatementExecuteQuery(factory->GetStatement(), &adbc_stream, &rows_affected, error.get()),
               IOException);

    // Create and return the wrapper owning the stream for DuckDB
    auto wrapper = make_uniq<ArrowArrayStreamWrapper>();
    std::memcpy(&wrapper->arrow_array_stream, &adbc_stream, sizeof(ArrowArrayStream));
    wrapper->number_of_rows = rows_affected;
    return wrapper;
}

AdbcArrowScanFunctionData::AdbcArrowScanFunctionData(ClientContext &context, unique_ptr<AdbcArrowStreamFactory> factory)
    : ArrowScanFunctionData(AdbcProduceArrowScan, reinterpret_cast<uintptr_t>(factory.get())),
      adbc_arrow_stream_factory(std::move(factory)) {

    // Retrieve and register the schema information from ADBC with DuckDB
    Handle<Private::AdbcError> error = {};
    auto *statement = adbc_arrow_stream_factory->GetStatement();
    auto *schema = reinterpret_cast<ArrowSchema *>(&schema_root.arrow_schema);

    // Try running ExecuteSchema(...)
    auto schema_status = AdbcStatementExecuteSchema(statement, schema, error.get());

    // If it's not available, then execute the query, get the schema, and cancel the query
    if (schema_status == ADBC_STATUS_NOT_IMPLEMENTED) {
        error.reset();
        Handle<ArrowArrayStream> stream = {};
        int64_t rows_affected = 0;
        CHECK_ADBC(AdbcStatementExecuteQuery(statement, stream.get(), &rows_affected, error.get()), BinderException);
        if (stream->get_schema(stream.get(), schema) != 0) {
            throw BinderException("Failed to get schema from ADBC stream");
        }
        stream.reset();
        adbc_arrow_stream_factory->ResetStatement();
    } else {
        CHECK_ADBC(schema_status, BinderException);
    }

    ArrowTableFunction::PopulateArrowTableSchema(DBConfig::GetConfig(context), arrow_table, schema_root.arrow_schema);
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
    if (global_state.CanRemoveFilterColumns()) {
        local_state.all_columns.Reset();
        local_state.all_columns.SetCardinality(output_size);

        ArrowTableFunction::ArrowToDuckDB(local_state,
                                          function_data.arrow_table.GetColumns(),
                                          local_state.all_columns,
                                          function_data.lines_read - output_size,
                                          false);
        output.ReferenceColumns(local_state.all_columns, global_state.projection_ids);
    } else {
        output.SetCardinality(output_size);
        ArrowTableFunction::ArrowToDuckDB(local_state,
                                          function_data.arrow_table.GetColumns(),
                                          output,
                                          function_data.lines_read - output_size,
                                          false);
    }

    output.Verify();
    local_state.chunk_offset += output.size();
}

unique_ptr<FunctionData> AdbcScanBindFunction(ClientContext &context,
                                              TableFunctionBindInput &input,
                                              vector<LogicalType> &return_types,
                                              vector<string> &names) {

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
    names = function_data->arrow_table.GetNames();
    return_types = function_data->arrow_table.GetTypes();
    function_data->all_types = return_types;
    return function_data;
}

} // namespace adbc
} // namespace duckdb
