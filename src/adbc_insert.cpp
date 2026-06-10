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

#include "adbc_insert.hpp"
#include "adbc_util.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/storage/temporary_memory_manager.hpp"
#include <condition_variable>

// Check if an ADBC command went wrong, if it did then save the state and return
#define CHECK_INVALID_STATUS(EXPR)                                                                                     \
    do {                                                                                                               \
        gstate.status = (EXPR);                                                                                        \
        if (gstate.status != ADBC_STATUS_OK) {                                                                         \
            return;                                                                                                    \
        }                                                                                                              \
    } while (false)


namespace duckdb {
namespace adbc {

AdbcInsert::AdbcInsert(PhysicalPlan &physical_plan,
                       LogicalOperator &op,
                       const vector<LogicalType> &types,
                       const vector<string> &names,
                       const string &table_name,
                       const string &schema_name,
                       shared_ptr<AdbcConnectionPool> pool,
                       InsertMode mode)
    : PhysicalOperator(physical_plan, PhysicalOperatorType::EXTENSION, op.types, 1), column_types(types),
      column_names(names), table_name(table_name), schema_name(schema_name), pool(pool), insert_mode(mode) {
}

class AdbcInsertGlobalState : public GlobalSinkState {
public:
    explicit AdbcInsertGlobalState(ClientContext &context,
                                   unique_ptr<AdbcPooledConnection> conn,
                                   const vector<LogicalType> &types,
                                   const vector<string> &names,
                                   const string &table_name,
                                   const string &schema_name,
                                   InsertMode insert_mode,
                                   idx_t max_thread_memory)
        : context(context), connection(std::move(conn)), column_types(types), column_names(names),
          table_name(table_name), schema_name(schema_name), insert_mode(insert_mode), collection(context, types),
          temporary_memory_state(TemporaryMemoryManager::Get(context).Register(context)) {

        // Set whether we materialize insert rows
        Value option_value;
        context.TryGetCurrentSetting("adbc_materialize_insert_rows", option_value);
        materialize_input = option_value.GetValue<bool>();

        // Set the maximum number of chunks using the configuration setting
        context.TryGetCurrentSetting("adbc_insert_buffer_size", option_value);
        max_chunks = option_value.GetValue<int64_t>();

        // Track the maximum amount of available memory we have
        temporary_memory_state->SetRemainingSizeAndUpdateReservation(context, max_thread_memory);
    }

    ~AdbcInsertGlobalState() {
        {
            // Acquire the lock to update flags
            lock_guard<mutex> state_lock(insert_mutex);
            // Set the cancel flag
            canceled = true;
            // Set the done flag
            done = true;
        }
        // Notify both the consumer and producer threads
        consumer_cv.notify_one();
        producer_cv.notify_one();

        // Wait for the consumer thread to exit gracefully if possible
        if (consumer_thread.joinable()) {
            consumer_thread.join();
        }
    }

    // Insert thread
    thread consumer_thread;

    // Synchronization primitives
    mutex insert_mutex;
    std::condition_variable consumer_cv;
    std::condition_variable producer_cv;
    bool done = false;
    bool canceled = false;
    bool full = false;
    bool first_insert = true;

    // Whether to materialize the entire input
    bool materialize_input = false;

    // Batch size control
    int64_t max_chunks;
    int64_t active_chunks = 0;

    // ADBC State
    Handle<Private::AdbcError> error = {};
    Private::AdbcStatusCode status = {};

    // Insert state
    ClientContext &context;
    unique_ptr<AdbcPooledConnection> connection;
    idx_t insert_count = 0;
    int64_t rows_affected = 0;
    vector<LogicalType> column_types;
    vector<string> column_names;
    string table_name;
    string schema_name;
    InsertMode insert_mode;
    ColumnDataCollection collection;
    unique_ptr<TemporaryMemoryState> temporary_memory_state;
};

unique_ptr<GlobalSinkState> AdbcInsert::GetGlobalSinkState(ClientContext &context) const {

    auto gstate = make_uniq<AdbcInsertGlobalState>(context,
                                                   pool->GetConnection(),
                                                   column_types,
                                                   column_names,
                                                   table_name,
                                                   schema_name,
                                                   insert_mode,
                                                   GetMaxThreadMemory(context));
    // Create the table
    if (insert_mode == InsertMode::CTAS) {
        auto *raw_conn = gstate->connection->GetRawConnection();

        Handle<Private::AdbcStatement> statement = {};
        Handle<ArrowSchema> schema = {};
        Handle<ArrowArray> array = {};
        auto &error = gstate->error;

        // Build the Arrow schema from the column types/names
        auto properties = context.GetClientProperties();
        ArrowConverter::ToArrowSchema(schema.get(), column_types, column_names, properties);

        // Build an empty ArrowArray matching the schema
        ArrowArrayInitFromSchema(array.get(), schema.get(), nullptr);
        ArrowArrayFinishBuilding(array.get(), NANOARROW_VALIDATION_LEVEL_NONE, nullptr);

        // Create a statement to do the CREATE TABLE
        CHECK_ADBC(AdbcStatementNew(raw_conn, statement.get(), error.get()), IOException);

        // Set the schema name
        if (!schema_name.empty()) {
            CHECK_ADBC(AdbcStatementSetOption(statement.get(),
                                              ADBC_INGEST_OPTION_TARGET_DB_SCHEMA,
                                              schema_name.c_str(),
                                              error.get()),
                       IOException);
        }

        // Set the table name
        CHECK_ADBC(
            AdbcStatementSetOption(statement.get(), ADBC_INGEST_OPTION_TARGET_TABLE, table_name.c_str(), error.get()),
            IOException);

        // Bind the empty batch — this creates the table schema without inserting rows
        CHECK_ADBC(AdbcStatementBind(statement.get(), array.get(), schema.get(), error.get()), IOException);
        CHECK_ADBC(AdbcStatementExecuteQuery(statement.get(), nullptr, nullptr, error.get()), IOException);
    }
    return gstate;
}

struct ArrowStreamCollectionData {
    AdbcInsertGlobalState *gstate;
    unique_ptr<ArrowSchema> cached_schema;
    unique_ptr<ColumnDataScanState> scan_state;
};

static void CreateArrowStreamFromCollection(AdbcInsertGlobalState &gstate, ArrowArrayStream *stream) {

    // Populate the necessary state to stream the input
    auto data = make_uniq<ArrowStreamCollectionData>();
    data->gstate = &gstate;
    data->scan_state = make_uniq<ColumnDataScanState>();
    gstate.collection.InitializeScan(*data->scan_state);

    // Eagerly create the schema
    data->cached_schema = make_uniq<ArrowSchema>();
    auto properties = gstate.context.GetClientProperties();
    ArrowConverter::ToArrowSchema(data->cached_schema.get(), gstate.column_types, gstate.column_names, properties);

    // Return the cached schema
    stream->get_schema = [](ArrowArrayStream *s, ArrowSchema *out) -> int {
        auto *data = static_cast<ArrowStreamCollectionData *>(s->private_data);
        memcpy(out, data->cached_schema.get(), sizeof(ArrowSchema));
        return 0;
    };

    // Scan the next chunk for the buffered data and convert it to Arrow format
    stream->get_next = [](ArrowArrayStream *s, ArrowArray *out) -> int {
        auto *data = static_cast<ArrowStreamCollectionData *>(s->private_data);
        auto &gstate = *data->gstate;

        {
            // Acquire the lock
            unique_lock<mutex> state_lock(gstate.insert_mutex);

            // Wait for the buffer to be full or done
            gstate.consumer_cv.wait(state_lock, [&gstate] { return gstate.full || gstate.done; });

            // Read the next chunk
            DataChunk chunk;
            chunk.Initialize(Allocator::DefaultAllocator(), gstate.column_types);
            bool read_chunk = gstate.collection.Scan(*data->scan_state, chunk);

            // Convert the DuckDB chunk to Arrow
            ArrowConverter::ToArrowArray(
                chunk,
                out,
                gstate.context.GetClientProperties(),
                ArrowTypeExtensionData::GetExtensionTypes(gstate.context, gstate.column_types));

            if (read_chunk) {
                // Decrement the chunk count
                gstate.active_chunks -= 1;
            } else {
                // If we are done then simply exit
                if (gstate.done) {
                    // Release the output ArrowArray
                    if (out && out->release) {
                        out->release(out);
                        out->release = nullptr;
                    }
                }

                // If the producer is blocked then reset the buffer and signal for more
                // work
                if (gstate.full) {
                    gstate.full = false;
                    gstate.collection.Reset();
                    data->scan_state = make_uniq<ColumnDataScanState>();
                    gstate.collection.InitializeScan(*data->scan_state);
                }
            }
        }
        gstate.producer_cv.notify_one();
        return gstate.canceled ? ECANCELED : 0;
    };

    stream->get_last_error = [](ArrowArrayStream *) -> const char * { return nullptr; };

    // Take ownership of the data and release it
    stream->release = [](ArrowArrayStream *s) {
        // No locking required as the stream data is only used from this thread
        unique_ptr<ArrowStreamCollectionData> data(static_cast<ArrowStreamCollectionData *>(s->private_data));
        s->release = nullptr;
        auto &gstate = *data->gstate;
        {
            // Acquire lock and notifying done
            unique_lock<mutex> state_lock(gstate.insert_mutex);
            gstate.done = true;
        }
        gstate.producer_cv.notify_one();
    };

    stream->private_data = data.release();
}

// Invariant: Only modify gstate members that are not modified by DuckDB threads
static void AsyncInsert(AdbcInsertGlobalState &gstate) {

    // Get the ADBC connection
    auto *connection = gstate.connection->GetRawConnection();

    // Wrap the buffered data as an ArrowArrayStream
    Handle<ArrowArrayStream> stream = {};
    CreateArrowStreamFromCollection(gstate, stream.get());

    // Create the ADBC statement to perform the insert
    Handle<Private::AdbcStatement> statement = {};
    CHECK_INVALID_STATUS(AdbcStatementNew(connection, statement.get(), gstate.error.get()));

    // Set append mode
    CHECK_INVALID_STATUS(AdbcStatementSetOption(statement.get(),
                                                ADBC_INGEST_OPTION_MODE,
                                                ADBC_INGEST_OPTION_MODE_APPEND,
                                                gstate.error.get()));

    // Set the schema name to perform the insert on
    if (!gstate.schema_name.empty()) {
        CHECK_INVALID_STATUS(AdbcStatementSetOption(statement.get(),
                                                    ADBC_INGEST_OPTION_TARGET_DB_SCHEMA,
                                                    gstate.schema_name.c_str(),
                                                    gstate.error.get()));
    }

    // Set the table name to perform the insert on
    CHECK_INVALID_STATUS(AdbcStatementSetOption(statement.get(),
                                                ADBC_INGEST_OPTION_TARGET_TABLE,
                                                gstate.table_name.c_str(),
                                                gstate.error.get()));

    // Bind the stream to the statement
    CHECK_INVALID_STATUS(AdbcStatementBindStream(statement.get(), stream.get(), gstate.error.get()));

    // Execute the insert
    CHECK_INVALID_STATUS(
        AdbcStatementExecuteQuery(statement.get(), nullptr, &gstate.rows_affected, gstate.error.get()));
}

SinkResultType AdbcInsert::Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const {
    auto &gstate = input.global_state.Cast<AdbcInsertGlobalState>();

    {
        // Acquire the lock
        unique_lock<mutex> state_lock(gstate.insert_mutex);

        // Immediately insert the first chunk for responsive error handling
        bool first_chunk = false;

        // Launch the insert thread on the first insert
        if (gstate.first_insert) {
            first_chunk = true;
            gstate.first_insert = false;
            gstate.consumer_thread = thread(AsyncInsert, std::ref(gstate));
        }

        // Wait until the buffer is not full or we are done
        gstate.producer_cv.wait(state_lock, [&gstate] { return !gstate.full || gstate.done; });

        // Exit if cancel flag is set (errors are handled in Finalize(...))
        if (gstate.canceled) {
            return SinkResultType::FINISHED;
        }

        // Insert the chunk
        gstate.collection.Append(chunk);
        gstate.insert_count += chunk.size();
        gstate.active_chunks += 1;

        // Consider blocking if the entire input doesn't have to be materialized
        if (!gstate.materialize_input) {
            // Block on 50% memory usage or hitting the max chunk size
            auto used_bytes = gstate.collection.SizeInBytes();
            auto free_bytes = gstate.temporary_memory_state->GetReservation();
            bool exhausted_memory = used_bytes >= 0.5 * free_bytes;
            bool hit_chunk_limit = gstate.active_chunks >= gstate.max_chunks;
            if (first_chunk || exhausted_memory || hit_chunk_limit) {
                gstate.full = true;
            }
        }
    }
    gstate.consumer_cv.notify_one();
    return SinkResultType::NEED_MORE_INPUT;
}

SinkFinalizeType AdbcInsert::Finalize(Pipeline &pipeline,
                                      Event &event,
                                      ClientContext &context,
                                      OperatorSinkFinalizeInput &input) const {
    auto &gstate = input.global_state.Cast<AdbcInsertGlobalState>();

    // Signal that all data has been sent
    {
        lock_guard<mutex> state_lock(gstate.insert_mutex);
        gstate.done = true;
    }

    // Notify the consumer thread that all work is completed
    gstate.consumer_cv.notify_one();

    // Wait for the consumer thread to complete
    if (gstate.consumer_thread.joinable()) {
        gstate.consumer_thread.join();
    }

    // Check for ADBC errors
    auto &error = gstate.error;
    CHECK_ADBC(gstate.status, IOException);

    // Validate the affected row count
    if (static_cast<idx_t>(gstate.rows_affected) != gstate.insert_count) {
        throw IOException("Row count mismatch: expected %llu, got %lld", gstate.insert_count, gstate.rows_affected);
    }
    return SinkFinalizeType::READY;
}
#if DUCKDB_MAJOR_VERSION >= 1 && DUCKDB_MINOR_VERSION >= 5
SourceResultType AdbcInsert::GetDataInternal(ExecutionContext &context,
                                             DataChunk &chunk,
                                             OperatorSourceInput &input) const {
#else
SourceResultType AdbcInsert::GetData(ExecutionContext &context, DataChunk &chunk, OperatorSourceInput &input) const {
#endif
    auto &gstate = sink_state->Cast<AdbcInsertGlobalState>();
    {
        // Acquire the lock before fetching the insert count
        lock_guard<mutex> state_lock(gstate.insert_mutex);
        chunk.SetCardinality(1);
        chunk.SetValue(0, 0, Value::BIGINT(gstate.insert_count));
    }
    return SourceResultType::FINISHED;
}

string AdbcInsert::GetName() const {
    string operator_name;
    switch (insert_mode) {
    case InsertMode::APPEND:
        operator_name = "INSERT";
        break;
    case InsertMode::CTAS:
        operator_name = "CREATE_TABLE_AS";
        break;
    }
    D_ASSERT(!operator_name.empty());
    return operator_name;
}

InsertionOrderPreservingMap<string> AdbcInsert::ParamsToString() const {
    InsertionOrderPreservingMap<string> result;
    result["Table"] = schema_name + "." + table_name;
    return result;
}

} // namespace adbc
} // namespace duckdb
