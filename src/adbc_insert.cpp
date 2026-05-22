#include "adbc_insert.hpp"
#include "adbc_catalog.hpp"
#include "duckdb/common/arrow/arrow_converter.hpp"
#include "duckdb/planner/operator/logical_insert.hpp"
#include "duckdb/storage/temporary_memory_manager.hpp"
#include <mutex>
#include <condition_variable>

// Check if an ADBC command went wrong, if it did then save the state and return
#define CHECK_STATUS(EXPR)                                                     \
  do {                                                                         \
    Private::AdbcError error = {};                                             \
    AdbcStatusCode status = (EXPR);                                            \
    if (status != ADBC_STATUS_OK) {                                            \
      std::lock_guard<std::mutex> state_lock(gstate.mutex);                    \
      gstate.status = status;                                                  \
      gstate.error = error;                                                    \
      return;                                                                  \
    }                                                                          \
  } while (false)

namespace duckdb {
namespace adbc {

AdbcInsert::AdbcInsert(PhysicalPlan &physical_plan, LogicalOperator &op,
                       const vector<LogicalType> &types,
                       const vector<string> &names, const string &table_name,
                       const string &schema_name, AdbcCatalog &catalog,
                       InsertMode mode)
    : PhysicalOperator(physical_plan, PhysicalOperatorType::EXTENSION, op.types,
                       1),
      column_types(types), column_names(names), table_name(table_name),
      schema_name(schema_name), catalog(catalog), insert_mode(mode) {}

//===--------------------------------------------------------------------===//
// States
//===--------------------------------------------------------------------===//
class AdbcInsertGlobalState : public GlobalSinkState {
public:
  explicit AdbcInsertGlobalState(ClientContext &context, AdbcCatalog &catalog,
                                 const vector<LogicalType> &types,
                                 const vector<string> &names,
                                 const string &table_name,
                                 const string &schema_name,
                                 InsertMode insert_mode,
                                 idx_t max_thread_memory)
      : context(context), catalog(catalog), column_types(types),
        column_names(names), table_name(table_name), schema_name(schema_name),
        insert_mode(insert_mode), collection(context, types),
        temporary_memory_state(
            TemporaryMemoryManager::Get(context).Register(context)) {

    // Set the maximum number of chunks using the configuration setting
    Value option_value;
    context.TryGetCurrentSetting("adbc_insert_batch_size", option_value);
    max_chunks = option_value.GetValue<int64_t>();

    // Track the maximum amount of available memory we have
    temporary_memory_state->SetRemainingSizeAndUpdateReservation(
        context, max_thread_memory);
  }

  ~AdbcInsertGlobalState() {
    {
      // Acquire the lock to update flags
      std::lock_guard<std::mutex> state_lock(mutex);
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

    // Release the error state
    if (error.release) {
      error.release(&error);
      error.release = nullptr;
    }
  }

  // Insert thread
  std::thread consumer_thread;

  // Synchronization primitives
  std::mutex mutex;
  std::condition_variable consumer_cv;
  std::condition_variable producer_cv;
  bool done = false;
  bool canceled = false;
  bool full = false;
  bool first_insert = true;

  // Batch size control
  int64_t max_chunks;
  int64_t active_chunks = 0;

  // ADBC State
  Private::AdbcError error = {};
  Private::AdbcStatusCode status = {};

  // Insert state
  ClientContext &context;
  AdbcCatalog &catalog;
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

unique_ptr<GlobalSinkState>
AdbcInsert::GetGlobalSinkState(ClientContext &context) const {
  return make_uniq<AdbcInsertGlobalState>(
      context, catalog, column_types, column_names, table_name, schema_name,
      insert_mode, GetMaxThreadMemory(context));
}

struct ArrowStreamCollectionData {
  AdbcInsertGlobalState *gstate;
  unique_ptr<ArrowSchema> cached_schema;
  unique_ptr<ColumnDataScanState> scan_state;
};

static void CreateArrowStreamFromCollection(AdbcInsertGlobalState &gstate,
                                            ArrowArrayStream *stream) {

  // Populate the necessary state to stream the input
  auto data = make_uniq<ArrowStreamCollectionData>();
  data->gstate = &gstate;
  data->scan_state = make_uniq<ColumnDataScanState>();
  gstate.collection.InitializeScan(*data->scan_state);

  // Use DuckDB helpers to retrieve the ArrowSchema
  stream->get_schema = [](ArrowArrayStream *s, ArrowSchema *out) -> int {
    auto *data = static_cast<ArrowStreamCollectionData *>(s->private_data);
    auto &gstate = *data->gstate;
    if (!data->cached_schema) {
      // Acquire the lock to retrieve the schema
      std::lock_guard<std::mutex> state_lock(gstate.mutex);
      data->cached_schema = make_uniq<ArrowSchema>();
      auto properties = gstate.context.GetClientProperties();
      ArrowConverter::ToArrowSchema(data->cached_schema.get(),
                                    gstate.column_types, gstate.column_names,
                                    properties);
    }
    memcpy(out, data->cached_schema.get(), sizeof(ArrowSchema));
    return 0;
  };

  // Scan the next chunk for the buffered data and convert it to Arrow format
  stream->get_next = [](ArrowArrayStream *s, ArrowArray *out) -> int {
    auto *data = static_cast<ArrowStreamCollectionData *>(s->private_data);
    auto &gstate = *data->gstate;

    {
      // Acquire the lock
      std::unique_lock<std::mutex> state_lock(gstate.mutex);

      // Wait for the buffer to be full or done
      gstate.consumer_cv.wait(state_lock,
                              [&gstate] { return gstate.full || gstate.done; });

      // Read the next chunk
      DataChunk chunk;
      chunk.Initialize(Allocator::DefaultAllocator(), gstate.column_types);
      bool read_chunk = gstate.collection.Scan(*data->scan_state, chunk);

      // Convert the DuckDB chunk to Arrow
      ArrowConverter::ToArrowArray(chunk, out,
                                   gstate.context.GetClientProperties(),
                                   ArrowTypeExtensionData::GetExtensionTypes(
                                       gstate.context, gstate.column_types));

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

  stream->get_last_error = [](ArrowArrayStream *) -> const char * {
    return nullptr;
  };

  // Take ownership of the data and release it
  stream->release = [](ArrowArrayStream *s) {
    // No locking required as the stream data is only used from this thread
    unique_ptr<ArrowStreamCollectionData> data(
        static_cast<ArrowStreamCollectionData *>(s->private_data));
    s->release = nullptr;
    auto &gstate = *data->gstate;
    {
      // Acquire lock and notifying done
      std::unique_lock<std::mutex> state_lock(gstate.mutex);
      gstate.done = true;
    }
    gstate.producer_cv.notify_one();
  };

  stream->private_data = data.release();
}

static void AsyncInsert(AdbcInsertGlobalState &gstate) {

  // Get the ADBC connection
  auto pooled_connection = gstate.catalog.GetPooledConnection();
  auto *connection = pooled_connection->GetRawConnection();

  // Wrap the buffered data as an ArrowArrayStream
  Handle<ArrowArrayStream> stream = {};
  CreateArrowStreamFromCollection(gstate, stream.get());

  // Create the ADBC statement to perform the insert
  Handle<Private::AdbcStatement> statement = {};
  CHECK_STATUS(AdbcStatementNew(connection, statement.get(), &error));

  // Set append mode only if we are doing an INSERT (not a CTAS)
  if (gstate.insert_mode == InsertMode::APPEND) {
    CHECK_STATUS(
        AdbcStatementSetOption(statement.get(), ADBC_INGEST_OPTION_MODE,
                               ADBC_INGEST_OPTION_MODE_APPEND, &error));
  }

  // Set the schema name to perform the insert on
  CHECK_STATUS(AdbcStatementSetOption(statement.get(),
                                      ADBC_INGEST_OPTION_TARGET_DB_SCHEMA,
                                      gstate.schema_name.c_str(), &error));

  // Set the table name to perform the insert on
  CHECK_STATUS(AdbcStatementSetOption(statement.get(),
                                      ADBC_INGEST_OPTION_TARGET_TABLE,
                                      gstate.table_name.c_str(), &error));

  // Bind the stream to the statement
  CHECK_STATUS(AdbcStatementBindStream(statement.get(), stream.get(), &error));

  // Execute the insert
  CHECK_STATUS(AdbcStatementExecuteQuery(statement.get(), nullptr,
                                         &gstate.rows_affected, &error));
};

//===--------------------------------------------------------------------===//
// Sink
//===--------------------------------------------------------------------===//

SinkResultType AdbcInsert::Sink(ExecutionContext &context, DataChunk &chunk,
                                OperatorSinkInput &input) const {
  auto &gstate = input.global_state.Cast<AdbcInsertGlobalState>();

  {
    // Acquire the lock
    std::unique_lock<std::mutex> state_lock(gstate.mutex);

    // Immediately insert the first chunk for responsive error handling
    bool first_chunk = false;

    // Launch the insert thread on the first insert
    if (gstate.first_insert) {
      first_chunk = true;
      gstate.first_insert = false;
      gstate.consumer_thread = std::thread(AsyncInsert, std::ref(gstate));
    }

    // Wait until the buffer is not full or we are done
    gstate.producer_cv.wait(state_lock,
                            [&gstate] { return !gstate.full || gstate.done; });

    // Exit if cancel flag is set (errors are handled in Finalize(...))
    if (gstate.canceled) {
      return SinkResultType::FINISHED;
    }

    // Insert the chunk
    gstate.collection.Append(chunk);
    gstate.insert_count += chunk.size();
    gstate.active_chunks += 1;

    // Block on 50% memory usage or hitting the max chunk size
    auto used_bytes = gstate.collection.SizeInBytes();
    auto free_bytes = gstate.temporary_memory_state->GetReservation();
    bool exhausted_memory = used_bytes >= 0.5 * free_bytes;
    bool hit_chunk_limit = gstate.active_chunks >= gstate.max_chunks;
    if (first_chunk || exhausted_memory || hit_chunk_limit) {
      gstate.full = true;
    }
  }
  gstate.consumer_cv.notify_one();
  return SinkResultType::NEED_MORE_INPUT;
}

//===--------------------------------------------------------------------===//
// Finalize
//===--------------------------------------------------------------------===//
SinkFinalizeType AdbcInsert::Finalize(Pipeline &pipeline, Event &event,
                                      ClientContext &context,
                                      OperatorSinkFinalizeInput &input) const {
  auto &gstate = input.global_state.Cast<AdbcInsertGlobalState>();

  // Signal that all data has been sent
  {
    std::lock_guard<std::mutex> state_lock(gstate.mutex);
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
    throw IOException("Row count mismatch: expected %llu, got %lld",
                      gstate.insert_count, gstate.rows_affected);
  }
  return SinkFinalizeType::READY;
}

//===--------------------------------------------------------------------===//
// GetData
//===--------------------------------------------------------------------===//
//
SourceResultType AdbcInsert::GetData(ExecutionContext &context,
                                     DataChunk &chunk,
                                     OperatorSourceInput &input) const {
  auto &gstate = sink_state->Cast<AdbcInsertGlobalState>();
  {
    // Acquire the lock before fetching the insert count
    std::lock_guard<std::mutex> state_lock(gstate.mutex);
    chunk.SetCardinality(1);
    chunk.SetValue(0, 0, Value::BIGINT(gstate.insert_count));
  }
  return SourceResultType::FINISHED;
}

//===--------------------------------------------------------------------===//
// Helpers
//===--------------------------------------------------------------------===//
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
