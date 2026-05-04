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
                       Catalog &catalog, InsertMode mode)
    : PhysicalOperator(physical_plan, PhysicalOperatorType::EXTENSION, op.types,
                       1),
      column_types(types), column_names(names), table_name(table_name),
      catalog(catalog), insert_mode(mode) {}

//===--------------------------------------------------------------------===//
// States
//===--------------------------------------------------------------------===//
class AdbcInsertGlobalState : public GlobalSinkState {
public:
  explicit AdbcInsertGlobalState(ClientContext &context, Catalog &catalog,
                                 const vector<LogicalType> &types,
                                 const vector<string> &names,
                                 const string &table_name,
                                 InsertMode insert_mode,
                                 idx_t max_thread_memory)
      : context(context), catalog(catalog), insert_count(0), rows_affected(0),
        column_types(types), column_names(names), table_name(table_name),
        insert_mode(insert_mode), collection(context, types),
        temporary_memory_state(
            TemporaryMemoryManager::Get(context).Register(context)) {
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
    // Notify both the consumering and producering threads
    consumer_cv.notify_one();
    producer_cv.notify_one();

    // Wait for it to exit gracefully if possible
    if (consumer_thread.joinable()) {
      consumer_thread.join();
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

  // ADBC State
  Private::AdbcError error = {};
  Private::AdbcStatusCode status = {};

  // Insert state
  ClientContext &context;
  Catalog &catalog;
  idx_t insert_count;
  int64_t rows_affected;
  vector<LogicalType> column_types;
  vector<string> column_names;
  string table_name;
  InsertMode insert_mode;
  ColumnDataCollection collection;
  unique_ptr<TemporaryMemoryState> temporary_memory_state;
};

unique_ptr<GlobalSinkState>
AdbcInsert::GetGlobalSinkState(ClientContext &context) const {
  return make_uniq<AdbcInsertGlobalState>(context, catalog, column_types,
                                          column_names, table_name, insert_mode,
                                          GetMaxThreadMemory(context));
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
      bool has_chunk = gstate.collection.Scan(*data->scan_state, chunk);
      ArrowConverter::ToArrowArray(chunk, out,
                                   gstate.context.GetClientProperties(),
                                   ArrowTypeExtensionData::GetExtensionTypes(
                                       gstate.context, gstate.column_types));

      // We just read the last chunk
      if (!has_chunk) {

        // Done flag is set so we exit
        if (gstate.done) {
          out->release = nullptr;
        }

        // Full flag is set so we signal the producer to do more work
        if (gstate.full) {
          gstate.full = false;
          gstate.collection.Reset();
          data->scan_state = make_uniq<ColumnDataScanState>();
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

// TODO: Add static functions to ADBC insert
static void AsyncInsert(AdbcInsertGlobalState &gstate) {

  // Get the ADBC connection
  auto &adbc_catalog = gstate.catalog.Cast<AdbcCatalog>();
  auto shared_connection = adbc_catalog.GetSharedConnection();
  std::lock_guard<std::mutex> connection_lock(shared_connection->GetMutex());
  auto *connection = shared_connection->GetConnection();

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

    // Launch the insert thread on the first insert
    if (gstate.first_insert) {
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

    // Set the full flag if we exceed 90% of the buffer
    if (gstate.collection.SizeInBytes() >=
        0.9 * gstate.temporary_memory_state->GetReservation()) {
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
  result["Table"] = table_name;
  return result;
}

} // namespace adbc
} // namespace duckdb
