#include "adbc_insert.hpp"
#include "adbc_catalog.hpp"
#include "duckdb/common/arrow/arrow_converter.hpp"
#include "duckdb/planner/operator/logical_insert.hpp"
#include <mutex>
#include <condition_variable>
#include <iostream>

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
                                 InsertMode insert_mode)
      : context(context), catalog(catalog), insert_count(0), rows_affected(0),
        column_types(types), column_names(names), table_name(table_name),
        insert_mode(insert_mode), collection(context, types) {}

  // Insert thread
  std::thread insert_thread;

  // Synchronization primitives
  std::mutex mutex;
  std::condition_variable cv;
  bool done = false;
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
};

unique_ptr<GlobalSinkState>
AdbcInsert::GetGlobalSinkState(ClientContext &context) const {
  return make_uniq<AdbcInsertGlobalState>(
      context, catalog, column_types, column_names, table_name, insert_mode);
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
    std::cout << "Worker: get_schema(...) called!" << std::endl;
    auto *data = static_cast<ArrowStreamCollectionData *>(s->private_data);
    auto &gstate = *data->gstate;
    if (!data->cached_schema) {
      std::cout << "Worker: cached_schema not there, acquiring lock!"
                << std::endl;
      // Acquire the lock to retrieve the schema
      std::lock_guard<std::mutex> state_lock(gstate.mutex);
      data->cached_schema = make_uniq<ArrowSchema>();
      auto properties = gstate.context.GetClientProperties();
      ArrowConverter::ToArrowSchema(data->cached_schema.get(),
                                    gstate.column_types, gstate.column_names,
                                    properties);
    } else {
      std::cout << "Worker: cached_schema is there!" << std::endl;
    }
    std::cout << "Worker: No locks held now, copying schema" << std::endl;
    memcpy(out, data->cached_schema.get(), sizeof(ArrowSchema));
    return 0;
  };

  // Scan the next chunk for the buffered data and convert it to Arrow format
  stream->get_next = [](ArrowArrayStream *s, ArrowArray *out) -> int {
    auto *data = static_cast<ArrowStreamCollectionData *>(s->private_data);
    auto &gstate = *data->gstate;

    std::cout << "Worker: get_next(...) called and creating unique_lock!"
              << std::endl;
    // Acquire the lock
    std::unique_lock<std::mutex> state_lock(gstate.mutex);
    std::cout << "Worker: Initializing chunk!" << std::endl;
    DataChunk chunk;
    chunk.Initialize(Allocator::DefaultAllocator(), gstate.column_types);

    std::cout << "Worker: Calling cv.wait(...)" << std::endl;

    // Wait until there's data to insert or the done flag is set
    gstate.cv.wait(state_lock, [&gstate, &data, &chunk] {
      return gstate.collection.Scan(*data->scan_state, chunk) || gstate.done;
    });

    std::cout << "Worker: Returned from cv.wait(...)" << std::endl;
    // No more data, ensure that the producer has set the done flag
    if (chunk.size() == 0) {
      std::cout << "Worker: No more data, exiting..." << std::endl;
      D_ASSERT(gstate.done);
      out->release = nullptr;
      return 0;
    } else {
      std::cout << "Worker: Data available!" << std::endl;
    }

    std::cout << "Worker: Converting chunk to Arrow and returning" << std::endl;
    // Otherwise scan the next chunk and return
    ArrowConverter::ToArrowArray(chunk, out,
                                 gstate.context.GetClientProperties(),
                                 ArrowTypeExtensionData::GetExtensionTypes(
                                     gstate.context, gstate.column_types));
    return 0;
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
  };

  stream->private_data = data.release();
}

// TODO: Add static functions to ADBC insert
static void AsyncInsert(AdbcInsertGlobalState &gstate) {

  std::cout << "Worker: AsyncInsert called!" << std::endl;

  // Get the ADBC connection
  auto &adbc_catalog = gstate.catalog.Cast<AdbcCatalog>();
  auto shared_connection = adbc_catalog.GetSharedConnection();
  std::lock_guard<std::mutex> connection_lock(shared_connection->GetMutex());
  auto *connection = shared_connection->GetConnection();

  std::cout << "Worker: Got connection and calling "
               "CreateArrowStreamFromCollection(...)!"
            << std::endl;

  // Wrap the buffered data as an ArrowArrayStream
  Handle<ArrowArrayStream> stream = {};
  CreateArrowStreamFromCollection(gstate, stream.get());

  std::cout << "Worker: Creating ADBC statement!" << std::endl;

  // Create the ADBC statement to perform the insert
  Handle<Private::AdbcStatement> statement = {};
  CHECK_STATUS(AdbcStatementNew(connection, statement.get(), &error));

  std::cout << "Worker: Setting insert mode!" << std::endl;

  // Set append mode only if we are doing an INSERT (not a CTAS)
  if (gstate.insert_mode == InsertMode::APPEND) {
    CHECK_STATUS(
        AdbcStatementSetOption(statement.get(), ADBC_INGEST_OPTION_MODE,
                               ADBC_INGEST_OPTION_MODE_APPEND, &error));
  }

  std::cout << "Worker: Setting table name!" << std::endl;

  // Set the table name to perform the insert on
  CHECK_STATUS(AdbcStatementSetOption(statement.get(),
                                      ADBC_INGEST_OPTION_TARGET_TABLE,
                                      gstate.table_name.c_str(), &error));

  std::cout << "Worker: Binding stream!" << std::endl;

  // Bind the stream to the statement
  CHECK_STATUS(AdbcStatementBindStream(statement.get(), stream.get(), &error));

  std::cout << "Worker: Calling ExecuteQuery(...) on stream!" << std::endl;

  // Execute the insert
  CHECK_STATUS(AdbcStatementExecuteQuery(statement.get(), nullptr,
                                         &gstate.rows_affected, &error));

  std::cout << "Worker: Returning from AsyncInsert normally!" << std::endl;

  // AdbcStatementRelease(statement.get(), &error);
};

//===--------------------------------------------------------------------===//
// Sink
//===--------------------------------------------------------------------===//
SinkResultType AdbcInsert::Sink(ExecutionContext &context, DataChunk &chunk,
                                OperatorSinkInput &input) const {
  auto &gstate = input.global_state.Cast<AdbcInsertGlobalState>();

  std::cout << "Sink(...) called and acquiring lock!" << std::endl;
  {
    // Acquire the lock and insert the chunk into the buffer
    std::lock_guard<std::mutex> state_lock(gstate.mutex);

    // If this is the first insert then spawn the insert thread
    if (gstate.first_insert) {
      std::cout << "first_insert was true!" << std::endl;
      gstate.first_insert = false;
      std::cout << "Launching thread!" << std::endl;
      gstate.insert_thread = std::thread(AsyncInsert, std::ref(gstate));
    } else {
      std::cout << "first_insert was false!" << std::endl;
    }

    // Now insert into the buffer
    gstate.collection.Append(chunk);
    gstate.insert_count += chunk.size();

    std::cout << "Appending chunk to buffer!" << std::endl;
    std::cout << "Insert count is now: " << gstate.insert_count << std::endl;

    // Throw an exception if the insert thread hit an error
    std::cout << "Checking error and throwing if there is one!" << std::endl;
    auto &error = gstate.error;
    CHECK_ADBC(gstate.status, IOException);
  }
  std::cout << "Sink(...) releasing lock and notifying thread of work!"
            << std::endl;

  // Notify the insert thread of new work
  gstate.cv.notify_one();
  return SinkResultType::NEED_MORE_INPUT;
}

//===--------------------------------------------------------------------===//
// Finalize
//===--------------------------------------------------------------------===//
SinkFinalizeType AdbcInsert::Finalize(Pipeline &pipeline, Event &event,
                                      ClientContext &context,
                                      OperatorSinkFinalizeInput &input) const {
  auto &gstate = input.global_state.Cast<AdbcInsertGlobalState>();
  std::cout << "Finalize(...) called and acquiring lock!" << std::endl;

  // Signal that all data has been sent
  {
    std::lock_guard<std::mutex> state_lock(gstate.mutex);
    std::cout << "Setting done flag!" << std::endl;
    gstate.done = true;
  }
  std::cout << "Finalize(...) releasing lock and notifying thread of done flag!"
            << std::endl;

  // Notify the insert thread that all work is completed
  gstate.cv.notify_one();

  std::cout << "Calling join() on thread" << std::endl;
  // Wait for the insert thread to complete
  gstate.insert_thread.join();
  std::cout << "join() completed" << std::endl;

  // Check for ADBC errors
  std::cout << "Checking error and throwing if there is one!" << std::endl;
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
  std::cout << "GetData(...) called and acquiring lock!" << std::endl;
  {
    // Acquire the lock before fetching the insert count
    std::lock_guard<std::mutex> state_lock(gstate.mutex);
    chunk.SetCardinality(1);
    chunk.SetValue(0, 0, Value::BIGINT(gstate.insert_count));
  }
  std::cout << "GetData(...) finished and released lock!" << std::endl;
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
