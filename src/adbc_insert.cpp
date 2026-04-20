#include "adbc_insert.hpp"
#include "adbc_catalog.hpp"
#include "duckdb/common/arrow/arrow_converter.hpp"
#include "duckdb/planner/operator/logical_insert.hpp"

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
  explicit AdbcInsertGlobalState(ClientContext &context,
                                 const vector<LogicalType> &types,
                                 const vector<string> &names)
      : insert_count(0), column_types(types), column_names(names),
        collection(context, types) {}

  idx_t insert_count;
  vector<LogicalType> column_types;
  vector<string> column_names;
  ColumnDataCollection collection;
};

unique_ptr<GlobalSinkState>
AdbcInsert::GetGlobalSinkState(ClientContext &context) const {
  return make_uniq<AdbcInsertGlobalState>(context, column_types, column_names);
}

//===--------------------------------------------------------------------===//
// Sink
//===--------------------------------------------------------------------===//
SinkResultType AdbcInsert::Sink(ExecutionContext &context, DataChunk &chunk,
                                OperatorSinkInput &input) const {
  auto &gstate = input.global_state.Cast<AdbcInsertGlobalState>();
  // Buffer the input chunk
  gstate.collection.Append(chunk);
  gstate.insert_count += chunk.size();
  return SinkResultType::NEED_MORE_INPUT;
}

//===--------------------------------------------------------------------===//
// Finalize
//===--------------------------------------------------------------------===//
SinkFinalizeType AdbcInsert::Finalize(Pipeline &pipeline, Event &event,
                                      ClientContext &context,
                                      OperatorSinkFinalizeInput &input) const {
  auto &gstate = input.global_state.Cast<AdbcInsertGlobalState>();

  // Get the ADBC connection
  auto &adbc_catalog = catalog.Cast<AdbcCatalog>();
  auto shared_connection = adbc_catalog.GetSharedConnection();
  std::lock_guard<std::mutex> connection_lock(shared_connection->GetMutex());
  auto *connection = shared_connection->GetConnection();

  // Wrap the buffered data as an ArrowArrayStream
  Handle<ArrowArrayStream> stream = {};
  CreateArrowStreamFromCollection(context, gstate.collection,
                                  gstate.column_types, gstate.column_names,
                                  stream.get());

  // Create the ADBC statement to perform the insert
  Private::AdbcError error = {};
  Handle<Private::AdbcStatement> statement = {};
  CHECK_ADBC(AdbcStatementNew(connection, statement.get(), &error),
             IOException);

  // Set append mode only if we are doing an INSERT (not a CTAS)
  if (insert_mode == InsertMode::APPEND) {
    CHECK_ADBC(AdbcStatementSetOption(statement.get(), ADBC_INGEST_OPTION_MODE,
                                      ADBC_INGEST_OPTION_MODE_APPEND, &error),
               IOException);
  }

  CHECK_ADBC(AdbcStatementSetOption(statement.get(),
                                    ADBC_INGEST_OPTION_TARGET_TABLE,
                                    table_name.c_str(), &error),
             IOException);

  // Bind the stream to the statement
  CHECK_ADBC(AdbcStatementBindStream(statement.get(), stream.get(), &error),
             IOException);

  // Execute the insert
  int64_t rows_affected = 0;
  CHECK_ADBC(AdbcStatementExecuteQuery(statement.get(), nullptr, &rows_affected,
                                       &error),
             IOException);
  AdbcStatementRelease(statement.get(), &error);

  // Validate the affected row count
  if (static_cast<idx_t>(rows_affected) != gstate.insert_count) {
    throw IOException("Row count mismatch: expected %llu, got %lld",
                      gstate.insert_count, rows_affected);
  }

  return SinkFinalizeType::READY;
}

struct ArrowStreamCollectionData {
  ClientContext *context;
  vector<LogicalType> types;
  vector<string> names;
  unique_ptr<ArrowSchema> cached_schema;
  unique_ptr<ColumnDataScanState> scan_state;
  ColumnDataCollection *collection;
};

void AdbcInsert::CreateArrowStreamFromCollection(
    ClientContext &context, ColumnDataCollection &collection,
    const vector<LogicalType> &types, const vector<string> &names,
    ArrowArrayStream *stream) const {

  // Populate the necessary state to stream the input from the buffered (SELECT
  // ...)
  auto data = make_uniq<ArrowStreamCollectionData>();
  data->context = &context;
  data->types = types;
  data->names = names;
  data->scan_state = make_uniq<ColumnDataScanState>();
  collection.InitializeScan(*data->scan_state);
  data->collection = &collection;

  // Use DuckDB helpers to retrieve the ArrowSchema
  stream->get_schema = [](ArrowArrayStream *s, ArrowSchema *out) -> int {
    auto *d = static_cast<ArrowStreamCollectionData *>(s->private_data);
    if (!d->cached_schema) {
      d->cached_schema = make_uniq<ArrowSchema>();
      auto properties = d->context->GetClientProperties();
      ArrowConverter::ToArrowSchema(d->cached_schema.get(), d->types, d->names,
                                    properties);
    }
    memcpy(out, d->cached_schema.get(), sizeof(ArrowSchema));
    return 0;
  };

  // Scan the next chunk for the buffered data and convert it to Arrow format
  stream->get_next = [](ArrowArrayStream *s, ArrowArray *out) -> int {
    auto *d = static_cast<ArrowStreamCollectionData *>(s->private_data);
    DataChunk chunk;
    chunk.Initialize(Allocator::DefaultAllocator(), d->types);

    if (!d->collection->Scan(*d->scan_state, chunk)) {
      out->release = nullptr;
      return 0;
    }

    ArrowConverter::ToArrowArray(
        chunk, out, d->context->GetClientProperties(),
        ArrowTypeExtensionData::GetExtensionTypes(*d->context, d->types));
    return 0;
  };

  stream->get_last_error = [](ArrowArrayStream *) -> const char * {
    return nullptr;
  };

  // Take ownership of the data and release it
  stream->release = [](ArrowArrayStream *s) {
    unique_ptr<ArrowStreamCollectionData> data(
        static_cast<ArrowStreamCollectionData *>(s->private_data));
    s->release = nullptr;
  };

  stream->private_data = data.release();
}

//===--------------------------------------------------------------------===//
// GetData
//===--------------------------------------------------------------------===//
//
SourceResultType AdbcInsert::GetData(ExecutionContext &context,
                                     DataChunk &chunk,
                                     OperatorSourceInput &input) const {
  auto &insert_gstate = sink_state->Cast<AdbcInsertGlobalState>();
  chunk.SetCardinality(1);
  chunk.SetValue(0, 0, Value::BIGINT(insert_gstate.insert_count));
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
