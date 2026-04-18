#include "adbc_catalog.hpp"
#include "adbc_schema_entry.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/common/types/column/column_data_collection.hpp"
#include "duckdb/execution/operator/scan/physical_table_scan.hpp"
#include "duckdb/parser/parsed_data/create_schema_info.hpp"
#include "duckdb/planner/operator/logical_insert.hpp"
#include "duckdb/storage/database_size.hpp"
#include "duckdb/common/arrow/arrow_converter.hpp"
#include "duckdb/planner/operator/logical_create_table.hpp"
#include "duckdb/planner/parsed_data/bound_create_table_info.hpp"

namespace duckdb {
namespace adbc {

AdbcInsert::AdbcInsert(PhysicalPlan &physical_plan, LogicalOperator &op,
                       TableCatalogEntry &table,
                       physical_index_vector_t<idx_t> column_index_map_p)
    : PhysicalOperator(physical_plan, PhysicalOperatorType::EXTENSION, op.types,
                       1),
      table(&table), column_index_map(std::move(column_index_map_p)) {}

//===--------------------------------------------------------------------===//
// States
//===--------------------------------------------------------------------===//
class AdbcInsertGlobalState : public GlobalSinkState {
public:
  explicit AdbcInsertGlobalState(ClientContext &context,
                                 const TableCatalogEntry &table)
      : insert_count(0), collection(context, table.GetTypes()) {
    auto &columns = table.GetColumns();
    for (auto &col : columns.Logical()) {
      column_types.push_back(col.GetType());
      column_names.push_back(col.GetName());
    }
  }

  idx_t insert_count;
  vector<LogicalType> column_types;
  vector<string> column_names;
  ColumnDataCollection collection;
};

unique_ptr<GlobalSinkState>
AdbcInsert::GetGlobalSinkState(ClientContext &context) const {
  return make_uniq<AdbcInsertGlobalState>(context, *table);
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
  auto &adbc_catalog = table->catalog.Cast<AdbcCatalog>();
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
  CHECK_ADBC(AdbcStatementSetOption(statement.get(), ADBC_INGEST_OPTION_MODE,
                                    ADBC_INGEST_OPTION_MODE_APPEND, &error),
             IOException);

  CHECK_ADBC(AdbcStatementSetOption(statement.get(),
                                    ADBC_INGEST_OPTION_TARGET_TABLE,
                                    table->name.c_str(), &error),
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
  auto *data = new ArrowStreamCollectionData();
  data->context = &context;
  data->types = types;
  data->names = names;
  data->scan_state = make_uniq<ColumnDataScanState>();
  collection.InitializeScan(*data->scan_state);
  data->collection = &collection;

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

  stream->release = [](ArrowArrayStream *s) {
    delete static_cast<ArrowStreamCollectionData *>(s->private_data);
    s->release = nullptr;
  };

  stream->private_data = data;
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
string AdbcInsert::GetName() const { return "write_adbc"; }

InsertionOrderPreservingMap<string> AdbcInsert::ParamsToString() const {
  InsertionOrderPreservingMap<string> result;
  result["Table"] = table->name;
  return result;
}

void AdbcCatalog::ForEachCatalog(
    const char *schema_name, int depth,
    const std::function<bool(ArrowArray *)> &callback) {

  // Lock the connection and retrieve the catalog info from the Adbc
  // connection
  std::lock_guard<std::mutex> connection_lock(shared_connection->GetMutex());
  Private::AdbcError error = {};
  Handle<ArrowArrayStream> stream = {};
  CHECK_ADBC(AdbcConnectionGetObjects(shared_connection->GetConnection(), depth,
                                      nullptr, schema_name, nullptr, nullptr,
                                      nullptr, stream.get(), &error),
             IOException);

  while (true) {
    Handle<ArrowArray> batch = {};
    if (stream->get_next(stream.get(), batch.get()) != 0 ||
        batch->release == nullptr) {
      break;
    }
    // Execute the callback on the batch
    if (!callback(batch.get())) {
      return;
    }
  }
}

bool AdbcCatalog::SchemaExists(const string &schema_name) {
  // Check if the schema exists
  bool exists = false;
  ForEachCatalog(schema_name.c_str(), ADBC_OBJECT_DEPTH_DB_SCHEMAS,
                 [&exists](ArrowArray *batch) {
                   auto *catalog_schemas_list = batch->children[1];
                   auto *offsets = reinterpret_cast<const int32_t *>(
                       catalog_schemas_list->buffers[1]);
                   for (int64_t i = 0; i < batch->length; ++i) {
                     // Check if schema exists
                     if (offsets[i + 1] > offsets[i]) {
                       exists = true;
                       return false;
                     }
                   }
                   return true;
                 });
  return exists;
}

vector<string> AdbcCatalog::FetchSchemaNames() {
  // Collect all schema names from the result
  vector<string> schema_names;
  ForEachCatalog(nullptr, ADBC_OBJECT_DEPTH_DB_SCHEMAS,
                 [&schema_names](ArrowArray *batch) {
                   auto *catalog_schemas_list = batch->children[1];
                   for (int64_t i = 0; i < batch->length; ++i) {
                     auto *schema_offsets = reinterpret_cast<const int32_t *>(
                         catalog_schemas_list->buffers[1]);
                     auto start_idx = schema_offsets[i];
                     auto end_idx = schema_offsets[i + 1];

                     auto *schemas_struct = catalog_schemas_list->children[0];
                     auto *name_array = schemas_struct->children[0];
                     auto *name_offsets = reinterpret_cast<const int32_t *>(
                         name_array->buffers[1]);
                     auto *name_data =
                         reinterpret_cast<const char *>(name_array->buffers[2]);

                     for (int32_t j = start_idx; j < end_idx; ++j) {
                       auto name_start = name_offsets[j];
                       auto name_end = name_offsets[j + 1];
                       schema_names.emplace_back(name_data + name_start,
                                                 name_end - name_start);
                     }
                   }
                   return true;
                 });
  return schema_names;
}

vector<string> AdbcCatalog::FetchTableNames(const string &schema_name) {
  // Collect all table names
  vector<string> table_names;
  ForEachCatalog(
      schema_name.c_str(), ADBC_OBJECT_DEPTH_TABLES,
      [&table_names](ArrowArray *batch) {
        // Get the catalogs
        auto *catalogs = batch;
        auto *catalog_schemas_list = batch->children[1];
        for (int64_t i = 0; i < catalogs->length; ++i) {
          auto *schema_offsets = reinterpret_cast<const int32_t *>(
              catalog_schemas_list->buffers[1]);
          auto schema_start = schema_offsets[i];
          auto schema_end = schema_offsets[i + 1];

          auto *schemas_struct = catalog_schemas_list->children[0];

          // Get the schemas for this catalog
          for (int32_t j = schema_start; j < schema_end; ++j) {
            auto *tables_list = schemas_struct->children[1];
            auto *table_offsets =
                reinterpret_cast<const int32_t *>(tables_list->buffers[1]);
            auto table_start = table_offsets[j];
            auto table_end = table_offsets[j + 1];

            auto *table_struct = tables_list->children[0];
            auto *table_names_array = table_struct->children[0];

            auto *name_offsets = reinterpret_cast<const int32_t *>(
                table_names_array->buffers[1]);
            auto *name_data =
                reinterpret_cast<const char *>(table_names_array->buffers[2]);

            // Get the tables for each schema
            for (int32_t k = table_start; k < table_end; ++k) {
              auto start = name_offsets[k];
              auto end = name_offsets[k + 1];
              table_names.emplace_back(name_data + start, end - start);
            }
          }
        }
        return true;
      });
  return table_names;
}

SchemaCatalogEntry *AdbcCatalog::GetCatalogEntry(const string &schema_name) {
  // acquire the read lock and lookup the catalog entry
  std::shared_lock<std::shared_mutex> read_lock(schemas_mutex);
  auto it = owned_schemas.find(schema_name);
  if (it != owned_schemas.end()) {
    return it->second.get();
  }
  return nullptr;
}

SchemaCatalogEntry *AdbcCatalog::CreateCatalogEntry(const string &schema_name) {
  // acquire the write lock and insert the entry into the catalog
  std::unique_lock<std::shared_mutex> write_lock(schemas_mutex);

  // return the entry if it already exists
  auto it = owned_schemas.find(schema_name);
  if (it != owned_schemas.end()) {
    return it->second.get();
  }

  // create an insert the entry
  CreateSchemaInfo info;
  info.schema = schema_name;
  auto schema_entry = make_uniq<AdbcSchemaEntry>(*this, info);

  // otherwise insert the entry and return it
  auto ptr = schema_entry.get();
  owned_schemas[schema_name] = std::move(schema_entry);
  return ptr;
}

void AdbcCatalog::ScanSchemas(
    ClientContext &context,
    std::function<void(SchemaCatalogEntry &)> callback) {

  // For each schema, create an entry in the catalog (if it doesn't already
  // exist) and execute the callback
  for (auto &schema_name : FetchSchemaNames()) {
    if (auto *entry = GetCatalogEntry(schema_name)) {
      callback(*entry);
    } else {
      callback(*CreateCatalogEntry(schema_name));
    }
  }
}

optional_ptr<SchemaCatalogEntry>
AdbcCatalog::LookupSchema(CatalogTransaction transaction,
                          const EntryLookupInfo &schema_lookup,
                          OnEntryNotFound if_not_found) {

  // Return the entry if it already exists
  const auto &name = schema_lookup.GetEntryName();
  if (auto *entry = GetCatalogEntry(name)) {
    return entry;
  }

  // Throw an exception if the schema doesn't exist
  if (!SchemaExists(name)) {
    throw IOException("Unable to find schema with name: \"%s\"", name);
  }

  // Otherwise add it to the catalog and return it
  return CreateCatalogEntry(name);
}

optional_ptr<CatalogEntry>
AdbcCatalog::CreateSchema(CatalogTransaction transaction,
                          CreateSchemaInfo &info) {
  throw NotImplementedException(
      "CREATE SCHEMA not yet supported with the ADBC extension");
}

void AdbcCatalog::DropSchema(ClientContext &context, DropInfo &info) {
  throw NotImplementedException(
      "DROP SCHEMA not yet supported with the ADBC extension");
}

DatabaseSize AdbcCatalog::GetDatabaseSize(ClientContext &context) {
  throw NotImplementedException("Getting the database size is not yet "
                                "supported with the ADBC extension");
}

bool AdbcCatalog::InMemory() { return false; }

string AdbcCatalog::GetDBPath() { return uri; }

PhysicalOperator &AdbcCatalog::PlanCreateTableAs(ClientContext &context,
                                                 PhysicalPlanGenerator &planner,
                                                 LogicalCreateTable &op,
                                                 PhysicalOperator &plan) {
  throw NotImplementedException(
      "CREATE TABLE not yet supported with the ADBC extension");
}

PhysicalOperator &AdbcCatalog::PlanInsert(ClientContext &context,
                                          PhysicalPlanGenerator &planner,
                                          LogicalInsert &op,
                                          optional_ptr<PhysicalOperator> plan) {
  if (op.return_chunk) {
    throw BinderException(
        "RETURNING clause not yet supported for insertion into Adbc table");
  }

  if (op.on_conflict_info.action_type != OnConflictAction::THROW) {
    throw BinderException("ON CONFLICT clause not yet supported for "
                          "insertion into Adbc table");
  }

  D_ASSERT(plan);
  auto &insert = planner.Make<AdbcInsert>(op, op.table, op.column_index_map);
  insert.children.push_back(*plan);
  return insert;
}

PhysicalOperator &AdbcCatalog::PlanDelete(ClientContext &context,
                                          PhysicalPlanGenerator &planner,
                                          LogicalDelete &op,
                                          PhysicalOperator &plan) {
  throw NotImplementedException(
      "DELETE not yet supported with the ADBC extension");
}

PhysicalOperator &AdbcCatalog::PlanUpdate(ClientContext &context,
                                          PhysicalPlanGenerator &planner,
                                          LogicalUpdate &op,
                                          PhysicalOperator &plan) {
  throw NotImplementedException(
      "UPDATE not yet supported with the ADBC extension");
}

} // namespace adbc
} // namespace duckdb
