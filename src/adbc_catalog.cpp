#include "adbc_catalog.hpp"
#include "adbc_insert.hpp"
#include "adbc_schema_entry.hpp"
#include "adbc_table_entry.hpp"
#include "duckdb/parser/parsed_data/create_schema_info.hpp"
#include "duckdb/planner/operator/logical_create_table.hpp"
#include "duckdb/planner/operator/logical_insert.hpp"

// Calculate the buffer size based on the actual constants
#define MAX_OPTION_LEN                                                         \
  (sizeof(ADBC_OPTION_VALUE_ENABLED) > sizeof(ADBC_OPTION_VALUE_DISABLED)      \
       ? sizeof(ADBC_OPTION_VALUE_ENABLED)                                     \
       : sizeof(ADBC_OPTION_VALUE_DISABLED))

namespace duckdb {
namespace adbc {

// Each of these public methods must acquire the catalog lock
void AdbcCatalog::ScanSchemas(
    ClientContext &context,
    std::function<void(SchemaCatalogEntry &)> callback) {

  auto catalog_lock = AcquireScopedLock();

  // For each schema, create a catalog entry and execute the callback
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
  auto catalog_lock = AcquireScopedLock();

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

PhysicalOperator &AdbcCatalog::PlanCreateTableAs(ClientContext &context,
                                                 PhysicalPlanGenerator &planner,
                                                 LogicalCreateTable &op,
                                                 PhysicalOperator &plan) {
  auto catalog_lock = AcquireScopedLock();

  // Ensure no IF NOT EXISTS or REPLACE qualifiers are included in the CTAS
  auto &info = op.info;
  if (info->Base().on_conflict != OnCreateConflict::ERROR_ON_CONFLICT) {
    throw BinderException("CREATE TABLE commands not yet supported with IF NOT "
                          "EXISTS or REPLACE with the ADBC extension");
  }

  // Collect column names & types
  vector<LogicalType> column_types;
  vector<string> column_names;
  for (auto &col : info->Base().columns.Logical()) {
    column_types.push_back(col.GetType());
    column_names.push_back(col.GetName());
  }
  auto table_name = info->Base().table;
  auto &insert = planner.Make<AdbcInsert>(op, column_types, column_names,
                                          table_name, *this, InsertMode::CTAS);
  insert.children.push_back(plan);
  return insert;
}

PhysicalOperator &AdbcCatalog::PlanInsert(ClientContext &context,
                                          PhysicalPlanGenerator &planner,
                                          LogicalInsert &op,
                                          optional_ptr<PhysicalOperator> plan) {
  auto catalog_lock = AcquireScopedLock();

  // Ensure no RETURNING clause or ON CONFLICT
  if (op.return_chunk) {
    throw BinderException(
        "RETURNING clause not yet supported for INSERTs into an ADBC table");
  }

  if (op.on_conflict_info.action_type != OnConflictAction::THROW) {
    throw BinderException("ON CONFLICT clause not yet supported for "
                          "INSERTs into ADBC table");
  }

  D_ASSERT(plan);

  // Collect column names & types
  vector<LogicalType> column_types;
  vector<string> column_names;
  auto &table = op.table;
  auto &columns = table.GetColumns();
  for (auto &col : columns.Logical()) {
    column_types.push_back(col.GetType());
    column_names.push_back(col.GetName());
  }
  auto table_name = table.name;
  auto &insert = planner.Make<AdbcInsert>(
      op, column_types, column_names, table_name, *this, InsertMode::APPEND);
  insert.children.push_back(*plan);
  return insert;
}

// Below are Private methods which assume that the catalog lock is already held
void AdbcCatalog::ForEachCatalog(
    const char *schema_name, int depth,
    const std::function<bool(ArrowArray *)> &callback) {

  // Retrieve the catalog info from the ADBC connection
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
  // Look up the entry
  auto it = owned_schemas.find(schema_name);
  if (it != owned_schemas.end()) {
    return it->second.get();
  }
  return nullptr;
}

SchemaCatalogEntry *AdbcCatalog::CreateCatalogEntry(const string &schema_name) {
  // Return the entry if it already exists
  if (auto *entry = GetCatalogEntry(schema_name)) {
    return entry;
  }

  // Create and insert the entry
  CreateSchemaInfo info;
  info.schema = schema_name;
  auto schema_entry = make_uniq<AdbcSchemaEntry>(*this, info);
  auto ptr = schema_entry.get();
  owned_schemas[schema_name] = std::move(schema_entry);
  return ptr;
}

bool AdbcCatalog::AutocommitEnabled() {
  // We create a buffer for the option value returned via ADBC
  char option_value[MAX_OPTION_LEN] = {0};
  size_t option_length = MAX_OPTION_LEN;
  Private::AdbcError error = {};
  CHECK_ADBC(AdbcConnectionGetOption(shared_connection->GetConnection(),
                                     ADBC_CONNECTION_OPTION_AUTOCOMMIT,
                                     option_value, &option_length, &error),
             IOException);

  return strcmp(option_value, ADBC_OPTION_VALUE_ENABLED) == 0;
}

} // namespace adbc
} // namespace duckdb
