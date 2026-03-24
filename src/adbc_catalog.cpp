#include "duckdb.hpp"
#include "duckdb/storage/database_size.hpp"
#include "duckdb/parser/parsed_data/create_schema_info.hpp"
#include "adbc_catalog.hpp"

namespace duckdb {
namespace adbc {

AdbcCatalog::~AdbcCatalog() = default;

void AdbcCatalog::ScanSchemas(
    ClientContext &context,
    std::function<void(SchemaCatalogEntry &)> callback) {

  // Lookup all schemas via ADBC
  Private::AdbcError error = {};
  Private::ArrowArrayStream stream;
  CHECK_ADBC(AdbcConnectionGetObjects(
                 connection.get(), ADBC_OBJECT_DEPTH_DB_SCHEMAS, nullptr,
                 nullptr, nullptr, nullptr, nullptr, &stream, &error),
             IOException);

  // Collect all schema names from the result
  vector<string> schema_names;
  while (true) {
    Private::ArrowArray batch = {};
    if (stream.get_next(&stream, &batch) != 0 || batch.release == nullptr) {
      break;
    }

    Private::ArrowArray *catalog_schemas_list = batch.children[1];
    for (int64_t i = 0; i < batch.length; ++i) {
      const int32_t *schema_offsets =
          (const int32_t *)catalog_schemas_list->buffers[1];
      int32_t start_idx = schema_offsets[i];
      int32_t end_idx = schema_offsets[i + 1];

      Private::ArrowArray *schemas_struct = catalog_schemas_list->children[0];
      Private::ArrowArray *name_array = schemas_struct->children[0];
      const int32_t *name_offsets = (const int32_t *)name_array->buffers[1];
      const char *name_data = (const char *)name_array->buffers[2];

      for (int32_t j = start_idx; j < end_idx; ++j) {
        int32_t name_start = name_offsets[j];
        int32_t name_end = name_offsets[j + 1];
        schema_names.emplace_back(name_data + name_start,
                                  name_end - name_start);
      }
    }
    batch.release(&batch);
  }
  stream.release(&stream);

  // For each schema, create an entry in the catalog and execute the callback
  for (auto &schema_name : schema_names) {
    // Check if the schema does not exist
    if (owned_schemas.find(schema_name) == owned_schemas.end()) {
      CreateSchemaInfo info;
      info.schema = schema_name;
      auto schema_entry = make_uniq<AdbcSchemaEntry>(*this, info);
      auto ptr = schema_entry.get();
      // save the entry
      owned_schemas[schema_name] = std::move(schema_entry);
    }
    // execute the callback
    callback(*owned_schemas[schema_name]);
  }
}

optional_ptr<SchemaCatalogEntry>
AdbcCatalog::LookupSchema(CatalogTransaction transaction,
                          const EntryLookupInfo &schema_lookup,
                          OnEntryNotFound if_not_found) {
  const auto &name = schema_lookup.GetEntryName();

  // Lookup the name and see if the schema actually exists
  Private::AdbcError error = {};
  Private::ArrowArrayStream stream;
  CHECK_ADBC(AdbcConnectionGetObjects(
                 connection.get(), ADBC_OBJECT_DEPTH_DB_SCHEMAS, nullptr,
                 name.c_str(), nullptr, nullptr, nullptr, &stream, &error),
             IOException);

  bool exists = false;
  while (true) {
    Private::ArrowArray batch = {};
    if (stream.get_next(&stream, &batch) != 0 || batch.release == nullptr) {
      break;
    }

    // For each catalog
    for (int64_t i = 0; i < batch.length; ++i) {
      Private::ArrowArray *catalog_schemas_list = batch.children[1];
      const int32_t *offsets =
          (const int32_t *)catalog_schemas_list->buffers[1];

      // Is there a schema?
      if (offsets[i + 1] > offsets[i]) {
        exists = true;
        break;
      }
    }
    batch.release(&batch);
    if (exists)
      break;
  }

  if (!exists) {
    throw IOException("Unable to find schema with name: \"%s\"", name);
  }

  CreateSchemaInfo info;
  info.schema = name;
  auto schema_entry = make_uniq<AdbcSchemaEntry>(*this, info);
  auto ptr = schema_entry.get();
  owned_schemas[name] = std::move(schema_entry);
  return ptr;
}

optional_ptr<CatalogEntry>
AdbcCatalog::CreateSchema(CatalogTransaction transaction,
                          CreateSchemaInfo &info) {
  throw NotImplementedException("ADBC catalog is read-only");
}

void AdbcCatalog::DropSchema(ClientContext &context, DropInfo &info) {
  throw NotImplementedException("ADBC catalog is read-only");
}

DatabaseSize AdbcCatalog::GetDatabaseSize(ClientContext &context) {
  throw NotImplementedException(
      "ADBC catalog does not support getting database size");
}

bool AdbcCatalog::InMemory() { return false; }

string AdbcCatalog::GetDBPath() { return uri; }

PhysicalOperator &AdbcCatalog::PlanCreateTableAs(ClientContext &context,
                                                 PhysicalPlanGenerator &planner,
                                                 LogicalCreateTable &op,
                                                 PhysicalOperator &plan) {
  throw NotImplementedException("ADBC catalog is read-only");
}

PhysicalOperator &AdbcCatalog::PlanInsert(ClientContext &context,
                                          PhysicalPlanGenerator &planner,
                                          LogicalInsert &op,
                                          optional_ptr<PhysicalOperator> plan) {
  throw NotImplementedException("ADBC catalog is read-only");
}

PhysicalOperator &AdbcCatalog::PlanDelete(ClientContext &context,
                                          PhysicalPlanGenerator &planner,
                                          LogicalDelete &op,
                                          PhysicalOperator &plan) {
  throw NotImplementedException("ADBC catalog is read-only");
}

PhysicalOperator &AdbcCatalog::PlanUpdate(ClientContext &context,
                                          PhysicalPlanGenerator &planner,
                                          LogicalUpdate &op,
                                          PhysicalOperator &plan) {
  throw NotImplementedException("ADBC catalog is read-only");
}

} // namespace adbc
} // namespace duckdb
