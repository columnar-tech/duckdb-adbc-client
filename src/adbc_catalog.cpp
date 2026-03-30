#include "adbc_catalog.hpp"
#include "adbc_schema_entry.hpp"
#include "duckdb.hpp"
#include "duckdb/parser/parsed_data/create_schema_info.hpp"
#include "duckdb/storage/database_size.hpp"
namespace duckdb {
namespace adbc {

void AdbcCatalog::ForEachCatalog(
    const char *schema_name, int depth,
    const std::function<bool(Private::ArrowArray *)> &callback) {
    std::lock_guard<std::mutex> connection_lock(shared_connection->GetMutex());
    Private::AdbcError error = {};
    Handle<Private::ArrowArrayStream> stream = {};
    CHECK_ADBC(AdbcConnectionGetObjects(shared_connection->GetConnection(),
                                        depth, nullptr, schema_name, nullptr,
                                        nullptr, nullptr, stream.get(), &error),
               IOException);

    while (true) {
        Handle<Private::ArrowArray> batch = {};
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
                   [&exists](Private::ArrowArray *batch) {
                       Private::ArrowArray *catalog_schemas_list =
                           batch->children[1];
                       const int32_t *offsets =
                           (const int32_t *)catalog_schemas_list->buffers[1];
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
    ForEachCatalog(
        nullptr, ADBC_OBJECT_DEPTH_DB_SCHEMAS,
        [&schema_names](Private::ArrowArray *batch) {
            Private::ArrowArray *catalog_schemas_list = batch->children[1];
            for (int64_t i = 0; i < batch->length; ++i) {
                const int32_t *schema_offsets =
                    (const int32_t *)catalog_schemas_list->buffers[1];
                int32_t start_idx = schema_offsets[i];
                int32_t end_idx = schema_offsets[i + 1];

                Private::ArrowArray *schemas_struct =
                    catalog_schemas_list->children[0];
                Private::ArrowArray *name_array = schemas_struct->children[0];
                const int32_t *name_offsets =
                    (const int32_t *)name_array->buffers[1];
                const char *name_data = (const char *)name_array->buffers[2];

                for (int32_t j = start_idx; j < end_idx; ++j) {
                    int32_t name_start = name_offsets[j];
                    int32_t name_end = name_offsets[j + 1];
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
        [&table_names](Private::ArrowArray *batch) {
            // Get the catalogs
            Private::ArrowArray *catalogs = batch;
            Private::ArrowArray *catalog_schemas_list = batch->children[1];
            for (int64_t i = 0; i < catalogs->length; ++i) {
                const int32_t *schema_offsets =
                    (const int32_t *)catalog_schemas_list->buffers[1];
                int32_t schema_start = schema_offsets[i];
                int32_t schema_end = schema_offsets[i + 1];

                Private::ArrowArray *schemas_struct =
                    catalog_schemas_list->children[0];

                // Get the schemas for this catalog
                for (int32_t j = schema_start; j < schema_end; ++j) {
                    Private::ArrowArray *tables_list =
                        schemas_struct->children[1];
                    const int32_t *table_offsets =
                        (const int32_t *)tables_list->buffers[1];
                    int32_t table_start = table_offsets[j];
                    int32_t table_end = table_offsets[j + 1];

                    Private::ArrowArray *table_struct =
                        tables_list->children[0];
                    Private::ArrowArray *table_names_array =
                        table_struct->children[0];

                    const int32_t *name_offsets =
                        (const int32_t *)table_names_array->buffers[1];
                    const char *name_data =
                        (const char *)table_names_array->buffers[2];

                    // Get the tables for each schema
                    for (int32_t k = table_start; k < table_end; ++k) {
                        int32_t start = name_offsets[k];
                        int32_t end = name_offsets[k + 1];
                        table_names.emplace_back(name_data + start,
                                                 end - start);
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
