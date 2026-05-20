#pragma once

#include "adbc_insert.hpp"
#include "adbc_raii.hpp"
#include "adbc_scan.hpp"
#include "adbc_schema_entry.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/common/arrow/arrow.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "duckdb/planner/parsed_data/bound_create_table_info.hpp"
#include "duckdb/storage/database_size.hpp"
#include <functional>
#include <mutex>

namespace duckdb {
namespace adbc {

class AdbcCatalog : public Catalog {
public:
  friend class AdbcSchemaEntry;
  friend class AdbcTableEntry;
  friend class AdbcTransactionManager;

  explicit AdbcCatalog(AttachedDatabase &db, const string &uri)
      : Catalog(db), uri(uri),
        shared_connection(make_shared_ptr<SharedAdbcConnection>()) {
    // Initialize the regular connection
    InitializeDatabase(*shared_connection, uri);
    InitializeConnection(*shared_connection);
  }

  shared_ptr<SharedAdbcConnection> GetSharedConnection() {
    return shared_connection;
  }

  void Initialize(bool load_builtin) override {}
  string GetCatalogType() override { return "adbc"; }

  void ScanSchemas(ClientContext &context,
                   std::function<void(SchemaCatalogEntry &)> callback) override;
  optional_ptr<SchemaCatalogEntry>
  LookupSchema(CatalogTransaction transaction,
               const EntryLookupInfo &schema_lookup,
               OnEntryNotFound if_not_found) override;
  CatalogLookupBehavior CatalogTypeLookupRule(CatalogType type) const override {
    if (type == CatalogType::TABLE_ENTRY) {
      return CatalogLookupBehavior::STANDARD;
    }
    return CatalogLookupBehavior::NEVER_LOOKUP;
  }
  optional_ptr<CatalogEntry> CreateSchema(CatalogTransaction transaction,
                                          CreateSchemaInfo &info) override {
    throw NotImplementedException(
        "CREATE SCHEMA not yet supported with the ADBC extension");
  }
  void DropSchema(ClientContext &context, DropInfo &info) override {
    throw NotImplementedException(
        "DROP SCHEMA not yet supported with the ADBC extension");
  }
  DatabaseSize GetDatabaseSize(ClientContext &context) override {
    throw NotImplementedException("Getting the database size is not yet "
                                  "supported with the ADBC extension");
    return DatabaseSize();
  }
  bool InMemory() override { return false; }
  string GetDBPath() override { return uri; }
  PhysicalOperator &PlanCreateTableAs(ClientContext &context,
                                      PhysicalPlanGenerator &planner,
                                      LogicalCreateTable &op,
                                      PhysicalOperator &plan) override;
  PhysicalOperator &PlanInsert(ClientContext &context,
                               PhysicalPlanGenerator &planner,
                               LogicalInsert &op,
                               optional_ptr<PhysicalOperator> plan) override;
  PhysicalOperator &PlanDelete(ClientContext &context,
                               PhysicalPlanGenerator &planner,
                               LogicalDelete &op,
                               PhysicalOperator &plan) override {
    throw NotImplementedException(
        "DELETE not yet supported with the ADBC extension");
  }

  PhysicalOperator &PlanUpdate(ClientContext &context,
                               PhysicalPlanGenerator &planner,
                               LogicalUpdate &op,
                               PhysicalOperator &plan) override {
    throw NotImplementedException(
        "UPDATE not yet supported with the ADBC extension");
  }

private:
  std::unique_lock<std::recursive_mutex> AcquireScopedLock() {
    return std::unique_lock(mutex);
  }

  // These methods assume that the catalog lock is held
  void ForEachCatalog(const char *schema_name, int depth,
                      const std::function<bool(ArrowArray *)> &callback);
  bool SchemaExists(const string &schema_name);
  vector<string> FetchSchemaNames();
  vector<string> FetchTableNames(const string &schema_name);
  SchemaCatalogEntry *GetCatalogEntry(const string &schema_name);
  SchemaCatalogEntry *CreateCatalogEntry(const string &schema_name);
  bool AutocommitEnabled();

private:
  string uri;
  shared_ptr<SharedAdbcConnection> shared_connection;
  std::recursive_mutex mutex;
  case_insensitive_map_t<unique_ptr<AdbcSchemaEntry>> owned_schemas;
};

} // namespace adbc
} // namespace duckdb
