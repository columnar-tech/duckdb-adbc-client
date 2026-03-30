#pragma once

#include <functional>
#include <mutex>
#include <shared_mutex>
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "adbc_raii.hpp"
#include "adbc_scan.hpp"
#include "adbc_schema_entry.hpp"

namespace duckdb {
namespace adbc {

class AdbcCatalog : public Catalog {
public:
  explicit AdbcCatalog(AttachedDatabase &db, const string &uri)
      : Catalog(db), uri(uri),
        shared_connection(make_shared_ptr<SharedAdbcConnection>()) {
    InitializeDatabase(*shared_connection, uri);
    InitializeConnection(*shared_connection);
  }

  shared_ptr<SharedAdbcConnection> GetSharedConnection() {
    return shared_connection;
  }
  bool SchemaExists(const string &schema_name);
  vector<string> FetchSchemaNames();
  vector<string> FetchTableNames(const string &schema_name);

  const string &GetUri() const { return uri; }

  void Initialize(bool load_builtin) override {}
  string GetCatalogType() override { return "adbc"; }

  SchemaCatalogEntry *GetCatalogEntry(const string &schema_name);
  SchemaCatalogEntry *CreateCatalogEntry(const string &schema_name);
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
                                          CreateSchemaInfo &info) override;
  void DropSchema(ClientContext &context, DropInfo &info) override;
  DatabaseSize GetDatabaseSize(ClientContext &context) override;
  bool InMemory() override;
  string GetDBPath() override;
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
                               PhysicalOperator &plan) override;
  PhysicalOperator &PlanUpdate(ClientContext &context,
                               PhysicalPlanGenerator &planner,
                               LogicalUpdate &op,
                               PhysicalOperator &plan) override;

private:
  void ForEachCatalog(
    const char *schema_name, int depth,
    const std::function<bool(Private::ArrowArray *)> &callback); 

private:
  string uri;
  shared_ptr<SharedAdbcConnection> shared_connection;
  std::shared_mutex schemas_mutex;
  case_insensitive_map_t<unique_ptr<AdbcSchemaEntry>> owned_schemas;
};

} // namespace adbc
} // namespace duckdb
