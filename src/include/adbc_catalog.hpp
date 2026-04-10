#pragma once

#include "adbc_raii.hpp"
#include "adbc_scan.hpp"
#include "adbc_schema_entry.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/common/arrow/arrow.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "duckdb/common/index_vector.hpp"
#include "duckdb/execution/physical_operator.hpp"
#include <functional>
#include <mutex>
#include <shared_mutex>

namespace duckdb {
namespace adbc {

class AdbcInsert : public PhysicalOperator {
public:
  //! INSERT INTO
  AdbcInsert(PhysicalPlan &physical_plan, LogicalOperator &op,
             TableCatalogEntry &table,
             physical_index_vector_t<idx_t> column_index_map);

  //! The table to insert into
  optional_ptr<TableCatalogEntry> table;
  //! column_index_map
  physical_index_vector_t<idx_t> column_index_map;

public:
  // Source interface
  SourceResultType GetData(ExecutionContext &context, DataChunk &chunk,
                           OperatorSourceInput &input) const override;
  bool IsSource() const override { return true; }

public:
  // Sink interface
  unique_ptr<GlobalSinkState>
  GetGlobalSinkState(ClientContext &context) const override;
  SinkResultType Sink(ExecutionContext &context, DataChunk &chunk,
                      OperatorSinkInput &input) const override;
  SinkFinalizeType Finalize(Pipeline &pipeline, Event &event,
                            ClientContext &context,
                            OperatorSinkFinalizeInput &input) const override;

  void CreateArrowStreamFromCollection(ClientContext &context,
                                       ColumnDataCollection &collection,
                                       const vector<LogicalType> &types,
                                       const vector<string> &names,
                                       ArrowArrayStream *stream) const;

  bool IsSink() const override { return true; }

  bool ParallelSink() const override { return false; }

  string GetName() const override;
  InsertionOrderPreservingMap<string> ParamsToString() const override;
};

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
  void HandleAdbcScans(PhysicalOperator &op);
  void ForEachCatalog(const char *schema_name, int depth,
                      const std::function<bool(ArrowArray *)> &callback);

private:
  string uri;
  shared_ptr<SharedAdbcConnection> shared_connection;
  std::shared_mutex schemas_mutex;
  case_insensitive_map_t<unique_ptr<AdbcSchemaEntry>> owned_schemas;
};

} // namespace adbc
} // namespace duckdb
