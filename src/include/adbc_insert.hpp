#pragma once

#include "duckdb/execution/physical_operator.hpp"
// #include "duckdb/catalog/catalog.hpp"
#include "duckdb/common/types/column/column_data_collection.hpp"

namespace duckdb {
namespace adbc {

enum class InsertMode { APPEND, CTAS };

class AdbcCatalog;

class AdbcInsert : public PhysicalOperator {
public:
  AdbcInsert(PhysicalPlan &physical_plan, LogicalOperator &op,
             const vector<LogicalType> &types, const vector<string> &names,
             const string &table_name, const string &schema_name,
             AdbcCatalog &catalog, InsertMode mode);

private:
  vector<LogicalType> column_types;
  vector<string> column_names;
  string table_name;
  string schema_name;
  AdbcCatalog &catalog;
  InsertMode insert_mode;

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

  bool IsSink() const override { return true; }

  bool ParallelSink() const override { return false; }

  string GetName() const override;
  InsertionOrderPreservingMap<string> ParamsToString() const override;
};

} // namespace adbc
} // namespace duckdb
