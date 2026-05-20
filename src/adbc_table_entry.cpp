#include "adbc_table_entry.hpp"
#include "adbc_scan.hpp"
#include "adbc_catalog.hpp"
#include "duckdb/main/client_context.hpp"

namespace duckdb {
namespace adbc {

AdbcTableEntry::AdbcTableEntry(Catalog &catalog, SchemaCatalogEntry &schema,
                               CreateTableInfo &info)
    : TableCatalogEntry(catalog, schema, info) {}

TableFunction
AdbcTableEntry::GetScanFunction(ClientContext &context,
                                unique_ptr<FunctionData> &bind_data) {
  auto &adbc_catalog = catalog.Cast<AdbcCatalog>();
  auto catalog_lock = adbc_catalog.AcquireScopedLock();

  // construct an ADBC scan function using a new connection object for the scan
  auto qualified_name = StringUtil::Format("\"%s\".\"%s\"", schema.name, name);
  auto sql = "SELECT * FROM " + qualified_name;
  auto adbc_arrow_stream_factory =
      make_uniq<AdbcArrowStreamFactory>(adbc_catalog.uri, sql);
  auto arrow_function_data = make_uniq<AdbcArrowScanFunctionData>(
      context, std::move(adbc_arrow_stream_factory));
  arrow_function_data->all_types = arrow_function_data->arrow_table.GetTypes();
  bind_data = std::move(arrow_function_data);

  TableFunction scan_adbc_function(
      "read_adbc", {},
      adbc::AdbcScanFunction,                  // Custom ADBC scan
      nullptr,                                 // Already bound
      ArrowTableFunction::ArrowScanInitGlobal, // Use DuckDB's init
      ArrowTableFunction::ArrowScanInitLocal); // Use DuckDB's init local
  scan_adbc_function.cardinality =
      [](ClientContext &context,
         const FunctionData *bind_data) -> unique_ptr<NodeStatistics> {
    return make_uniq<NodeStatistics>();
  };
  scan_adbc_function.projection_pushdown = true;
  scan_adbc_function.filter_pushdown = false;
  return scan_adbc_function;
}

unique_ptr<BaseStatistics> AdbcTableEntry::GetStatistics(ClientContext &context,
                                                         column_t column_id) {
  return nullptr;
}

TableStorageInfo AdbcTableEntry::GetStorageInfo(ClientContext &context) {
  return TableStorageInfo();
}

void AdbcTableEntry::BindUpdateConstraints(Binder &binder, LogicalGet &get,
                                           LogicalProjection &proj,
                                           LogicalUpdate &update,
                                           ClientContext &context) {
  throw NotImplementedException("UPDATE is not supported on ADBC tables");
}

} // namespace adbc
} // namespace duckdb
