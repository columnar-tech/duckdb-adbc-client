#define DUCKDB_EXTENSION_MAIN

#include "adbc_extension.hpp"
#include "adbc-vendor/adbc.hpp"
#include "adbc-vendor/adbc_driver_manager.hpp"
#include "adbc_scan.hpp"
#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/function/table/arrow.hpp"
#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"

namespace duckdb {

static void LoadInternal(ExtensionLoader &loader) {

  // Construct a TableFunction which reads from ADBC given an input URI and SQL
  // query
  TableFunction read_adbc_function(
      "read_adbc",
      {LogicalType::VARCHAR, LogicalType::VARCHAR}, // Input URI, SQL query
      adbc::AdbcScanFunction,                       // Custom ADBC scan
      adbc::AdbcScanBindFunction,                   // Custom bind function
      ArrowTableFunction::ArrowScanInitGlobal,      // Use DuckDB's init
      ArrowTableFunction::ArrowScanInitLocal);      // Use DuckDB's init local

  // Duplicated ArrowFunction::ArrowScanCardinality as it is protected in DuckDB
  read_adbc_function.cardinality =
      [](ClientContext &context,
         const FunctionData *bind_data) -> unique_ptr<NodeStatistics> {
    return make_uniq<NodeStatistics>();
  };
  // Our scanner must project only the required columns
  read_adbc_function.projection_pushdown = true;
  // No support for filter pushdown
  read_adbc_function.filter_pushdown = false;
  loader.RegisterFunction(read_adbc_function);
}

void AdbcExtension::Load(ExtensionLoader &loader) { LoadInternal(loader); }
std::string AdbcExtension::Name() { return "adbc"; }

std::string AdbcExtension::Version() const {
#ifdef EXT_VERSION_ADBC
  return EXT_VERSION_ADBC;
#else
  return "";
#endif
}
} // namespace duckdb

extern "C" {

DUCKDB_CPP_EXTENSION_ENTRY(adbc, loader) { duckdb::LoadInternal(loader); }
}
