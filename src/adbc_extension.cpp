#define DUCKDB_EXTENSION_MAIN

#include "adbc_extension.hpp"
#include "adbc_execute.hpp"
#include "adbc_scan.hpp"
#include "adbc_storage.hpp"
#include "adbc_util.hpp"
#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"

namespace duckdb {

static void LoadInternal(ExtensionLoader &loader) {

  // Construct a read_adbc(uri, query) function to read from ADBC
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

  // Construct an adbc_execute(uri, query) to perform DML via ADBC
  TableFunction adbc_execute_function(
      "adbc_execute", {LogicalType::VARCHAR, LogicalType::VARCHAR},
      adbc::AdbcExecuteFunction, adbc::AdbcExecuteBindFunction);
  loader.RegisterFunction(adbc_execute_function);

  // Storage extension for ATTACH
  auto &config = DBConfig::GetConfig(loader.GetDatabaseInstance());
  config.storage_extensions["adbc"] = make_uniq<adbc::AdbcStorageExtension>();

  // Create a custom knob to control the buffer size for inserts
  config.AddExtensionOption(
      "adbc_insert_batch_size",
      "The number of chunks (default 1000) to buffer in memory before "
      "inserting via ADBC.",
      LogicalType::BIGINT, Value::BIGINT(1000),
      [](ClientContext &context, SetScope scope, Value &parameter) {
        if (parameter.GetValue<int64_t>() <= 0) {
          throw InvalidInputException(
              "adbc_insert_batch_size must be greater than zero!");
        }
      },
      SetScope::SESSION);

  // Create a custom knob to control the connection pool size per catalog
  config.AddExtensionOption(
      "adbc_connection_pool_size",
      "The number of connections (default 50) to pool (cache) before the "
      "catalog creates ephemeral connections to serve requests.",
      LogicalType::BIGINT, Value::BIGINT(50),
      [](ClientContext &context, SetScope scope, Value &parameter) {
        if (parameter.GetValue<int64_t>() <= 0) {
          throw InvalidInputException(
              "adbc_connection_pool_size must be greater than zero!");
        }
      },
      SetScope::SESSION);
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
