// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.


#define DUCKDB_EXTENSION_MAIN

#include "adbc_extension.hpp"
#include "adbc_clear_cache.hpp"
#include "adbc_execute.hpp"
#include "adbc_scan.hpp"
#include "adbc_storage.hpp"
#include "adbc_util.hpp"
#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"

namespace duckdb {

static void LoadInternal(ExtensionLoader &loader) {

    // Construct a read_adbc(uri, query) function to read from ADBC
    TableFunction read_adbc_function("read_adbc",
                                     {LogicalType::VARCHAR, LogicalType::VARCHAR}, // Input URI, SQL query
                                     adbc::AdbcScanFunction,                       // Use our own ADBC scan
                                     adbc::AdbcScanBindFunction,                   // Custom bind function
                                     ArrowTableFunction::ArrowScanInitGlobal,      // Use DuckDB's init
                                     ArrowTableFunction::ArrowScanInitLocal);      // Use DuckDB's init local

    // Duplicated ArrowFunction::ArrowScanCardinality as it is protected in DuckDB
    read_adbc_function.cardinality = [](ClientContext &context,
                                        const FunctionData *bind_data) -> unique_ptr<NodeStatistics> {
        return make_uniq<NodeStatistics>();
    };
    // Our scanner must project only the required columns
    read_adbc_function.projection_pushdown = true;
    // No support for filter pushdown
    read_adbc_function.filter_pushdown = false;

    // Function description for read_adbc
    CreateTableFunctionInfo read_adbc_info(read_adbc_function);
    FunctionDescription read_adbc_desc;
    read_adbc_desc.parameter_names = {"adbc_connection_profile_uri", "sql_query"};
    read_adbc_desc.description = "Executes a SQL query against a remote database via an ADBC connection profile URI "
                                 "and returns the result as a DuckDB table.";
    read_adbc_desc.examples = {"SELECT * FROM read_adbc('profile://mydb', 'SELECT * FROM games');"};
    read_adbc_desc.categories = {"adbc"};
    read_adbc_info.descriptions.push_back(read_adbc_desc);
    loader.RegisterFunction(std::move(read_adbc_info));

    // Construct an adbc_execute(uri, query) function to perform DML via ADBC
    TableFunction adbc_execute_function("adbc_execute",
                                        {LogicalType::VARCHAR, LogicalType::VARCHAR},
                                        adbc::AdbcExecuteFunction,
                                        adbc::AdbcExecuteBindFunction);
    CreateTableFunctionInfo adbc_execute_info(adbc_execute_function);
    FunctionDescription adbc_execute_desc;
    adbc_execute_desc.parameter_names = {"adbc_connection_profile_uri", "sql_query"};
    adbc_execute_desc.description =
        "Executes an SQL statement (e.g. DROP TABLE, CREATE INDEX) against a remote ADBC database.";
    adbc_execute_desc.examples = {"CALL adbc_execute('profile://mydb', 'DROP TABLE games');"};
    adbc_execute_desc.categories = {"adbc"};
    adbc_execute_info.descriptions.push_back(adbc_execute_desc);
    loader.RegisterFunction(std::move(adbc_execute_info));

    // Construct an adbc_clear_cache function to clear catalog metadata
    TableFunction adbc_clear_cache_function("adbc_clear_cache",
                                            {},
                                            adbc::AdbcClearCacheFunction,
                                            adbc::AdbcClearCacheBindFunction);
    CreateTableFunctionInfo adbc_clear_cache_info(adbc_clear_cache_function);
    FunctionDescription adbc_clear_cache_desc;
    adbc_clear_cache_desc.parameter_names = {};
    adbc_clear_cache_desc.description = "Clears DuckDB's locally cached metadata for attached ADBC databases. Forces "
                                        "catalog and schema information to be fetched from the ADBC database.";
    adbc_clear_cache_desc.examples = {"CALL adbc_clear_cache();"};
    adbc_clear_cache_desc.categories = {"adbc"};
    adbc_clear_cache_info.descriptions.push_back(adbc_clear_cache_desc);
    loader.RegisterFunction(std::move(adbc_clear_cache_info));

    // Storage extension for ATTACH
    auto &config = DBConfig::GetConfig(loader.GetDatabaseInstance());
    StorageExtension::Register(config, "adbc", make_uniq<adbc::AdbcStorageExtension>());
    // Create a custom knob to control whether concurrent ADBC reads and writes
    // are supported
    config.AddExtensionOption("adbc_mix_reads_writes",
                              "Whether ADBC reads and writes can be mixed within "
                              "the same SQL statement (default false).",
                              LogicalType::BOOLEAN,
                              Value::BOOLEAN(false),
                              nullptr,
                              SetScope::SESSION);

    // Create a custom knob to control whether ADBC inputs are fully materialized
    config.AddExtensionOption("adbc_materialize_insert_rows",
                              "Whether input rows for INSERTs are materialized before inserting via ADBC.",
                              LogicalType::BOOLEAN,
                              Value::BOOLEAN(false),
                              nullptr,
                              SetScope::SESSION);

    // Create a custom knob to control the buffer size for inserts
    config.AddExtensionOption(
        "adbc_insert_buffer_size",
        "The number of chunks (default 1000) to buffer in memory before "
        "inserting via ADBC.",
        LogicalType::BIGINT,
        Value::BIGINT(1000),
        [](ClientContext &context, SetScope scope, Value &parameter) {
            if (parameter.GetValue<int64_t>() <= 0) {
                throw InvalidInputException("adbc_insert_buffer_size must be greater than zero!");
            }
        },
        SetScope::SESSION);

    // Create a custom knob to control the connection pool size per catalog
    config.AddExtensionOption(
        "adbc_connection_pool_size",
        "The number of connections (default 50) to pool (cache) before the "
        "catalog creates ephemeral connections to serve requests.",
        LogicalType::BIGINT,
        Value::BIGINT(50),
        [](ClientContext &context, SetScope scope, Value &parameter) {
            if (parameter.GetValue<int64_t>() <= 0) {
                throw InvalidInputException("adbc_connection_pool_size must be greater than zero!");
            }
        },
        SetScope::SESSION);
}

void AdbcExtension::Load(ExtensionLoader &loader) {
    LoadInternal(loader);
}
std::string AdbcExtension::Name() {
    return "adbc";
}

std::string AdbcExtension::Version() const {
#ifdef EXT_VERSION_ADBC
    return EXT_VERSION_ADBC;
#else
    return "";
#endif
}
} // namespace duckdb

extern "C" {
DUCKDB_CPP_EXTENSION_ENTRY(adbc, loader) {
    duckdb::LoadInternal(loader);
}
}
