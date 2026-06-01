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

#include "adbc_table_entry.hpp"
#include "adbc_catalog.hpp"
#include "adbc_scan.hpp"

namespace duckdb {
namespace adbc {

AdbcTableEntry::AdbcTableEntry(Catalog &catalog, SchemaCatalogEntry &schema, CreateTableInfo &info)
    : TableCatalogEntry(catalog, schema, info) {
}

TableFunction AdbcTableEntry::GetScanFunction(ClientContext &context, unique_ptr<FunctionData> &bind_data) {

    auto &adbc_catalog = catalog.Cast<AdbcCatalog>();
    auto catalog_lock = adbc_catalog.AcquireScopedLock();

    // construct an ADBC scan function using a new connection object for the scan
    auto delimiter = adbc_catalog.GetDelimiter();
    auto quoted_schema_name = delimiter[0] + schema.name + delimiter[1];
    auto quoted_table_name = delimiter[0] + name + delimiter[1];
    auto sql = StringUtil::Format(" SELECT * FROM %s.%s", quoted_schema_name, quoted_table_name);
    auto adbc_arrow_stream_factory = make_uniq<AdbcArrowStreamFactory>(adbc_catalog.GetPooledConnection(), sql);
    auto arrow_function_data = make_uniq<AdbcArrowScanFunctionData>(context, std::move(adbc_arrow_stream_factory));
    arrow_function_data->all_types = arrow_function_data->arrow_table.GetTypes();
    bind_data = std::move(arrow_function_data);

    TableFunction scan_adbc_function("read_adbc",
                                     {},
                                     adbc::AdbcScanFunction,                  // Custom ADBC scan
                                     nullptr,                                 // Already bound
                                     ArrowTableFunction::ArrowScanInitGlobal, // Use DuckDB's init
                                     ArrowTableFunction::ArrowScanInitLocal); // Use DuckDB's init local
    scan_adbc_function.cardinality = [](ClientContext &context,
                                        const FunctionData *bind_data) -> unique_ptr<NodeStatistics> {
        return make_uniq<NodeStatistics>();
    };
    scan_adbc_function.projection_pushdown = true;
    scan_adbc_function.filter_pushdown = false;
    return scan_adbc_function;
}
} // namespace adbc
} // namespace duckdb
