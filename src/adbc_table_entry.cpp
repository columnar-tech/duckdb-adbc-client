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

static optional_idx GetTableCardinality(Private::AdbcConnection *connection,
                                        const char *schema_name,
                                        const char *table_name) {
    Handle<Private::AdbcError> error = {};
    Handle<ArrowArrayStream> stream = {};
    AdbcStatusCode stats_status =
        AdbcConnectionGetStatistics(connection, nullptr, schema_name, table_name, 1, stream.get(), error.get());
    if (stats_status == ADBC_STATUS_NOT_IMPLEMENTED) {
        return optional_idx();
    }
    CHECK_ADBC(stats_status, IOException);
    while (true) {
        Handle<ArrowArray> batch = {};
        if (stream->get_next(stream.get(), batch.get()) != 0 || batch->release == nullptr) {
            break;
        }
        if (batch->n_children < 2 || !batch->children[1]) {
            continue;
        }
        auto *schemas_list = static_cast<ArrowArray *>(batch->children[1]);
        if (schemas_list->length == 0 || schemas_list->n_children < 1 || !schemas_list->children[0]) {
            continue;
        }
        auto *schema_struct = static_cast<ArrowArray *>(schemas_list->children[0]);
        if (schema_struct->length == 0 || schema_struct->n_children < 2 || !schema_struct->children[1]) {
            continue;
        }
        auto *tables_list = static_cast<ArrowArray *>(schema_struct->children[1]);
        if (tables_list->length == 0 || tables_list->n_children < 1 || !tables_list->children[0]) {
            continue;
        }
        auto *table_struct = static_cast<ArrowArray *>(tables_list->children[0]);
        if (table_struct->length == 0 || table_struct->n_children < 4) {
            continue;
        }
        if (!table_struct->children[1] || !table_struct->children[2] || !table_struct->children[3]) {
            continue;
        }
        auto *stat_key_col = static_cast<ArrowArray *>(table_struct->children[2]);
        auto *stat_val_col = static_cast<ArrowArray *>(table_struct->children[3]);
        auto *col_name_col = static_cast<ArrowArray *>(table_struct->children[1]);
        if (stat_val_col->n_children < 3 || !stat_val_col->children[2]) {
            continue;
        }
        auto *double_child = static_cast<ArrowArray *>(stat_val_col->children[2]);
        if (!stat_key_col->buffers[1] || !stat_val_col->buffers[0] || !double_child->buffers[1]) {
            continue;
        }
        const auto *keys = static_cast<const int16_t *>(stat_key_col->buffers[1]);
        const auto *types = static_cast<const int8_t *>(stat_val_col->buffers[0]);
        const auto *values = static_cast<const double *>(double_child->buffers[1]);
        const auto *validity = static_cast<const uint8_t *>(col_name_col->buffers[0]);
        for (int64_t i = 0; i < table_struct->length; i++) {
            bool is_whole_table = !validity || !((validity[i / 8] >> (i % 8)) & 1);
            if (is_whole_table && keys[i] == ADBC_STATISTIC_ROW_COUNT_KEY && types[i] == 2) {
                return optional_idx(static_cast<idx_t>(values[i]));
            }
        }
    }
    return optional_idx();
}

AdbcTableEntry::AdbcTableEntry(Catalog &catalog, SchemaCatalogEntry &schema, CreateTableInfo &info)
    : TableCatalogEntry(catalog, schema, info) {
}

TableFunction AdbcTableEntry::GetScanFunction(ClientContext &context, unique_ptr<FunctionData> &bind_data) {

    auto &adbc_catalog = catalog.Cast<AdbcCatalog>();
    auto catalog_lock = adbc_catalog.AcquireScopedLock();

    // construct an ADBC scan function using a new connection object for the scan
    auto delimiter = adbc_catalog.GetDelimiter();
    auto pooled_connection = adbc_catalog.GetPooledConnection();
    auto cardinality = GetTableCardinality(pooled_connection->GetRawConnection(), schema.name.c_str(), name.c_str());

    auto quoted_schema_name = delimiter[0] + schema.name + delimiter[1];
    auto quoted_table_name = delimiter[0] + name + delimiter[1];
    auto sql = StringUtil::Format(" SELECT * FROM %s.%s", quoted_schema_name, quoted_table_name);
    auto adbc_arrow_stream_factory = make_uniq<AdbcArrowStreamFactory>(std::move(pooled_connection), sql);
    auto arrow_function_data = make_uniq<AdbcArrowScanFunctionData>(context, std::move(adbc_arrow_stream_factory));
    arrow_function_data->all_types = arrow_function_data->arrow_table.GetTypes();
    arrow_function_data->cardinality = cardinality;
    bind_data = std::move(arrow_function_data);

    TableFunction scan_adbc_function("read_adbc",
                                     {},
                                     adbc::AdbcScanFunction,                  // Use our own ADBC scan
                                     nullptr,                                 // Already bound
                                     ArrowTableFunction::ArrowScanInitGlobal, // Use DuckDB's init
                                     ArrowTableFunction::ArrowScanInitLocal); // Use DuckDB's init local
    scan_adbc_function.cardinality = [](ClientContext &context,
                                        const FunctionData *bind_data) -> unique_ptr<NodeStatistics> {
        auto &data = bind_data->Cast<AdbcArrowScanFunctionData>();
        if (data.cardinality.IsValid()) {
            return make_uniq<NodeStatistics>(data.cardinality.GetIndex());
        }
        return make_uniq<NodeStatistics>();
    };
    scan_adbc_function.projection_pushdown = true;
    scan_adbc_function.filter_pushdown = false;
    return scan_adbc_function;
}
} // namespace adbc
} // namespace duckdb
