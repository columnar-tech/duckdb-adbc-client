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

#pragma once

#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/storage/table_storage_info.hpp"

namespace duckdb {
namespace adbc {

class AdbcTableEntry : public TableCatalogEntry {
public:
    AdbcTableEntry(Catalog &catalog, SchemaCatalogEntry &schema, CreateTableInfo &info);

    TableFunction GetScanFunction(ClientContext &context, unique_ptr<FunctionData> &bind_data) override;

    unique_ptr<BaseStatistics> GetStatistics(ClientContext &context, column_t column_id) override {
        return nullptr;
    }

    TableStorageInfo GetStorageInfo(ClientContext &context) override {
        return TableStorageInfo();
    }

    void BindUpdateConstraints(Binder &binder,
                               LogicalGet &get,
                               LogicalProjection &proj,
                               LogicalUpdate &update,
                               ClientContext &context) override {

        throw NotImplementedException("UPDATE is not supported on ADBC tables");
    }
};

} // namespace adbc
} // namespace duckdb
