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

#include "adbc_schema_entry.hpp"
#include "adbc_catalog.hpp"
#include "adbc_scan.hpp"
#include "adbc_table_entry.hpp"
#include "duckdb/common/exception/catalog_exception.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"

namespace duckdb {
namespace adbc {

CatalogEntry *AdbcSchemaEntry::GetOrCreateTableEntry(ClientContext &context, const string &table_name) {

    // Check if the table exists
    {
        std::unique_lock<std::mutex> tables_lock(tables_mutex);

        // Return the entry if it already exists
        auto it = owned_tables.find(table_name);
        if (it != owned_tables.end()) {
            return it->second.get();
        }
    }

    // Create the entry to be inserted
    auto &adbc_catalog = catalog.Cast<AdbcCatalog>();

    // Bind a SQL statement and use ADBC to retrieve the metadata for the table
    string sql = "SELECT * FROM  " + adbc_catalog.GetDelimitedInternalName(name, table_name);

    auto factory = make_uniq<AdbcArrowStreamFactory>(adbc_catalog.GetPooledConnection(), sql);
    auto bind_data = make_uniq<AdbcArrowScanFunctionData>(context, std::move(factory));

    auto col_names = bind_data->arrow_table.GetNames();
    auto col_types = bind_data->arrow_table.GetTypes();
    auto table_info = make_uniq<CreateTableInfo>(*this, table_name);
    for (idx_t i = 0; i < col_names.size(); i++) {
        ColumnDefinition col(col_names[i], col_types[i]);
        table_info->columns.AddColumn(std::move(col));
    }
    table_info->internal = false;

    // Check again if the table exists
    {
        std::unique_lock<std::mutex> tables_lock(tables_mutex);

        // Return the entry if it already exists
        auto it = owned_tables.find(table_name);
        if (it != owned_tables.end()) {
            return it->second.get();
        }

        // Insert the entry
        auto table_entry = make_uniq<AdbcTableEntry>(catalog, *this, *table_info);
        auto ptr = table_entry.get();
        owned_tables[table_name] = std::move(table_entry);
        return ptr;
    }
    return nullptr;
}

optional_ptr<CatalogEntry> AdbcSchemaEntry::LookupEntry(CatalogTransaction transaction,
                                                        const EntryLookupInfo &lookup_info) {
    try {
        return GetOrCreateTableEntry(transaction.GetContext(), lookup_info.GetEntryName());
    } catch (...) {
        return nullptr;
    }
}

void AdbcSchemaEntry::Scan(ClientContext &context,
                           CatalogType type,
                           const std::function<void(CatalogEntry &)> &callback) {


    // We only support table lookups
    if (type != CatalogType::TABLE_ENTRY) {
        return;
    }


    auto &adbc_catalog = catalog.Cast<AdbcCatalog>();
    auto schema_name = adbc_catalog.GetInternalSchemaName(name);
    auto uri = adbc_catalog.GetDBPath();

    // For each ADBC table in the schema, get or create an entry for it
    for (const auto &table_name : adbc_catalog.FetchTableNames(schema_name)) {
        if (auto *entry = GetOrCreateTableEntry(context, table_name)) {
            callback(*entry);
        }
    }
}

optional_ptr<CatalogEntry> AdbcSchemaEntry::CreateTable(CatalogTransaction transaction, BoundCreateTableInfo &info) {

    auto &adbc_catalog = catalog.Cast<AdbcCatalog>();

    // Throw an exception if the entry already exists
    std::unique_lock<std::mutex> tables_lock(tables_mutex);
    auto table_name = info.Base().table;
    auto it = owned_tables.find(table_name);
    if (it != owned_tables.end()) {
        throw CatalogException::EntryAlreadyExists(CatalogType::TABLE_ENTRY, table_name);
    }

    throw NotImplementedException("CreateTable not yet supported with the ADBC extension");
}

} // namespace adbc
} // namespace duckdb
