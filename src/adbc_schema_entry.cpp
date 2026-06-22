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

void AdbcSchemaEntry::LazyLoadNewTables() {
    unique_lock<mutex> table_lock(tables_mutex);
    tables_loaded = false;
}

CatalogEntry *AdbcSchemaEntry::GetOrCreateTableEntryInternal(ClientContext &context, const string &table_name) {
    // Return the entry if it already exists
    auto it = owned_tables.find(table_name);
    if (it != owned_tables.end()) {
        return it->second.get();
    }

    // Table doesn't exist if it's not in the cache
    if (tables_loaded) {
        return nullptr;
    }

    // Create the entry to be inserted
    auto &adbc_catalog = catalog.Cast<AdbcCatalog>();

    // Bind a SQL statement and use ADBC to retrieve the metadata for the table
    string sql = "SELECT * FROM  " + adbc_catalog.GetDelimitedInternalName(name.GetIdentifierName(), table_name);

    auto factory = make_uniq<AdbcArrowStreamFactory>(adbc_catalog.GetPooledConnection(), sql);
    auto bind_data = make_uniq<AdbcArrowScanFunctionData>(context, std::move(factory));

    auto col_names = bind_data->arrow_table.GetNames();
    auto col_types = bind_data->arrow_table.GetTypes();
    auto table_info = make_uniq<CreateTableInfo>(*this, Identifier(table_name));
    for (idx_t i = 0; i < col_names.size(); i++) {
        ColumnDefinition col(Identifier(col_names[i]), col_types[i]);
        table_info->columns.AddColumn(std::move(col));
    }
    table_info->internal = false;

    // Insert the entry
    auto table_entry = make_uniq<AdbcTableEntry>(catalog, *this, *table_info);
    auto ptr = table_entry.get();
    owned_tables[table_name] = std::move(table_entry);
    return ptr;
}

CatalogEntry *AdbcSchemaEntry::GetOrCreateTableEntry(ClientContext &context, const string &table_name) {
    unique_lock<mutex> tables_lock(tables_mutex);
    return GetOrCreateTableEntryInternal(context, table_name);
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

    // Load the tables if we haven't already
    unique_lock<mutex> table_lock(tables_mutex);
    if (!tables_loaded) {


        // First fetch the table names for this schema
        auto &adbc_catalog = catalog.Cast<AdbcCatalog>();
        auto schema_name = adbc_catalog.GetInternalSchemaName(name.GetIdentifierName());
        auto table_names = adbc_catalog.FetchTableNames(schema_name);

        // Next fo reach table name, create an entry for it
        for (const auto &table_name : table_names) {
            GetOrCreateTableEntryInternal(context, table_name);
        }

        // Mark tables as loaded now
        tables_loaded = true;
    }

    // Now fire the callback for each entry
    for (auto &[_, entry] : owned_tables) {
        callback(*entry);
    }
}

optional_ptr<CatalogEntry> AdbcSchemaEntry::CreateTable(CatalogTransaction transaction, BoundCreateTableInfo &info) {

    auto &adbc_catalog = catalog.Cast<AdbcCatalog>();
    auto &context = transaction.GetContext();
    auto table_name = info.Base().table;

    // Guard against OR REPLACE qualifier
    if (info.Base().on_conflict == OnCreateConflict::REPLACE_ON_CONFLICT) {
        throw NotImplementedException("CREATE OR REPLACE TABLE not yet supported with the ADBC extension");
    }

    {
        // Throw an exception if the entry already exists
        unique_lock<mutex> tables_lock(tables_mutex);
        auto it = owned_tables.find(table_name.GetIdentifierName());
        if (it != owned_tables.end()) {
            throw CatalogException::EntryAlreadyExists(CatalogType::TABLE_ENTRY, table_name);
        }

        auto connection = adbc_catalog.GetPooledConnection();
        auto *raw_conn = connection->GetRawConnection();
        Handle<Private::AdbcStatement> statement = {};
        Handle<ArrowSchema> schema = {};
        Handle<ArrowArrayStream> stream = {};
        Handle<Private::AdbcError> error = {};

        // Build the Arrow schema from the column types/names
        auto properties = context.GetClientProperties();
        vector<LogicalType> column_types;
        vector<string> column_names;
        for (auto &col : info.Base().columns.Logical()) {
            column_types.push_back(col.GetType());
            column_names.push_back(col.GetName().GetIdentifierName());
        }
        auto internal_schema = adbc_catalog.GetInternalSchemaName(info.Base().schema.GetIdentifierName());


        ArrowConverter::ToArrowSchema(schema.get(), column_types, column_names, properties);

        // Manually wire up a minimal ArrowArrayStream backed by the schema.
        // The stream holds a raw pointer to the schema so it can serve get_schema,
        // then immediately returns end-of-stream (0) on get_next.
        stream->private_data = schema.get();

        stream->get_schema = [](ArrowArrayStream *s, ArrowSchema *out) -> int {
            auto *src = static_cast<ArrowSchema *>(s->private_data);
            ArrowSchemaMove(src, out);
            return 0;
        };

        stream->get_next = [](ArrowArrayStream *, ArrowArray *out) -> int {
            out->release = nullptr; // signals end-of-stream
            return 0;
        };

        stream->get_last_error = [](ArrowArrayStream *) -> const char * { return nullptr; };

        stream->release = [](ArrowArrayStream *s) {
            s->private_data = nullptr;
            s->release = nullptr;
        };

        // Create a statement to do the CREATE TABLE
        CHECK_ADBC(AdbcStatementNew(raw_conn, statement.get(), error.get()), IOException);

        // Set the schema name
        if (!internal_schema.empty()) {
            CHECK_ADBC(AdbcStatementSetOption(statement.get(),
                                              ADBC_INGEST_OPTION_TARGET_DB_SCHEMA,
                                              internal_schema.c_str(),
                                              error.get()),
                       IOException);
        }

        // Set the table name
        CHECK_ADBC(
            AdbcStatementSetOption(statement.get(), ADBC_INGEST_OPTION_TARGET_TABLE, table_name.c_str(), error.get()),
            IOException);

        // Bind the empty stream — this creates the table schema without inserting rows
        CHECK_ADBC(AdbcStatementBindStream(statement.get(), stream.get(), error.get()), IOException);
        CHECK_ADBC(AdbcStatementExecuteQuery(statement.get(), nullptr, nullptr, error.get()), IOException);
    }

    // Return the entry
    unique_lock<mutex> tables_lock(tables_mutex);
    tables_loaded = false;
    auto *entry = GetOrCreateTableEntryInternal(context, table_name.GetIdentifierName());
    tables_loaded = true;
    return entry;
}

} // namespace adbc
} // namespace duckdb
