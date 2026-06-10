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
#include "adbc_connection_pool.hpp"
#include "adbc_schema_entry.hpp"
#include "adbc_util.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/original/std/memory.hpp"
#include "duckdb/planner/parsed_data/bound_create_table_info.hpp"
#include "duckdb/storage/database_size.hpp"
#include <functional>

namespace duckdb {
namespace adbc {

class AdbcCatalog : public Catalog {
public:
    explicit AdbcCatalog(AttachedDatabase &db, ClientContext &context, const string &uri, const string &delimiter)
        : Catalog(db), context(context), uri(uri), delimiter(delimiter),
          pool(make_shared_ptr<AdbcConnectionPool>(uri, [&context]() {
              Value option_value;
              context.TryGetCurrentSetting("adbc_connection_pool_size", option_value);
              return option_value.GetValue<int64_t>();
          }())) {
        // Create a connection just to test that the URI is valid
        pool->GetConnection();

        auto schemas = FetchSchemaNames();
        no_schemas = (schemas.size() == 1 && schemas.front() == "");
    }

    bool NoSchemas() { return no_schemas; }

    string GetExternalSchemaName(const string& schema) {
        if (no_schemas && schema == "") {
            return "main";
        }
        return schema;
    }

    string GetInternalSchemaName(const string& schema) {
        if (no_schemas && schema == "main") {
            return "";
        }
        return schema;
    }

    string GetDelimitedInternalName(const string& schema, const string& table) {
        auto quoted_schema = delimiter[0] + GetInternalSchemaName(schema) + delimiter[1];
        auto quoted_table = delimiter[0] + table + delimiter[1];

        if (no_schemas) {
            return quoted_table;
        }
        return quoted_schema + "." + quoted_table;
    }

    unique_lock<std::recursive_mutex> AcquireScopedLock() {
        return unique_lock(mutex);
    }

    unique_ptr<AdbcPooledConnection> GetPooledConnection();
    vector<string> FetchTableNames(const string &schema_name);
    void Initialize(bool load_builtin) override {
    }
    string GetCatalogType() override {
        return "adbc";
    }

    void ClearCache();
    void ScanSchemas(ClientContext &context, std::function<void(SchemaCatalogEntry &)> callback) override;
    optional_ptr<SchemaCatalogEntry> LookupSchema(CatalogTransaction transaction,
                                                  const EntryLookupInfo &schema_lookup,
                                                  OnEntryNotFound if_not_found) override;
    CatalogLookupBehavior CatalogTypeLookupRule(CatalogType type) const override {
        if (type == CatalogType::TABLE_ENTRY) {
            return CatalogLookupBehavior::STANDARD;
        }
        return CatalogLookupBehavior::NEVER_LOOKUP;
    }
    optional_ptr<CatalogEntry> CreateSchema(CatalogTransaction transaction, CreateSchemaInfo &info) override {
        throw NotImplementedException("CREATE SCHEMA not yet supported with the ADBC extension");
    }
    void DropSchema(ClientContext &context, DropInfo &info) override {
        throw NotImplementedException("DROP SCHEMA not yet supported with the ADBC extension");
    }
    DatabaseSize GetDatabaseSize(ClientContext &context) override {
        throw NotImplementedException("Getting the database size is not yet "
                                      "supported with the ADBC extension");
        return DatabaseSize();
    }
    bool InMemory() override {
        return false;
    }
    string GetDBPath() override {
        return uri;
    }

    PhysicalOperator &PlanCreateTableAs(ClientContext &context,
                                        PhysicalPlanGenerator &planner,
                                        LogicalCreateTable &op,
                                        PhysicalOperator &plan) override;
    PhysicalOperator &PlanInsert(ClientContext &context,
                                 PhysicalPlanGenerator &planner,
                                 LogicalInsert &op,
                                 optional_ptr<PhysicalOperator> plan) override;
    PhysicalOperator &PlanDelete(ClientContext &context,
                                 PhysicalPlanGenerator &planner,
                                 LogicalDelete &op,
                                 PhysicalOperator &plan) override {
        throw NotImplementedException("DELETE not yet supported with the ADBC extension");
    }

    PhysicalOperator &PlanUpdate(ClientContext &context,
                                 PhysicalPlanGenerator &planner,
                                 LogicalUpdate &op,
                                 PhysicalOperator &plan) override {
        throw NotImplementedException("UPDATE not yet supported with the ADBC extension");
    }

private:
    void ForEachCatalog(const char *schema_name, int depth, const std::function<bool(ArrowArray *)> &callback);
    bool SchemaExists(const string &schema_name);
    vector<string> FetchSchemaNames();
    SchemaCatalogEntry *GetCatalogEntry(const string &schema_name);
    SchemaCatalogEntry *CreateCatalogEntry(const string &schema_name);
    bool ContainsAdbcReads(PhysicalOperator &op);

private:
    std::recursive_mutex mutex;
    ClientContext &context;
    string uri;
    string delimiter;
    shared_ptr<AdbcConnectionPool> pool;
    case_insensitive_map_t<unique_ptr<AdbcSchemaEntry>> owned_schemas;
    bool no_schemas;
};

} // namespace adbc
} // namespace duckdb
