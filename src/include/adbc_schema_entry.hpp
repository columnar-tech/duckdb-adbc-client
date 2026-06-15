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

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/original/std/memory.hpp"
#include "duckdb/planner/parsed_data/bound_create_table_info.hpp"

namespace duckdb {
namespace adbc {

class AdbcSchemaEntry : public SchemaCatalogEntry {
public:
    AdbcSchemaEntry(Catalog &catalog, CreateSchemaInfo &info) : SchemaCatalogEntry(catalog, info) {
    }

    CatalogEntry *GetOrCreateTableEntry(ClientContext &context, const string &table_name);

    optional_ptr<CatalogEntry> LookupEntry(CatalogTransaction transaction, const EntryLookupInfo &lookup_info) override;

    void Scan(CatalogType type, const std::function<void(CatalogEntry &)> &callback) override {
        throw NotImplementedException("ADBC does not support context-less scan");
    }

    void Scan(ClientContext &context, CatalogType type, const std::function<void(CatalogEntry &)> &callback) override;

    optional_ptr<CatalogEntry> CreateIndex(CatalogTransaction transaction,
                                           CreateIndexInfo &info,
                                           TableCatalogEntry &table) override {
        throw NotImplementedException("CreateIndex not yet supported with the ADBC extension");
    }

    optional_ptr<CatalogEntry> CreateFunction(CatalogTransaction transaction, CreateFunctionInfo &info) override {
        throw NotImplementedException("CreateFunction not yet supported with the ADBC extension");
    }

    optional_ptr<CatalogEntry> CreateTable(CatalogTransaction transaction, BoundCreateTableInfo &info) override;

    optional_ptr<CatalogEntry> CreateView(CatalogTransaction transaction, CreateViewInfo &info) override {
        throw NotImplementedException("CreateView not yet supported with the ADBC extension");
    }

    optional_ptr<CatalogEntry> CreateSequence(CatalogTransaction transaction, CreateSequenceInfo &info) override {
        throw NotImplementedException("CreateSequence not yet supported with the ADBC extension");
    }

    optional_ptr<CatalogEntry> CreateTableFunction(CatalogTransaction transaction,
                                                   CreateTableFunctionInfo &info) override {
        throw NotImplementedException("CreateTableFunction not yet supported with the ADBC extension");
    }

    optional_ptr<CatalogEntry> CreateCopyFunction(CatalogTransaction transaction,
                                                  CreateCopyFunctionInfo &info) override {
        throw NotImplementedException("CreateCopyFunction not yet supported with the ADBC extension");
    }

    optional_ptr<CatalogEntry> CreatePragmaFunction(CatalogTransaction transaction,
                                                    CreatePragmaFunctionInfo &info) override {
        throw NotImplementedException("CreatePragmaFunction not yet supported with the ADBC extension");
    }

    optional_ptr<CatalogEntry> CreateCollation(CatalogTransaction transaction, CreateCollationInfo &info) override {
        throw NotImplementedException("CreateCollation not yet supported with the ADBC extension");
    }

    optional_ptr<CatalogEntry> CreateType(CatalogTransaction transaction, CreateTypeInfo &info) override {
        throw NotImplementedException("CreateType not yet supported with the ADBC extension");
    }

    void DropEntry(ClientContext &context, DropInfo &info) override {
        throw NotImplementedException("DropEntry not yet supported with the ADBC extension");
    }

    void Alter(CatalogTransaction transaction, AlterInfo &info) override {
        throw NotImplementedException("Alter not yet supported with the ADBC extension");
    }

private:
    std::mutex tables_mutex;
    case_insensitive_map_t<unique_ptr<CatalogEntry>> owned_tables;
};

} // namespace adbc
} // namespace duckdb
