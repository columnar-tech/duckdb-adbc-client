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
    case_insensitive_map_t<unique_ptr<CatalogEntry>> owned_tables;
};

} // namespace adbc
} // namespace duckdb
