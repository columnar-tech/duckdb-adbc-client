#pragma once

#include <shared_mutex>
#include "duckdb/catalog/catalog.hpp"

namespace duckdb {
namespace adbc { 	

class AdbcSchemaEntry : public SchemaCatalogEntry {
public:
  AdbcSchemaEntry(Catalog &catalog, CreateSchemaInfo &info)
      : SchemaCatalogEntry(catalog, info) {}

  optional_ptr<CatalogEntry> CreateTableEntry(ClientContext &context, const string &table_name);

  optional_ptr<CatalogEntry>
  LookupEntry(CatalogTransaction transaction,
              const EntryLookupInfo &lookup_info) override;

  void Scan(CatalogType type,
            const std::function<void(CatalogEntry &)> &callback) override {
    throw NotImplementedException("ADBC does not support context-less scan");
  }

  void Scan(ClientContext &context, CatalogType type,
            const std::function<void(CatalogEntry &)> &callback) override;

  optional_ptr<CatalogEntry> CreateIndex(CatalogTransaction transaction,
                                         CreateIndexInfo &info,
                                         TableCatalogEntry &table) override {
    throw NotImplementedException(
        "CreateIndex is not supported for ADBC schemas");
  }

  optional_ptr<CatalogEntry> CreateFunction(CatalogTransaction transaction,
                                            CreateFunctionInfo &info) override {
    throw NotImplementedException(
        "CreateFunction is not supported for ADBC schemas");
  }

  optional_ptr<CatalogEntry> CreateTable(CatalogTransaction transaction,
                                         BoundCreateTableInfo &info) override {
    throw NotImplementedException(
        "CreateTable is not supported for ADBC schemas");
  }

  optional_ptr<CatalogEntry> CreateView(CatalogTransaction transaction,
                                        CreateViewInfo &info) override {
    throw NotImplementedException(
        "CreateView is not supported for ADBC schemas");
  }

  optional_ptr<CatalogEntry> CreateSequence(CatalogTransaction transaction,
                                            CreateSequenceInfo &info) override {
    throw NotImplementedException(
        "CreateSequence is not supported for ADBC schemas");
  }

  optional_ptr<CatalogEntry>
  CreateTableFunction(CatalogTransaction transaction,
                      CreateTableFunctionInfo &info) override {
    throw NotImplementedException(
        "CreateTableFunction is not supported for ADBC schemas");
  }

  optional_ptr<CatalogEntry>
  CreateCopyFunction(CatalogTransaction transaction,
                     CreateCopyFunctionInfo &info) override {
    throw NotImplementedException(
        "CreateCopyFunction is not supported for ADBC schemas");
  }

  optional_ptr<CatalogEntry>
  CreatePragmaFunction(CatalogTransaction transaction,
                       CreatePragmaFunctionInfo &info) override {
    throw NotImplementedException(
        "CreatePragmaFunction is not supported for ADBC schemas");
  }

  optional_ptr<CatalogEntry>
  CreateCollation(CatalogTransaction transaction,
                  CreateCollationInfo &info) override {
    throw NotImplementedException(
        "CreateCollation is not supported for ADBC schemas");
  }

  optional_ptr<CatalogEntry> CreateType(CatalogTransaction transaction,
                                        CreateTypeInfo &info) override {
    throw NotImplementedException(
        "CreateType is not supported for ADBC schemas");
  }

  void DropEntry(ClientContext &context, DropInfo &info) override {
    throw NotImplementedException(
        "DropEntry is not supported for ADBC schemas");
  }

  void Alter(CatalogTransaction transaction, AlterInfo &info) override {
    throw NotImplementedException("Alter is not supported for ADBC schemas");
  }

private:
  std::shared_mutex tables_mutex;
  case_insensitive_map_t<unique_ptr<CatalogEntry>> owned_tables;
};

} // namespace adbc
} // namespace duckdb
