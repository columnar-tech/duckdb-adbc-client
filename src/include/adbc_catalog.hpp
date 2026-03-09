#pragma once

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "duckdb/catalog/catalog_entry/view_catalog_entry.hpp"
#include "duckdb/parser/parsed_data/create_view_info.hpp"

namespace duckdb {
namespace adbc {

class AdbcSchemaEntry;

class AdbcCatalog : public Catalog {
public:
	explicit AdbcCatalog(AttachedDatabase &db, const string& uri) : Catalog(db), uri(uri) {} 

	~AdbcCatalog() override;

	const string& GetUri() const { return uri; }

	void Initialize(bool load_builtin) override {}
	string GetCatalogType() override {
		return "adbc";
	}
	void ScanSchemas(ClientContext &context, std::function<void(SchemaCatalogEntry &)> callback) override;
	optional_ptr<SchemaCatalogEntry> LookupSchema(CatalogTransaction transaction, const EntryLookupInfo &schema_lookup,
	                                              OnEntryNotFound if_not_found) override;
	CatalogLookupBehavior CatalogTypeLookupRule(CatalogType type) const override {
		if (type == CatalogType::TABLE_ENTRY) {
			return CatalogLookupBehavior::STANDARD;
		}
		return CatalogLookupBehavior::NEVER_LOOKUP;
	}
	optional_ptr<CatalogEntry> CreateSchema(CatalogTransaction transaction, CreateSchemaInfo &info) override;
	void DropSchema(ClientContext &context, DropInfo &info) override;
	DatabaseSize GetDatabaseSize(ClientContext &context) override;
	bool InMemory() override;
	string GetDBPath() override;
	PhysicalOperator &PlanCreateTableAs(ClientContext &context, PhysicalPlanGenerator &planner, LogicalCreateTable &op,
	                                    PhysicalOperator &plan) override;
	PhysicalOperator &PlanInsert(ClientContext &context, PhysicalPlanGenerator &planner, LogicalInsert &op,
	                             optional_ptr<PhysicalOperator> plan) override;
	PhysicalOperator &PlanDelete(ClientContext &context, PhysicalPlanGenerator &planner, LogicalDelete &op,
	                             PhysicalOperator &plan) override;
	PhysicalOperator &PlanUpdate(ClientContext &context, PhysicalPlanGenerator &planner, LogicalUpdate &op,
	                             PhysicalOperator &plan) override;

private:
	string uri;
	case_insensitive_map_t<unique_ptr<AdbcSchemaEntry>> schema_cache;
};

class AdbcSchemaEntry : public SchemaCatalogEntry {
public:
    AdbcSchemaEntry(Catalog &catalog, CreateSchemaInfo &info)
        : SchemaCatalogEntry(catalog, info) {}

    ~AdbcSchemaEntry() override;

    optional_ptr<CatalogEntry> LookupEntry(CatalogTransaction transaction, const EntryLookupInfo &lookup_info) override; 

         void Scan(CatalogType type, const std::function<void(CatalogEntry &)> &callback) override {
                throw NotImplementedException("ADBC does not support context-less scan");
        }

        void Scan(ClientContext &context, CatalogType type, const std::function<void(CatalogEntry &)> &callback) override;

        optional_ptr<CatalogEntry> CreateIndex(CatalogTransaction transaction, CreateIndexInfo &info,
                                                                     TableCatalogEntry &table) override {
                throw NotImplementedException("CreateIndex is not supported for ADBC schemas");
        }

        optional_ptr<CatalogEntry> CreateFunction(CatalogTransaction transaction,
                                                                        CreateFunctionInfo &info) override {
                throw NotImplementedException("CreateFunction is not supported for ADBC schemas");
        }

        optional_ptr<CatalogEntry> CreateTable(CatalogTransaction transaction,
                                                                     BoundCreateTableInfo &info) override {
                throw NotImplementedException("CreateTable is not supported for ADBC schemas");
        }

        optional_ptr<CatalogEntry> CreateView(CatalogTransaction transaction, CreateViewInfo &info) override {
                throw NotImplementedException("CreateView is not supported for ADBC schemas");
        }

        optional_ptr<CatalogEntry> CreateSequence(CatalogTransaction transaction,
                                                                        CreateSequenceInfo &info) override {
                throw NotImplementedException("CreateSequence is not supported for ADBC schemas");
        }

        optional_ptr<CatalogEntry> CreateTableFunction(CatalogTransaction transaction,
                                                                             CreateTableFunctionInfo &info) override {
                throw NotImplementedException("CreateTableFunction is not supported for ADBC schemas");
        }

        optional_ptr<CatalogEntry> CreateCopyFunction(CatalogTransaction transaction,
                                                                            CreateCopyFunctionInfo &info) override {
                throw NotImplementedException("CreateCopyFunction is not supported for ADBC schemas");
        }

        optional_ptr<CatalogEntry> CreatePragmaFunction(CatalogTransaction transaction,
                                                                              CreatePragmaFunctionInfo &info) override {
                throw NotImplementedException("CreatePragmaFunction is not supported for ADBC schemas");
        }

        optional_ptr<CatalogEntry> CreateCollation(CatalogTransaction transaction,
                                                                         CreateCollationInfo &info) override {
                throw NotImplementedException("CreateCollation is not supported for ADBC schemas");
        }

        optional_ptr<CatalogEntry> CreateType(CatalogTransaction transaction, CreateTypeInfo &info) override {
                throw NotImplementedException("CreateType is not supported for ADBC schemas");
        }

        void DropEntry(ClientContext &context, DropInfo &info) override {
                throw NotImplementedException("DropEntry is not supported for ADBC schemas");
        }

        void Alter(CatalogTransaction transaction, AlterInfo &info) override {
                throw NotImplementedException("Alter is not supported for ADBC schemas");
        }

private:
    case_insensitive_map_t<unique_ptr<CatalogEntry>> view_cache;

};


} // namespace adbc
} // namespace duckdb
