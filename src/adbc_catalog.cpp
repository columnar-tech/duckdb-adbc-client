#include "duckdb.hpp"
#include "duckdb/storage/database_size.hpp"
#include "duckdb/parser/parsed_data/create_schema_info.hpp"
#include "adbc_catalog.hpp"

namespace duckdb {
namespace adbc {

AdbcCatalog::~AdbcCatalog() = default;

void AdbcCatalog::ScanSchemas(ClientContext &context, std::function<void(SchemaCatalogEntry &)> callback) {
	// TODO: Implement this
	// For each schema: execute callback()
}

optional_ptr<SchemaCatalogEntry> AdbcCatalog::LookupSchema(CatalogTransaction transaction,
                                                                const EntryLookupInfo &schema_lookup,
                                                                OnEntryNotFound if_not_found) {
	// TODO: Consider what happens if the schema doesn't actually exist
	const auto &name = schema_lookup.GetEntryName();

	auto it = schema_cache.find(name);
	if (it != schema_cache.end()) {
	    return it->second.get();
	}

	CreateSchemaInfo info;
	info.schema = name; 
	auto new_entry = make_uniq<AdbcSchemaEntry>(*this, info);
	auto ptr = new_entry.get();
	schema_cache[name] = std::move(new_entry);
	return ptr; 
}

optional_ptr<CatalogEntry> AdbcCatalog::CreateSchema(CatalogTransaction transaction, CreateSchemaInfo &info) {
	throw NotImplementedException("ADBC catalog is read-only");
}

void AdbcCatalog::DropSchema(ClientContext &context, DropInfo &info) {
	throw NotImplementedException("ADBC catalog is read-only");
}

DatabaseSize AdbcCatalog::GetDatabaseSize(ClientContext &context) {
	throw NotImplementedException("ADBC catalog does not support getting database size");
}

bool AdbcCatalog::InMemory() {
	return false;
}

string AdbcCatalog::GetDBPath() {
	return uri;
}

PhysicalOperator &AdbcCatalog::PlanCreateTableAs(ClientContext &context, PhysicalPlanGenerator &planner,
                                                      LogicalCreateTable &op, PhysicalOperator &plan) {
	throw NotImplementedException("ADBC catalog is read-only");
}

PhysicalOperator &AdbcCatalog::PlanInsert(ClientContext &context, PhysicalPlanGenerator &planner,
                                               LogicalInsert &op, optional_ptr<PhysicalOperator> plan) {
	throw NotImplementedException("ADBC catalog is read-only");
}

PhysicalOperator &AdbcCatalog::PlanDelete(ClientContext &context, PhysicalPlanGenerator &planner,
                                               LogicalDelete &op, PhysicalOperator &plan) {
	throw NotImplementedException("ADBC catalog is read-only");
}

PhysicalOperator &AdbcCatalog::PlanUpdate(ClientContext &context, PhysicalPlanGenerator &planner,
                                               LogicalUpdate &op, PhysicalOperator &plan) {
	throw NotImplementedException("ADBC catalog is read-only");
}

} // namespace adbc
} // namespace duckdb
