#include "duckdb.hpp"
#include "adbc_catalog.hpp"
#include "duckdb/catalog/catalog_entry/view_catalog_entry.hpp"
#include "duckdb/parser/parsed_data/create_view_info.hpp"

namespace duckdb {
namespace adbc {

    AdbcSchemaEntry::~AdbcSchemaEntry() = default;

    optional_ptr<CatalogEntry> AdbcSchemaEntry::LookupEntry(CatalogTransaction transaction, const EntryLookupInfo &lookup_info)  {
	
	auto &name = lookup_info.GetEntryName();

	// See if we already have a view created for it
	auto it = view_cache.find(name);
	if (it != view_cache.end()) {
	    return it->second.get();
	}

	auto &adbc_catalog = catalog.Cast<AdbcCatalog>();
	auto &context = transaction.GetContext();

	// Bind a SELECT * FROM read_adbc(uri, SELECT * FROM <table>) statement	
	string sql = StringUtil::Format("SELECT * FROM read_adbc('%s', 'SELECT * FROM \"%s\".\"%s\"')", 
                                    adbc_catalog.GetUri(), this->name, name);
	
	auto select_stmt = CreateViewInfo::ParseSelect(sql);
	auto binder = Binder::CreateBinder(context);
	auto bound_statement = binder->Bind((SQLStatement&)*select_stmt);

	// Construct a VIEW from the statement (copying metadata from the binder)
	auto view_info = make_uniq<CreateViewInfo>();
	view_info->schema = this->name;
	view_info->view_name = name;
	view_info->query = CreateViewInfo::ParseSelect(sql); 
	view_info->aliases = bound_statement.names;
	view_info->types = bound_statement.types;
	view_info->temporary = true;
	view_info->internal = true; 

	// Return the VIEW as a catalog entry for the query
	auto view_entry = make_uniq<ViewCatalogEntry>(catalog, *this, *view_info);
	auto ptr = view_entry.get();
	view_cache[name] = std::move(view_entry);
	return ptr;

    }
	void AdbcSchemaEntry::Scan(ClientContext &context, CatalogType type, const std::function<void(CatalogEntry &)> &callback)  {
		if (type != CatalogType::TABLE_ENTRY) { return; }

		auto &adbc_catalog = catalog.Cast<AdbcCatalog>();
		// TODO: For each table, execute the callback
	}



} // namespace adbc
} // namespace duckdb
