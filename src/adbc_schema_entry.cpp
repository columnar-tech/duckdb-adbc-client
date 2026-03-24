#include "adbc_catalog.hpp"
#include "adbc_scan.hpp"
#include "adbc_schema_entry.hpp"
#include "adbc_table_entry.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "duckdb/parser/parsed_data/create_view_info.hpp"
#include "duckdb.hpp"

namespace duckdb {
namespace adbc {

optional_ptr<CatalogEntry> AdbcSchemaEntry::CreateTableEntry(ClientContext &context,
                                       const string &table_name) {
    auto &adbc_catalog = catalog.Cast<AdbcCatalog>();
    auto schema_name = this->name;

    string sql =
        StringUtil::Format("SELECT * FROM read_adbc('%s', 'SELECT * FROM "
                           "\"%s\".\"%s\" LIMIT 0')",
                           adbc_catalog.GetUri(), schema_name, table_name);

    auto select_stmt = CreateViewInfo::ParseSelect(sql);
    auto binder = Binder::CreateBinder(context);
    auto bound_statement = binder->Bind((SQLStatement &)*select_stmt);

    auto table_info = make_uniq<CreateTableInfo>(*this, table_name);
    for (idx_t i = 0; i < bound_statement.names.size(); i++) {
        // Construct the column definition directly
        ColumnDefinition col(bound_statement.names[i],
                             bound_statement.types[i]);
        table_info->columns.AddColumn(std::move(col));
    }
    table_info->internal = false;

    auto table_entry = make_uniq<AdbcTableEntry>(catalog, *this, *table_info,
                                                 adbc_catalog.GetUri());
    auto ptr = table_entry.get();
    owned_tables[table_name] = std::move(table_entry);
    return ptr;
}

optional_ptr<CatalogEntry>
AdbcSchemaEntry::LookupEntry(CatalogTransaction transaction,
                             const EntryLookupInfo &lookup_info) {
    auto &table_name = lookup_info.GetEntryName();
    if (owned_tables.find(table_name) == owned_tables.end()) {
        return CreateTableEntry(transaction.GetContext(), table_name);
    }
    return owned_tables[table_name].get();
}

void AdbcSchemaEntry::Scan(
    ClientContext &context, CatalogType type,
    const std::function<void(CatalogEntry &)> &callback) {

    if (type != CatalogType::TABLE_ENTRY) {
        return;
    }

    auto &adbc_catalog = catalog.Cast<AdbcCatalog>();
    auto schema_name = this->name;

    for (const auto &table_name :
         GetTableNamesFromSchema(adbc_catalog.GetUri(), schema_name)) {
        if (owned_tables.find(table_name) == owned_tables.end()) {
            auto ptr = CreateTableEntry(context, table_name);
	    callback(*ptr);
        } else {
            callback(*owned_tables[table_name]);
	}
    }
}

} // namespace adbc
} // namespace duckdb
