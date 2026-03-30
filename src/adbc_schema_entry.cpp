#include "adbc_schema_entry.hpp"
#include "adbc_catalog.hpp"
#include "adbc_scan.hpp"
#include "adbc_table_entry.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"

namespace duckdb {
namespace adbc {

CatalogEntry *AdbcSchemaEntry::GetOrCreateTableEntry(ClientContext &context,
                                                     const string &table_name) {
  // Acquire the read lock and check if the entry already exists
  {
    std::shared_lock<std::shared_mutex> read_lock(tables_mutex);
    auto it = owned_tables.find(table_name);
    if (it != owned_tables.end()) {
      return it->second.get();
    }
  }

  // Create the entry to be inserted
  auto &adbc_catalog = catalog.Cast<AdbcCatalog>();
  auto schema_name = this->name;

  // Bind a SQL statement and use ADBC to retrieve the metadata for the table
  string sql = StringUtil::Format("SELECT * FROM \"%s\".\"%s\"", schema_name,
                                  table_name);
  auto factory = make_uniq<AdbcArrowStreamFactory>(
      adbc_catalog.GetSharedConnection(), sql);
  auto bind_data =
      make_uniq<AdbcArrowScanFunctionData>(context, std::move(factory));
  auto col_names = bind_data->arrow_table.GetNames();
  auto col_types = bind_data->arrow_table.GetTypes();
  auto table_info = make_uniq<CreateTableInfo>(*this, table_name);
  for (idx_t i = 0; i < col_names.size(); i++) {
    ColumnDefinition col(col_names[i], col_types[i]);
    table_info->columns.AddColumn(std::move(col));
  }
  table_info->internal = false;
  auto table_entry = make_uniq<AdbcTableEntry>(catalog, *this, *table_info);

  // Now acquire the write lock and try to insert the entry
  std::unique_lock<std::shared_mutex> write_lock(tables_mutex);

  // Return it if it already exists
  auto it = owned_tables.find(table_name);
  if (it != owned_tables.end()) {
    return it->second.get();
  }

  // Otherwise insert it
  auto ptr = table_entry.get();
  owned_tables[table_name] = std::move(table_entry);
  return ptr;
}

optional_ptr<CatalogEntry>
AdbcSchemaEntry::LookupEntry(CatalogTransaction transaction,
                             const EntryLookupInfo &lookup_info) {
  return GetOrCreateTableEntry(transaction.GetContext(),
                               lookup_info.GetEntryName());
}

void AdbcSchemaEntry::Scan(
    ClientContext &context, CatalogType type,
    const std::function<void(CatalogEntry &)> &callback) {

  // We only support table lookups
  if (type != CatalogType::TABLE_ENTRY) {
    return;
  }

  auto &adbc_catalog = catalog.Cast<AdbcCatalog>();
  auto schema_name = this->name;
  auto uri = adbc_catalog.GetUri();

  // For each ADBC table in the schema, get or create an entry for it
  for (const auto &table_name : adbc_catalog.FetchTableNames(schema_name)) {
    callback(*GetOrCreateTableEntry(context, table_name));
  }
}

} // namespace adbc
} // namespace duckdb
