#include "adbc_schema_entry.hpp"
#include "adbc_catalog.hpp"
#include "adbc_scan.hpp"
#include "adbc_table_entry.hpp"
#include "duckdb/common/exception/catalog_exception.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"

namespace duckdb {
namespace adbc {

CatalogEntry *AdbcSchemaEntry::GetOrCreateTableEntry(ClientContext &context,
                                                     const string &table_name) {

  // Return the entry if it already exists
  auto it = owned_tables.find(table_name);
  if (it != owned_tables.end()) {
    return it->second.get();
  }

  // Create the entry to be inserted
  auto &adbc_catalog = catalog.Cast<AdbcCatalog>();
  auto delimiter = adbc_catalog.GetDelimiter();
  auto schema_name = this->name;
  auto quoted_schema_name = delimiter[0] + schema_name + delimiter[1];
  auto quoted_table_name = delimiter[0] + table_name + delimiter[1];

  // Bind a SQL statement and use ADBC to retrieve the metadata for the table
  string sql = StringUtil::Format("SELECT * FROM  %s.%s", quoted_schema_name,
                                  quoted_table_name);

  auto factory = make_uniq<AdbcArrowStreamFactory>(
      adbc_catalog.GetPooledConnection(), sql);
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

  // Insert the entry
  auto table_entry = make_uniq<AdbcTableEntry>(catalog, *this, *table_info);
  auto ptr = table_entry.get();
  owned_tables[table_name] = std::move(table_entry);
  return ptr;
}

optional_ptr<CatalogEntry>
AdbcSchemaEntry::LookupEntry(CatalogTransaction transaction,
                             const EntryLookupInfo &lookup_info) {
  auto &adbc_catalog = catalog.Cast<AdbcCatalog>();
  auto catalog_lock = adbc_catalog.AcquireScopedLock();

  try {
    return GetOrCreateTableEntry(transaction.GetContext(),
                                 lookup_info.GetEntryName());
  } catch (...) {
    return nullptr;
  }
}

void AdbcSchemaEntry::Scan(
    ClientContext &context, CatalogType type,
    const std::function<void(CatalogEntry &)> &callback) {

  auto &adbc_catalog = catalog.Cast<AdbcCatalog>();
  auto catalog_lock = adbc_catalog.AcquireScopedLock();

  // We only support table lookups
  if (type != CatalogType::TABLE_ENTRY) {
    return;
  }

  auto schema_name = this->name;
  auto uri = adbc_catalog.GetDBPath();

  // For each ADBC table in the schema, get or create an entry for it
  for (const auto &table_name : adbc_catalog.FetchTableNames(schema_name)) {
    if (auto *entry = GetOrCreateTableEntry(context, table_name)) {
      callback(*entry);
    }
  }
}

optional_ptr<CatalogEntry>
AdbcSchemaEntry::CreateTable(CatalogTransaction transaction,
                             BoundCreateTableInfo &info) {

  auto &adbc_catalog = catalog.Cast<AdbcCatalog>();
  auto catalog_lock = adbc_catalog.AcquireScopedLock();

  // Throw an exception if the entry already exists
  auto table_name = info.Base().table;
  auto it = owned_tables.find(table_name);
  if (it != owned_tables.end()) {
    throw CatalogException::EntryAlreadyExists(CatalogType::TABLE_ENTRY,
                                               table_name);
  }

  throw NotImplementedException(
      "CreateTable not yet supported with the ADBC extension");
}

} // namespace adbc
} // namespace duckdb
