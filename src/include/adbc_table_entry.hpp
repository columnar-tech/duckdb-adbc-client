#pragma once

#include "duckdb.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/storage/table_storage_info.hpp"

namespace duckdb {

class AdbcTableEntry : public TableCatalogEntry {
public:
    AdbcTableEntry(Catalog &catalog, SchemaCatalogEntry &schema,
                   CreateTableInfo &info, string uri);

    // ---- Read path --------------------------------------------------------
    // DuckDB calls this during physical planning for any SELECT touching
    // this table. We return the read_adbc table function pre-bound with
    // the correct URI and SQL query.
    TableFunction GetScanFunction(ClientContext &context,
                                  unique_ptr<FunctionData> &bind_data) override;

    // ---- Write path -------------------------------------------------------
    // DuckDB calls this during physical planning for INSERT INTO <adbc_table>.
    // Implemented in the next step — stubs for now.
    unique_ptr<BaseStatistics> GetStatistics(ClientContext &context,
                                             column_t column_id) override;

    TableStorageInfo GetStorageInfo(ClientContext &context) override;

    void BindUpdateConstraints(Binder &binder, LogicalGet &get,
                               LogicalProjection &proj, LogicalUpdate &update,
                               ClientContext &context) override;

    // The URI of the remote ADBC source — needed when building the scan query
    const string &GetUri() const { return adbc_uri; }

private:
    string adbc_uri;
};

} // namespace duckdb
