#pragma once

#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/storage/table_storage_info.hpp"

namespace duckdb {
namespace adbc {

class AdbcTableEntry : public TableCatalogEntry {
public:
    AdbcTableEntry(Catalog &catalog, SchemaCatalogEntry &schema, CreateTableInfo &info);

    TableFunction GetScanFunction(ClientContext &context, unique_ptr<FunctionData> &bind_data) override;

    unique_ptr<BaseStatistics> GetStatistics(ClientContext &context, column_t column_id) override {
        return nullptr;
    }

    TableStorageInfo GetStorageInfo(ClientContext &context) override {
        return TableStorageInfo();
    }

    void BindUpdateConstraints(Binder &binder,
                               LogicalGet &get,
                               LogicalProjection &proj,
                               LogicalUpdate &update,
                               ClientContext &context) override {

        throw NotImplementedException("UPDATE is not supported on ADBC tables");
    }
};

} // namespace adbc
} // namespace duckdb
