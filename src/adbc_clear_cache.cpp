#include "adbc_catalog.hpp"
#include "adbc_clear_cache.hpp"
#include "adbc_util.hpp"
#include "duckdb/main/attached_database.hpp"

namespace duckdb {
namespace adbc {

using namespace Private;

void AdbcClearCacheFunction(ClientContext &context, TableFunctionInput &input, DataChunk &output) {

    // Return if we already executed the command
    auto &function_data = input.bind_data->CastNoConst<AdbcClearCacheFunctionData>();
    if (function_data.finished) {
        return;
    }

    // Clear the cache
    auto databases = DatabaseManager::Get(context).GetDatabases(context);
    for (auto &db : databases) {
        if (db) {
            auto &catalog = db->GetCatalog();
            if (catalog.GetCatalogType() == "adbc") {
                catalog.Cast<AdbcCatalog>().ClearCache();
            }
        }
    }

    // Mark as completed
    output.SetCardinality(1);
    output.SetValue(0, 0, Value::BOOLEAN(true));
    function_data.finished = true;
}

unique_ptr<FunctionData> AdbcClearCacheBindFunction(ClientContext &context,
                                                    TableFunctionBindInput &input,
                                                    vector<LogicalType> &return_types,
                                                    vector<string> &names) {
    if (!input.inputs.empty()) {
        throw BinderException("adbc_clear_cache() requires zero parameters");
    }

    // Return type
    return_types.emplace_back(LogicalTypeId::BOOLEAN);
    names.emplace_back("Success");
    return make_uniq<AdbcClearCacheFunctionData>();
}

} // namespace adbc
} // namespace duckdb
