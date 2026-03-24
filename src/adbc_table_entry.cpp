#include "adbc_table_entry.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/table_function_catalog_entry.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/parser/parsed_data/create_view_info.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/parser/tableref/table_function_ref.hpp"

namespace duckdb {

AdbcTableEntry::AdbcTableEntry(Catalog &catalog, SchemaCatalogEntry &schema,
                                CreateTableInfo &info, string uri)
    : TableCatalogEntry(catalog, schema, info),
      adbc_uri(std::move(uri)) {}

TableFunction AdbcTableEntry::GetScanFunction(ClientContext &context,
                                               unique_ptr<FunctionData> &bind_data) {
    auto &func_entry = Catalog::GetEntry<TableFunctionCatalogEntry>(
        context, INVALID_CATALOG, DEFAULT_SCHEMA, "read_adbc");

    auto function = func_entry.functions.GetFunctionByArguments(
        context, {LogicalType::VARCHAR, LogicalType::VARCHAR});

    string sql = StringUtil::Format(
        "SELECT * FROM \"%s\".\"%s\"",
        schema.name, name);

    vector<Value> inputs = {
        Value(adbc_uri),
        Value(sql)
    };
    named_parameter_map_t named_params;
    vector<LogicalType>   input_table_types;
    vector<string>        input_table_names;

    TableFunctionRef empty_ref;
    TableFunctionBindInput bind_input(
        inputs, named_params,
        input_table_types, input_table_names,
        function.function_info.get(), nullptr,
       	function, empty_ref);

    vector<LogicalType> return_types;
    vector<string>      return_names;

    bind_data = function.bind(context, bind_input, return_types, return_names);
    return function;
}

//===--------------------------------------------------------------------===//
// Stubs
//===--------------------------------------------------------------------===//

unique_ptr<BaseStatistics> AdbcTableEntry::GetStatistics(ClientContext &context,
                                                           column_t column_id) {
    // No statistics available from a generic ADBC source.
    // Returning nullptr tells DuckDB's optimizer to use default estimates.
    return nullptr;
}

TableStorageInfo AdbcTableEntry::GetStorageInfo(ClientContext &context) {
    // Remote table — no local storage info.
    return TableStorageInfo();
}

void AdbcTableEntry::BindUpdateConstraints(Binder &binder, LogicalGet &get,
                                            LogicalProjection &proj,
                                            LogicalUpdate &update,
                                            ClientContext &context) {
    throw NotImplementedException("UPDATE is not supported on ADBC tables");
}

} // namespace duckdb
