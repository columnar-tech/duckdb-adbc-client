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

//===--------------------------------------------------------------------===//
// GetScanFunction
// DuckDB calls this once during physical planning whenever this table
// appears in a FROM clause.  We look up the registered read_adbc table
// function, manually invoke its bind() with the right arguments, and
// hand back the fully-bound function so DuckDB can execute it directly.
//===--------------------------------------------------------------------===//
TableFunction AdbcTableEntry::GetScanFunction(ClientContext &context,
                                               unique_ptr<FunctionData> &bind_data) {
    // 1. Look up the read_adbc table function that was registered at Load() time
    auto &func_entry = Catalog::GetEntry<TableFunctionCatalogEntry>(
        context, INVALID_CATALOG, DEFAULT_SCHEMA, "read_adbc");

    // read_adbc(uri VARCHAR, query VARCHAR) -- match on those two arg types
    auto function = func_entry.functions.GetFunctionByArguments(
        context, {LogicalType::VARCHAR, LogicalType::VARCHAR});

    // 2. Build the SQL query this entry represents.
    //    We quote both schema and table name to handle mixed-case identifiers.
    string sql = StringUtil::Format(
        "SELECT * FROM \"%s\".\"%s\"",
        schema.name, name);

    // 3. Construct the bind input exactly as if the user had written:
    //    SELECT * FROM read_adbc('<uri>', '<sql>')
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

    // 4. Call bind() — this is what populates bind_data and the return types.
    //    After this call, bind_data is fully initialised and owned by DuckDB.
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
