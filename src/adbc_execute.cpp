#include "duckdb/common/types.hpp"
#include "adbc_execute.hpp"
#include "adbc_scan.hpp"
#include "adbc-vendor/adbc.hpp"
#include "adbc-vendor/adbc_driver_manager.hpp"

namespace duckdb {
namespace adbc {

using namespace Private;

void AdbcExecuteFunction(ClientContext &context, TableFunctionInput &input,
                         DataChunk &output) {

  // Lock the connection
  auto &function_data = input.bind_data->CastNoConst<AdbcExecuteFunctionData>();

  // Return if we already executed the DML
  if (function_data.finished) {
    return;
  }

  // Execute the DML
  AdbcError error = {};
  CHECK_ADBC(AdbcStatementExecuteQuery(function_data.statement.get(), nullptr,
                                       nullptr, &error),
             IOException);

  // Mark as completed
  function_data.finished = true;
}

unique_ptr<FunctionData>
AdbcExecuteBindFunction(ClientContext &context, TableFunctionBindInput &input,
                        vector<LogicalType> &return_types,
                        vector<string> &names) {

  // Validate that the function was provided exactly two input parameters
  if (input.inputs.size() != 2) {
    throw BinderException("adbc_execute(...) requires two parameters: (1) the "
                          "adbc URI (2) the SQL query string");
  }

  // Return type
  return_types.emplace_back(LogicalTypeId::BOOLEAN);
  names.emplace_back("Success");

  // Get the input parameters
  auto uri = input.inputs[0].GetValue<string>();
  auto query_text = input.inputs[1].GetValue<string>();
  return make_uniq<AdbcExecuteFunctionData>(uri, query_text);
}

} // namespace adbc
} // namespace duckdb
