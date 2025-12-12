#include "duckdb.hpp"

namespace duckdb {
namespace adbc {

unique_ptr<FunctionData> AdbcScanBindFunction(ClientContext &context, TableFunctionBindInput &input,
                                              vector<LogicalType> &return_types, vector<string> &names);
} // namespace adbc
} // namespace duckdb