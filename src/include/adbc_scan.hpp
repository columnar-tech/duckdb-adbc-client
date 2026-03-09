#pragma once

#include "duckdb.hpp"

namespace duckdb {
namespace adbc {
void AdbcScanFunction(ClientContext &context, TableFunctionInput &data,
                      DataChunk &output);
unique_ptr<FunctionData> AdbcScanBindFunction(ClientContext &context,
                                              TableFunctionBindInput &input,
                                              vector<LogicalType> &return_types,
                                              vector<string> &names);
vector<string> GetTableNames(const string &uri);
} // namespace adbc
} // namespace duckdb
