#pragma once

#include "duckdb.hpp"

namespace duckdb {
namespace adbc {
void AdbcClearCacheFunction(ClientContext &context, TableFunctionInput &data, DataChunk &output);
unique_ptr<FunctionData> AdbcClearCacheBindFunction(ClientContext &context,
                                                    TableFunctionBindInput &input,
                                                    vector<LogicalType> &return_types,
                                                    vector<string> &names);

struct AdbcClearCacheFunctionData : public TableFunctionData {
    bool finished = false;
};

} // namespace adbc
} // namespace duckdb
