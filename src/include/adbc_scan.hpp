#pragma once

#include "duckdb.hpp"
#include "adbc_raii.hpp"
#include "adbc-vendor/adbc.hpp"

namespace duckdb {
namespace adbc {
void AdbcScanFunction(ClientContext &context, TableFunctionInput &data,
                      DataChunk &output);
unique_ptr<FunctionData> AdbcScanBindFunction(ClientContext &context,
                                              TableFunctionBindInput &input,
                                              vector<LogicalType> &return_types,
                                              vector<string> &names);

void InitializeDatabase(SharedAdbcConnection &shared_connection,
                        const string &uri);
void InitializeConnection(SharedAdbcConnection &shared_connection);
void InitializeStatement(SharedAdbcConnection &shared_connection,
                         Private::AdbcStatement *statement,
                         const string &query_text);
} // namespace adbc
} // namespace duckdb
