#pragma once

#include "duckdb.hpp"
#include "adbc-vendor/adbc.hpp"

namespace duckdb {
namespace adbc {
void AdbcScanFunction(ClientContext &context, TableFunctionInput &data,
                      DataChunk &output);
unique_ptr<FunctionData> AdbcScanBindFunction(ClientContext &context,
                                              TableFunctionBindInput &input,
                                              vector<LogicalType> &return_types,
                                              vector<string> &names);

void InitializeDatabase(Private::AdbcDatabase *database, const string &uri);
void InitializeConnection(Private::AdbcDatabase *database,
                          Private::AdbcConnection *connection);
void InitializeStatement(Private::AdbcConnection *connection,
                         Private::AdbcStatement *statement,
                         const string &query_text);
} // namespace adbc
} // namespace duckdb
