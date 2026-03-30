#pragma once

#include "duckdb/common/arrow/arrow.hpp"
#include "duckdb/function/table/arrow.hpp"
#include "duckdb/main/config.hpp"
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

// A factory class that holds the ADBC connection state and produces
// ArrowArrayStreamWrapper instances
class AdbcArrowStreamFactory {
public:
  AdbcArrowStreamFactory(const string &uri, const string &query_text);
  AdbcArrowStreamFactory(shared_ptr<SharedAdbcConnection> connection,
                         const string &query_text);

  std::mutex &GetMutex();
  AdbcStatement *GetStatement();

private:
  shared_ptr<SharedAdbcConnection> shared_connection;
  Handle<Private::AdbcStatement> statement;
};

// A wrapper class to take ownership of the factory object (and the
// corresponding ADBC state) during the scan
class AdbcArrowScanFunctionData : public ArrowScanFunctionData {
public:
  // Pass the factory and the factory function that creates an ArrowArrayStream
  AdbcArrowScanFunctionData(ClientContext &context,
                            unique_ptr<AdbcArrowStreamFactory> factory);

private:
  unique_ptr<AdbcArrowStreamFactory> adbc_arrow_stream_factory;
};

} // namespace adbc
} // namespace duckdb
