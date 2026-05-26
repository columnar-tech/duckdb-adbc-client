#pragma once

#include "adbc_connection_pool.hpp"
#include "adbc_util.hpp"

namespace duckdb {
namespace adbc {
void AdbcScanFunction(ClientContext &context, TableFunctionInput &data,
                      DataChunk &output);
unique_ptr<FunctionData> AdbcScanBindFunction(ClientContext &context,
                                              TableFunctionBindInput &input,
                                              vector<LogicalType> &return_types,
                                              vector<string> &names);

class AdbcCatalog;

// A factory class that holds the ADBC connection state and produces
// ArrowArrayStreamWrapper instances
class AdbcArrowStreamFactory {
public:
  // Create an ephemeral connection (i.e., read_adbc(...) is called directly)
  AdbcArrowStreamFactory(const string &uri, const string &query_text);
  // Use a connection from the catalog's pool (i.e., SELECT * FROM <adbc>)
  AdbcArrowStreamFactory(AdbcCatalog &catalog, const string &query_text);
  AdbcStatement *GetStatement();

private:
  unique_ptr<AdbcPooledConnection> connection;
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
