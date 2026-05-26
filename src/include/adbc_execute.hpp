#pragma once

#include "adbc_util.hpp"

namespace duckdb {
namespace adbc {
void AdbcExecuteFunction(ClientContext &context, TableFunctionInput &data,
                         DataChunk &output);
unique_ptr<FunctionData>
AdbcExecuteBindFunction(ClientContext &context, TableFunctionBindInput &input,
                        vector<LogicalType> &return_types,
                        vector<string> &names);

class AdbcExecuteFunctionData : public TableFunctionData {
public:
  explicit AdbcExecuteFunctionData(const string &uri, const string &query_text)
      : connection(make_uniq<AdbcConnection>(uri)), statement() {
    connection->InitializeStatement(statement.get(), query_text);
  }

  unique_ptr<AdbcConnection> connection;
  Handle<Private::AdbcStatement> statement;
  bool finished = false;
};

} // namespace adbc
} // namespace duckdb
