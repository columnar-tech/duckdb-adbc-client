// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#pragma once

#include "adbc_util.hpp"
#include "duckdb/original/std/memory.hpp"

namespace duckdb {
namespace adbc {
void AdbcExecuteFunction(ClientContext &context, TableFunctionInput &data, DataChunk &output);
unique_ptr<FunctionData> AdbcExecuteBindFunction(ClientContext &context,
                                                 TableFunctionBindInput &input,
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
