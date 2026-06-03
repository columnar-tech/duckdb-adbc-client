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

#include "adbc_connection_pool.hpp"
#include "adbc_util.hpp"

namespace duckdb {
namespace adbc {
unique_ptr<FunctionData> AdbcScanBindFunction(ClientContext &context,
                                              TableFunctionBindInput &input,
                                              vector<LogicalType> &return_types,
                                              vector<string> &names);

// A factory class that holds the ADBC connection state and produces
// ArrowArrayStreamWrapper instances
class AdbcArrowStreamFactory {
public:
    // Create an ephemeral connection (i.e., read_adbc(...) is called directly)
    AdbcArrowStreamFactory(const string &uri, const string &query_text);
    // Use a connection from the catalog's pool (i.e., SELECT * FROM <adbc>)
    AdbcArrowStreamFactory(unique_ptr<AdbcPooledConnection> connection, const string &query_text);
    AdbcStatement *GetStatement();
    void ResetStatement();

private:
    unique_ptr<AdbcPooledConnection> connection;
    Handle<Private::AdbcStatement> statement;
    string query_text;
};

// A wrapper class to take ownership of the factory object (and the
// corresponding ADBC state) during the scan
class AdbcArrowScanFunctionData : public ArrowScanFunctionData {
public:
    // Pass the factory and the factory function that creates an ArrowArrayStream
    AdbcArrowScanFunctionData(ClientContext &context, unique_ptr<AdbcArrowStreamFactory> factory);

private:
    unique_ptr<AdbcArrowStreamFactory> adbc_arrow_stream_factory;

public:
    optional_idx cardinality;
};

} // namespace adbc
} // namespace duckdb
