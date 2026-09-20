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
#include "duckdb/function/table/arrow.hpp"
#include "adbc_connection_pool.hpp"
#include "adbc_util.hpp"

namespace duckdb {
namespace adbc {
void AdbcScanFunction(ClientContext &context, TableFunctionInput &input, DataChunk &output);
unique_ptr<FunctionData> AdbcScanBindFunction(ClientContext &context,
                                              TableFunctionBindInput &input,
                                              vector<LogicalType> &return_types,
                                              vector<Identifier> &names);

// A factory class that holds the ADBC connection state and produces
// ArrowArrayStreamWrapper instances
class AdbcArrowStreamFactory : public ArrowScanFactory {
public:
    // Create an ephemeral connection (i.e., read_adbc(...) is called directly)
    AdbcArrowStreamFactory(const string &uri, const string &query_text);
    // Use a connection from the catalog's pool (i.e., SELECT * FROM <adbc>)
    AdbcArrowStreamFactory(unique_ptr<AdbcPooledConnection> connection, const string &table, const string &delimiter);
    AdbcStatement *GetStatement();
    void ResetStatement();
    void ApplyProjectionPushdown(const vector<string> &columns);
    bool CanPushdownProjections() const {
        return projection_pushdown;
    }

    // Methods to override
    void GetSchema(ArrowSchema &schema) override;
    unique_ptr<ArrowArrayStreamWrapper> ProduceStream(ArrowStreamParameters &parameters) override;

private:
    unique_ptr<AdbcPooledConnection> connection;
    string query_text;
    string table;
    string delimiter;
    bool projection_pushdown = false;
    Handle<Private::AdbcStatement> statement;
};

// A wrapper class to take ownership of the factory object (and the
// corresponding ADBC state) during the scan
class AdbcArrowScanFunctionData : public ArrowScanFunctionData {
public:
    // Pass the factory and the factory function that creates an ArrowArrayStream
    AdbcArrowScanFunctionData(ClientContext &context, shared_ptr<AdbcArrowStreamFactory> factory);

public:
    shared_ptr<AdbcArrowStreamFactory> adbc_arrow_stream_factory;
    optional_idx cardinality;
};

} // namespace adbc
} // namespace duckdb
