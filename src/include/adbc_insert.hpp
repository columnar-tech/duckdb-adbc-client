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
#include "duckdb/common/types/column/column_data_collection.hpp"
#include "duckdb/execution/physical_operator.hpp"

namespace duckdb {
namespace adbc {

enum class InsertMode { APPEND, CTAS };

class AdbcInsert : public PhysicalOperator {
public:
    AdbcInsert(PhysicalPlan &physical_plan,
               LogicalOperator &op,
               const vector<LogicalType> &types,
               const vector<string> &names,
               const string &table_name,
               const string &schema_name,
               shared_ptr<AdbcConnectionPool> pool,
               InsertMode mode);

private:
    vector<LogicalType> column_types;
    vector<string> column_names;
    string table_name;
    string schema_name;
    shared_ptr<AdbcConnectionPool> pool;
    InsertMode insert_mode;

public:
    // Source interface
    SourceResultType GetData(ExecutionContext &context, DataChunk &chunk, OperatorSourceInput &input) const override;
    bool IsSource() const override {
        return true;
    }

public:
    // Sink interface
    unique_ptr<GlobalSinkState> GetGlobalSinkState(ClientContext &context) const override;
    SinkResultType Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const override;
    SinkFinalizeType Finalize(Pipeline &pipeline,
                              Event &event,
                              ClientContext &context,
                              OperatorSinkFinalizeInput &input) const override;

    bool IsSink() const override {
        return true;
    }

    bool ParallelSink() const override {
        return false;
    }

    string GetName() const override;
    InsertionOrderPreservingMap<string> ParamsToString() const override;
};

} // namespace adbc
} // namespace duckdb
