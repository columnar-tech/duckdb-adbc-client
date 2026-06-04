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

#include "adbc_execute.hpp"
#include "adbc_scan.hpp"
#include "adbc_util.hpp"

namespace duckdb {
namespace adbc {

using namespace Private;

void AdbcExecuteFunction(ClientContext &context, TableFunctionInput &input, DataChunk &output) {

    // Return if we already executed the command
    auto &function_data = input.bind_data->CastNoConst<AdbcExecuteFunctionData>();
    if (function_data.finished) {
        return;
    }

    // Execute the command
    Handle<AdbcError> error = {};
    CHECK_ADBC(AdbcStatementExecuteQuery(function_data.statement.get(), nullptr, nullptr, error.get()), IOException);

    // Mark as completed
    output.SetCardinality(1);
    output.SetValue(0, 0, Value::BOOLEAN(true));
    function_data.finished = true;
}

unique_ptr<FunctionData> AdbcExecuteBindFunction(ClientContext &context,
                                                 TableFunctionBindInput &input,
                                                 vector<LogicalType> &return_types,
                                                 vector<string> &names) {

    // Validate that the function was provided exactly two input parameters
    if (input.inputs.size() != 2) {
        throw BinderException("adbc_execute(...) requires two parameters: (1) the "
                              "adbc URI (2) the SQL query string");
    }

    // Return type
    return_types.emplace_back(LogicalTypeId::BOOLEAN);
    names.emplace_back("Success");

    // Get the input parameters
    auto uri = input.inputs[0].GetValue<string>();
    auto query_text = input.inputs[1].GetValue<string>();
    return make_uniq<AdbcExecuteFunctionData>(uri, query_text);
}

} // namespace adbc
} // namespace duckdb
