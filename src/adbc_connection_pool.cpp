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

#include "adbc_connection_pool.hpp"

namespace duckdb {
namespace adbc {

AdbcPooledConnection::~AdbcPooledConnection() {
    if (!ephemeral) {
        pool->ReturnConnection(std::move(connection));
    }
}

unique_ptr<AdbcPooledConnection> AdbcConnectionPool::GetEphemeralConnection(const string &uri) {
    return make_uniq<AdbcPooledConnection>(nullptr, make_uniq<AdbcConnection>(uri), true);
}

unique_ptr<AdbcPooledConnection> AdbcConnectionPool::GetConnection() {
    lock_guard<mutex> connection_lock(connection_mutex);

    // If there are no available connections
    if (connections.empty()) {
        // Check if the pool cannot grow further
        if (active_connections == max_connections) {
            // In that case return an ephemeral connection
            return GetEphemeralConnection(uri);
        }
        // Otherwise create a new pooled connection
        connections.push_back(make_uniq<AdbcConnection>(uri));
    }

    // Return the available connection
    auto free_connection = std::move(connections.back());
    connections.pop_back();
    ++active_connections;
    return make_uniq<AdbcPooledConnection>(shared_from_this(), std::move(free_connection), false);
}

void AdbcConnectionPool::ReturnConnection(unique_ptr<AdbcConnection> connection) {
    lock_guard<mutex> connection_lock(connection_mutex);
    --active_connections;
    connections.push_back(std::move(connection));
}

} // namespace adbc
} // namespace duckdb
