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
#include "duckdb/common/mutex.hpp"
#include "duckdb/common/shared_ptr.hpp"
#include "duckdb/original/std/memory.hpp"

namespace duckdb {
namespace adbc {

class AdbcCatalog;
class AdbcConnectionPool;

class AdbcPooledConnection {
public:
    AdbcPooledConnection(shared_ptr<AdbcConnectionPool> pool, unique_ptr<AdbcConnection> conn, bool ephemeral)
        : pool(pool), connection(std::move(conn)), ephemeral(ephemeral) {
    }
    ~AdbcPooledConnection();

    AdbcConnection &GetConnection() {
        return *connection;
    }
    Private::AdbcConnection *GetRawConnection() {
        return connection->GetConnection();
    }

    // Disable copy constructors
    AdbcPooledConnection(const AdbcPooledConnection &other) = delete;
    AdbcPooledConnection &operator=(const AdbcPooledConnection &) = delete;

    // Enable move constructors
    AdbcPooledConnection(AdbcPooledConnection &&other) noexcept {
        std::swap(pool, other.pool);
        std::swap(connection, other.connection);
        std::swap(ephemeral, other.ephemeral);
    }

    AdbcPooledConnection &operator=(AdbcPooledConnection &&other) noexcept {
        std::swap(pool, other.pool);
        std::swap(connection, other.connection);
        std::swap(ephemeral, other.ephemeral);
        return *this;
    }

private:
    shared_ptr<AdbcConnectionPool> pool;
    unique_ptr<AdbcConnection> connection;
    bool ephemeral;
};

class AdbcConnectionPool : public enable_shared_from_this<AdbcConnectionPool> {
public:
    AdbcConnectionPool(const string &uri, idx_t max_connections) : uri(uri), max_connections(max_connections) {
    }

public:
    static unique_ptr<AdbcPooledConnection> GetEphemeralConnection(const string &uri);
    unique_ptr<AdbcPooledConnection> GetConnection();
    void ReturnConnection(unique_ptr<AdbcConnection> connection);

private:
    idx_t active_connections = 0;
    string uri;
    idx_t max_connections;
    mutex connection_mutex;
    vector<unique_ptr<AdbcConnection>> connections;
};

} // namespace adbc
} // namespace duckdb
