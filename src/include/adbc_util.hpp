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

#include "adbc-vendor/adbc.h"
#include "adbc-vendor/adbc_driver_manager.h"
#include "adbc-vendor/adbc_driver_manager_internal.h"
#include "adbc-vendor/adbc_utils.h"
#include "duckdb/common/arrow/arrow_converter.hpp"
#include "duckdb/common/exception/binder_exception.hpp"
#include "duckdb/function/table/arrow.hpp"

namespace duckdb {
namespace adbc {

using namespace Private;
#define CHECK_ADBC(EXPR, EXCEPTION_TYPE)                                                                               \
    do {                                                                                                               \
        AdbcStatusCode status = (EXPR);                                                                                \
        if (status != ADBC_STATUS_OK) {                                                                                \
            throw EXCEPTION_TYPE(AdbcToString(error.get()));                                                           \
        }                                                                                                              \
    } while (false)

#define ADBCV_STRINGIFY(s) #s
#define ADBCV_STRINGIFY_VALUE(s) ADBCV_STRINGIFY(s)

inline std::string StatusCodeToString(AdbcStatusCode code) {
#define CASE(CONSTANT)                                                                                                 \
    case ADBC_STATUS_##CONSTANT:                                                                                       \
        return ADBCV_STRINGIFY_VALUE(ADBC_STATUS_##CONSTANT) " (" #CONSTANT ")";

    switch (code) {
        CASE(OK);
        CASE(UNKNOWN);
        CASE(NOT_IMPLEMENTED);
        CASE(NOT_FOUND);
        CASE(ALREADY_EXISTS);
        CASE(INVALID_ARGUMENT);
        CASE(INVALID_STATE);
        CASE(INVALID_DATA);
        CASE(INTEGRITY);
        CASE(INTERNAL);
        CASE(IO);
        CASE(CANCELLED);
        CASE(TIMEOUT);
        CASE(UNAUTHENTICATED);
        CASE(UNAUTHORIZED);
    default:
        return "(unknown code)";
    }
#undef CASE
}
inline std::string AdbcToString(Private::AdbcError *error) {
    if (error && error->message) {
        return error->message;
    }
    return "";
}

template <typename Resource>
struct Initializer {
    static void Initialize(Resource *value) {
        memset(value, 0, sizeof(Resource));
    }
};

template <typename Resource>
struct Releaser {
    static void Release(Resource *value) {
        if (value->release) {
            value->release(value);
        }
    }
};

template <typename Resource>
struct Handle {
    Resource value{};

    Handle() {
        Initializer<Resource>::Initialize(&value);
    }

    Handle(const Handle &) = delete;
    Handle &operator=(const Handle &) = delete;

    Handle(Handle &&other) noexcept : value(other.value) {
        Initializer<Resource>::Initialize(&other.value);
    }

    Handle &operator=(Handle &&other) noexcept {
        if (this != &other) {
            Releaser<Resource>::Release(&value);
            value = other.value;
            Initializer<Resource>::Initialize(&other.value);
        }
        return *this;
    }

    ~Handle() {
        Releaser<Resource>::Release(&value);
    }

    Resource *operator->() {
        return &value;
    }
    const Resource *operator->() const {
        return &value;
    }

    Resource *get() {
        return &value;
    }
    const Resource *get() const {
        return &value;
    }

    Resource release() noexcept {
        Resource tmp = value;
        Initializer<Resource>::Initialize(&value);
        return tmp;
    }

    void reset() noexcept {
        Releaser<Resource>::Release(&value);
        Initializer<Resource>::Initialize(&value);
    }
};

// Releaser specializations come after Handle is fully defined,
// since they instantiate Handle<Private::AdbcError> by value.

template <>
struct Releaser<struct AdbcConnection> {
    static void Release(struct AdbcConnection *value) {
        if (value->private_data) {
            Handle<Private::AdbcError> error{};
            AdbcConnectionRelease(value, error.get());
        }
    }
};

template <>
struct Releaser<struct AdbcDatabase> {
    static void Release(struct AdbcDatabase *value) {
        if (value->private_data) {
            Handle<Private::AdbcError> error{};
            AdbcDatabaseRelease(value, error.get());
        }
    }
};

template <>
struct Releaser<struct AdbcStatement> {
    static void Release(struct AdbcStatement *value) {
        if (value->private_data) {
            Handle<Private::AdbcError> error{};
            AdbcStatementRelease(value, error.get());
        }
    }
};

class AdbcConnection {
public:
    AdbcConnection(const string &uri) : database(), connection() {
        // Initialize the database
        Handle<Private::AdbcError> error = {};
        CHECK_ADBC(AdbcDatabaseNew(database.get(), error.get()), BinderException);
        CHECK_ADBC(AdbcDatabaseSetOption(database.get(), "uri", uri.c_str(), error.get()), BinderException);
        CHECK_ADBC(AdbcDriverManagerDatabaseSetLoadFlags(database.get(), ADBC_LOAD_FLAG_DEFAULT, error.get()),
                   BinderException);
        CHECK_ADBC(AdbcDatabaseInit(database.get(), error.get()), BinderException);

        // Initialize the connection
        CHECK_ADBC(AdbcConnectionNew(connection.get(), error.get()), BinderException);
        CHECK_ADBC(AdbcConnectionInit(connection.get(), database.get(), error.get()), BinderException);
    }
    // Disable copy constructors
    AdbcConnection(const AdbcConnection &other) = delete;
    AdbcConnection &operator=(const AdbcConnection &) = delete;

    // Enable move constructors
    AdbcConnection(AdbcConnection &&other) noexcept {
        std::swap(connection, other.connection);
        std::swap(database, other.database);
    }
    AdbcConnection &operator=(AdbcConnection &&other) noexcept {
        std::swap(connection, other.connection);
        std::swap(database, other.database);
        return *this;
    }

    Private::AdbcConnection *GetConnection() {
        return connection.get();
    }
    Private::AdbcDatabase *GetDatabase() {
        return database.get();
    }

    Handle<Private::AdbcStatement> MakeStatement(const string &query_text) {
        Handle<Private::AdbcStatement> statement = {};
        Handle<Private::AdbcError> error = {};
        CHECK_ADBC(AdbcStatementNew(connection.get(), statement.get(), error.get()), BinderException);
        CHECK_ADBC(AdbcStatementSetSqlQuery(statement.get(), query_text.c_str(), error.get()), BinderException);
	return statement;
    }

private:
    Handle<Private::AdbcDatabase> database;
    Handle<Private::AdbcConnection> connection;
};

} // namespace adbc
} // namespace duckdb
