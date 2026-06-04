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
struct Handle {
    Resource value;

    Handle();
    ~Handle();
    Resource *operator->();
    Resource *get();
};

template <typename T>
struct Initializer {
    static void Initialize(T *value) {
        memset(value, 0, sizeof(T));
    }
};

template <typename T>
struct Releaser {
    static void Release(T *value) {
        if (value->release) {
            value->release(value);
        }
    }
};

template <>
struct Releaser<struct AdbcConnection> {
    static void Release(struct AdbcConnection *value) {
        if (value->private_data) {
            Handle<Private::AdbcError> error = {};
            AdbcConnectionRelease(value, error.get());
        }
    }
};

template <>
struct Releaser<struct AdbcDatabase> {
    static void Release(struct AdbcDatabase *value) {
        if (value->private_data) {
            Handle<Private::AdbcError> error = {};
            AdbcDatabaseRelease(value, error.get());
        }
    }
};

template <>
struct Releaser<struct AdbcStatement> {
    static void Release(struct AdbcStatement *value) {
        if (value->private_data) {
            Handle<Private::AdbcError> error = {};
            AdbcStatementRelease(value, error.get());
        }
    }
};

template <typename Resource>
Handle<Resource>::Handle() {
    Initializer<Resource>::Initialize(&value);
}

template <typename Resource>
Handle<Resource>::~Handle() {
    Releaser<Resource>::Release(&value);
}

template <typename Resource>
Resource *Handle<Resource>::operator->() {
    return &value;
}

template <typename Resource>
Resource *Handle<Resource>::get() {
    return &value;
}

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

    void InitializeStatement(Private::AdbcStatement *statement, const string &query_text) {
        // Initialize the statement
        Handle<Private::AdbcError> error = {};
        CHECK_ADBC(AdbcStatementNew(connection.get(), statement, error.get()), BinderException);
        CHECK_ADBC(AdbcStatementSetSqlQuery(statement, query_text.c_str(), error.get()), BinderException);
    }

private:
    Handle<Private::AdbcDatabase> database;
    Handle<Private::AdbcConnection> connection;
};

} // namespace adbc
} // namespace duckdb
