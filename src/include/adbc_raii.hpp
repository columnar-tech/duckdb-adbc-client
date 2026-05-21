#pragma once

#include <cstddef>
#include <cstring>
#include <memory>
#include <string>
#include "adbc-vendor/adbc.hpp"
#include "adbc-vendor/adbc_driver_manager.hpp"
#include "duckdb/common/exception/binder_exception.hpp"

namespace duckdb {
namespace adbc {

using namespace Private;
#define CHECK_ADBC(EXPR, EXCEPTION_TYPE)                                       \
  do {                                                                         \
    AdbcStatusCode status = (EXPR);                                            \
    if (status != ADBC_STATUS_OK) {                                            \
      auto message = AdbcToString(&error);                                     \
      throw EXCEPTION_TYPE(message);                                           \
    }                                                                          \
  } while (false)

#define ADBCV_STRINGIFY(s) #s
#define ADBCV_STRINGIFY_VALUE(s) ADBCV_STRINGIFY(s)

inline std::string StatusCodeToString(AdbcStatusCode code) {
#define CASE(CONSTANT)                                                         \
  case ADBC_STATUS_##CONSTANT:                                                 \
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
inline std::string AdbcToString(struct AdbcError *error) {
  if (error && error->message) {
    std::string result = error->message;
    error->release(error);
    return result;
  }
  return "";
}

template <typename T> struct Initializer {
  static void Initialize(T *value) { memset(value, 0, sizeof(T)); }
};

template <typename T> struct Releaser {
  static void Release(T *value) {
    if (value->release) {
      value->release(value);
    }
  }
};

template <> struct Releaser<struct AdbcConnection> {
  static void Release(struct AdbcConnection *value) {
    if (value->private_data) {
      struct AdbcError error = {};
      AdbcConnectionRelease(value, &error);
    }
  }
};

template <> struct Releaser<struct AdbcDatabase> {
  static void Release(struct AdbcDatabase *value) {
    if (value->private_data) {
      struct AdbcError error = {};
      AdbcDatabaseRelease(value, &error);
    }
  }
};

template <> struct Releaser<struct AdbcStatement> {
  static void Release(struct AdbcStatement *value) {
    if (value->private_data) {
      struct AdbcError error = {};
      AdbcStatementRelease(value, &error);
    }
  }
};

template <typename Resource> struct Handle {
  Resource value;

  Handle() { Initializer<Resource>::Initialize(&value); }

  ~Handle() { Releaser<Resource>::Release(&value); }

  Resource *operator->() { return &value; }

  Resource *get() { return &value; }
};

class AdbcConnection {
public:
  AdbcConnection(const string &uri) : database(), connection() {
    // Initialize the database
    Private::AdbcError error = {};
    CHECK_ADBC(AdbcDatabaseNew(database.get(), &error), BinderException);
    CHECK_ADBC(
        AdbcDatabaseSetOption(database.get(), "uri", uri.c_str(), &error),
        BinderException);
    CHECK_ADBC(AdbcDriverManagerDatabaseSetLoadFlags(
                   database.get(), ADBC_LOAD_FLAG_DEFAULT, &error),
               BinderException);
    CHECK_ADBC(AdbcDatabaseInit(database.get(), &error), BinderException);

    // Initialize the connection
    CHECK_ADBC(AdbcConnectionNew(connection.get(), &error), BinderException);
    CHECK_ADBC(AdbcConnectionInit(connection.get(), database.get(), &error),
               BinderException);
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

  Private::AdbcConnection *GetConnection() { return connection.get(); }
  Private::AdbcDatabase *GetDatabase() { return database.get(); }

  void InitializeStatement(Private::AdbcStatement *statement,
                           const string &query_text) {
    // Initialize the statement
    Private::AdbcError error = {};
    CHECK_ADBC(AdbcStatementNew(connection.get(), statement, &error),
               BinderException);
    CHECK_ADBC(AdbcStatementSetSqlQuery(statement, query_text.c_str(), &error),
               BinderException);
  }

private:
  Handle<Private::AdbcDatabase> database;
  Handle<Private::AdbcConnection> connection;
};

} // namespace adbc
} // namespace duckdb
