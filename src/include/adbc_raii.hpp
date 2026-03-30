#pragma once

#include <cstring>
#include <mutex>
#include <memory>
#include <string>
#include "adbc-vendor/adbc.hpp"
#include "adbc-vendor/adbc_driver_manager.hpp"

namespace duckdb {
namespace adbc {

using namespace Private;
#define CHECK_ADBC(EXPR, EXCEPTION_TYPE)                                       \
  do {                                                                         \
    AdbcStatusCode status = (EXPR);                                            \
    if (status != ADBC_STATUS_OK) {                                            \
      auto message = ToString(&error);                                         \
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
inline std::string ToString(struct AdbcError *error) {
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

struct SharedAdbcConnection {
  std::mutex &GetMutex() { return connection_mutex; }
  Private::AdbcConnection *GetConnection() { return connection.get(); }
  Private::AdbcDatabase *GetDatabase() { return database.get(); }

  std::mutex connection_mutex;
  Handle<Private::AdbcDatabase> database = {};
  Handle<Private::AdbcConnection> connection = {};
};

} // namespace adbc
} // namespace duckdb
