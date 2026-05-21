#pragma once

#include "adbc_raii.hpp"
#include <mutex>

namespace duckdb {
namespace adbc {

class AdbcCatalog;
class AdbcConnectionPool;

class AdbcPoolConnection {
public:
  AdbcPoolConnection(AdbcConnectionPool *pool, unique_ptr<AdbcConnection> conn,
                     bool ephemeral)
      : pool(pool), connection(std::move(conn)), ephemeral(ephemeral) {}
  ~AdbcPoolConnection();

  AdbcConnection &GetConnection() { return *connection; }
  Private::AdbcConnection *GetRawConnection() {
    return connection->GetConnection();
  }

  // Disable copy constructors
  AdbcPoolConnection(const AdbcPoolConnection &other) = delete;
  AdbcPoolConnection &operator=(const AdbcPoolConnection &) = delete;

  // Enable move constructors
  AdbcPoolConnection(AdbcPoolConnection &&other) noexcept {
    std::swap(pool, other.pool);
    std::swap(connection, other.connection);
    std::swap(ephemeral, other.ephemeral);
  }

  AdbcPoolConnection &operator=(AdbcPoolConnection &&other) noexcept {
    std::swap(pool, other.pool);
    std::swap(connection, other.connection);
    std::swap(ephemeral, other.ephemeral);
    return *this;
  }

private:
  AdbcConnectionPool *pool;
  unique_ptr<AdbcConnection> connection;
  bool ephemeral;
};

class AdbcConnectionPool {
public:
  static constexpr const idx_t DEFAULT_MAX_CONNECTIONS = 2;

  AdbcConnectionPool(const string &uri,
                     idx_t maximum_connections = DEFAULT_MAX_CONNECTIONS)
      : uri(uri), maximum_connections(maximum_connections) {}

public:
  static unique_ptr<AdbcPoolConnection>
  GetEphemeralConnection(const string &uri);
  unique_ptr<AdbcPoolConnection> GetConnection();
  void ReturnConnection(unique_ptr<AdbcConnection> connection);

private:
  idx_t active_connections = 0;
  string uri;
  idx_t maximum_connections;
  std::mutex connection_mutex;
  vector<unique_ptr<AdbcConnection>> connections;
};

} // namespace adbc
} // namespace duckdb
