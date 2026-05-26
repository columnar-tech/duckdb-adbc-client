#pragma once

#include "adbc_util.hpp"
#include <mutex>
#include <memory>
#include "duckdb/common/shared_ptr.hpp"

namespace duckdb {
namespace adbc {

class AdbcCatalog;
class AdbcConnectionPool;

class AdbcPooledConnection {
public:
  AdbcPooledConnection(shared_ptr<AdbcConnectionPool> pool,
                       unique_ptr<AdbcConnection> conn, bool ephemeral)
      : pool(pool), connection(std::move(conn)), ephemeral(ephemeral) {}
  ~AdbcPooledConnection();

  AdbcConnection &GetConnection() { return *connection; }
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
  AdbcConnectionPool(const string &uri, idx_t max_connections)
      : uri(uri), max_connections(max_connections) {}

public:
  static unique_ptr<AdbcPooledConnection>
  GetEphemeralConnection(const string &uri);
  unique_ptr<AdbcPooledConnection> GetConnection();
  void ReturnConnection(unique_ptr<AdbcConnection> connection);

private:
  idx_t active_connections = 0;
  string uri;
  idx_t max_connections;
  std::mutex connection_mutex;
  vector<unique_ptr<AdbcConnection>> connections;
};

} // namespace adbc
} // namespace duckdb
