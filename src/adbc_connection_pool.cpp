#include "adbc_connection_pool.hpp"

namespace duckdb {
namespace adbc {

AdbcPooledConnection::~AdbcPooledConnection() {
  if (!ephemeral) {
    pool->ReturnConnection(std::move(connection));
  }
}

unique_ptr<AdbcPooledConnection>
AdbcConnectionPool::GetEphemeralConnection(const string &uri) {
  return make_uniq<AdbcPooledConnection>(nullptr,
                                         make_uniq<AdbcConnection>(uri), true);
}

unique_ptr<AdbcPooledConnection> AdbcConnectionPool::GetConnection() {
  std::lock_guard<std::mutex> connection_lock(connection_mutex);

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
  return make_uniq<AdbcPooledConnection>(this, std::move(free_connection),
                                         false);
}

void AdbcConnectionPool::ReturnConnection(
    unique_ptr<AdbcConnection> connection) {
  std::lock_guard<std::mutex> connection_lock(connection_mutex);
  --active_connections;
  connections.push_back(std::move(connection));
}

} // namespace adbc
} // namespace duckdb
