#include "adbc_transaction.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/main/attached_database.hpp"

namespace duckdb {
namespace adbc {

AdbcTransaction::AdbcTransaction(TransactionManager &manager,
                                 ClientContext &context)
    : Transaction(manager, context) {}

AdbcTransactionManager::AdbcTransactionManager(AttachedDatabase &db,
                                               Catalog &catalog)
    : TransactionManager(db), catalog(catalog) {}

Transaction &AdbcTransactionManager::StartTransaction(ClientContext &context) {
  // Create a new transaction
  auto transaction = make_uniq<AdbcTransaction>(*this, context);
  auto &result = *transaction;
  // Serialize all transactions by holding the lock for their entire duration
  transaction_lock.lock();
  transactions[result] = std::move(transaction);
  return result;
}

ErrorData AdbcTransactionManager::CommitTransaction(ClientContext &context,
                                                    Transaction &transaction) {
  // Remove the committed transaction and release the lock
  transactions.erase(transaction);
  transaction_lock.unlock();
  return ErrorData();
}

void AdbcTransactionManager::RollbackTransaction(Transaction &transaction) {
  // Get the active connection
  auto &adbc_catalog = catalog.Cast<AdbcCatalog>();
  auto shared_connection = adbc_catalog.GetSharedConnection();
  auto *connection = shared_connection->GetConnection();

  // Cancel any in-progress operations
  Private::AdbcError error = {};
  CHECK_ADBC(AdbcConnectionCancel(connection, &error), IOException);

  // Remove the transaction we are rolling back and release the lock
  transactions.erase(transaction);
  transaction_lock.unlock();
}

void AdbcTransactionManager::Checkpoint(ClientContext &context, bool force) {
  // No-op for checkpointing
}

unique_ptr<TransactionManager>
AdbcCreateTransactionManager(optional_ptr<StorageExtensionInfo> storage_info,
                             AttachedDatabase &db, Catalog &catalog) {
  return make_uniq<AdbcTransactionManager>(db, catalog);
}

} // namespace adbc
} // namespace duckdb
