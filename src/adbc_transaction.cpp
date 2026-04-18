#include "adbc_transaction.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/main/attached_database.hpp"

// Calculate the buffer size based on the actual constants
#define MAX_OPTION_LEN                                                         \
  (sizeof(ADBC_OPTION_VALUE_ENABLED) > sizeof(ADBC_OPTION_VALUE_DISABLED)      \
       ? sizeof(ADBC_OPTION_VALUE_ENABLED)                                     \
       : sizeof(ADBC_OPTION_VALUE_DISABLED))

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
  transaction_lock.lock();

  // Check to make sure auto-commit mode is on
  auto &adbc_catalog = catalog.Cast<AdbcCatalog>();
  auto shared_connection = adbc_catalog.GetSharedConnection();
  auto *connection = shared_connection->GetConnection();

  // We create a buffer for the option value returned via ADBC
  char option_value[MAX_OPTION_LEN] = {0};
  size_t option_length = MAX_OPTION_LEN;
  Private::AdbcError error = {};
  CHECK_ADBC(AdbcConnectionGetOption(connection,
                                     ADBC_CONNECTION_OPTION_AUTOCOMMIT,
                                     option_value, &option_length, &error),
             IOException);

  // Throw an exception if auto-commit is not turned on
  if (strcmp(option_value, ADBC_OPTION_VALUE_ENABLED) != 0) {
    throw NotImplementedException(
        "Running with the auto-commit option turned off is not yet supported "
        "with the ADBC extension");
  }

  // Continue creating the transaction, serializing all transactions with a
  // shared lock
  auto transaction = make_uniq<AdbcTransaction>(*this, context);
  auto &result = *transaction;
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
