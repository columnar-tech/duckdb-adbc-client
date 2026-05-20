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

  // Acquire a scoped lock just in case we throw an exception here
  auto &adbc_catalog = catalog.Cast<AdbcCatalog>();
  auto catalog_lock = adbc_catalog.AcquireScopedLock();

  // Throw if auto-commit mode is not enabled
  if (!adbc_catalog.AutocommitEnabled()) {
    throw NotImplementedException(
        "Running with the auto-commit option turned off is not yet supported "
        "with the ADBC extension");
  }

  // Create the transaction and hold the catalog lock for its duration
  adbc_catalog.mutex.lock();
  auto transaction = make_uniq<AdbcTransaction>(*this, context);
  auto &result = *transaction;
  transactions[result] = std::move(transaction);
  return result;
}

ErrorData AdbcTransactionManager::CommitTransaction(ClientContext &context,
                                                    Transaction &transaction) {
  // Remove the committed transaction and release the lock
  transactions.erase(transaction);
  auto &adbc_catalog = catalog.Cast<AdbcCatalog>();
  adbc_catalog.mutex.unlock();
  return ErrorData();
}

void AdbcTransactionManager::RollbackTransaction(Transaction &transaction) {
  // Remove the transaction we are rolling back and release the lock
  transactions.erase(transaction);
  auto &adbc_catalog = catalog.Cast<AdbcCatalog>();
  adbc_catalog.mutex.unlock();
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
