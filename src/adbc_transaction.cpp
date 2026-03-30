#include "adbc_transaction.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/main/attached_database.hpp"

namespace duckdb {
namespace adbc {

AdbcTransaction::AdbcTransaction(TransactionManager &manager,
                                 ClientContext &context)
    : Transaction(manager, context) {}

AdbcTransactionManager::AdbcTransactionManager(AttachedDatabase &db)
    : TransactionManager(db) {}

Transaction &AdbcTransactionManager::StartTransaction(ClientContext &context) {
  // Create a new transaction
  auto transaction = make_uniq<AdbcTransaction>(*this, context);
  auto &result = *transaction;
  lock_guard<mutex> lock(transaction_lock);
  transactions[result] = std::move(transaction);
  return result;
}

ErrorData AdbcTransactionManager::CommitTransaction(ClientContext &context,
                                                    Transaction &transaction) {
  // Remove the committed transaction
  lock_guard<mutex> lock(transaction_lock);
  transactions.erase(transaction);
  return ErrorData();
}

void AdbcTransactionManager::RollbackTransaction(Transaction &transaction) {
  // Remove the transaction we are rolling back
  lock_guard<mutex> lock(transaction_lock);
  transactions.erase(transaction);
}

void AdbcTransactionManager::Checkpoint(ClientContext &context, bool force) {}

unique_ptr<TransactionManager>
AdbcCreateTransactionManager(optional_ptr<StorageExtensionInfo> storage_info,
                             AttachedDatabase &db, Catalog &catalog) {
  return make_uniq<AdbcTransactionManager>(db);
}

} // namespace adbc
} // namespace duckdb
