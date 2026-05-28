#pragma once

#include "adbc_catalog.hpp"
#include "duckdb/original/std/memory.hpp"
#include "duckdb/storage/storage_extension.hpp"
#include "duckdb/transaction/transaction.hpp"
#include "duckdb/transaction/transaction_manager.hpp"
#include "duckdb/common/mutex.hpp"

namespace duckdb {
namespace adbc {

class AdbcTransaction : public Transaction {
public:
    AdbcTransaction(TransactionManager &manager, ClientContext &context);
    ~AdbcTransaction() override = default;
};

class AdbcTransactionManager : public TransactionManager {
public:
    explicit AdbcTransactionManager(AttachedDatabase &db, Catalog &catalog);
    ~AdbcTransactionManager() override = default;

    Transaction &StartTransaction(ClientContext &context) override;
    ErrorData CommitTransaction(ClientContext &context, Transaction &transaction) override;
    void RollbackTransaction(Transaction &transaction) override;
    void Checkpoint(ClientContext &context, bool force = false) override;

private:
    Catalog &catalog;
    mutex map_mutex;
    reference_map_t<Transaction, unique_ptr<Transaction>> transactions;
};

} // namespace adbc
} // namespace duckdb
