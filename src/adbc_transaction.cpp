// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "adbc_transaction.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/main/attached_database.hpp"

// Calculate the buffer size based on the actual constants
#define MAX_OPTION_LEN                                                                                                 \
    (sizeof(ADBC_OPTION_VALUE_ENABLED) > sizeof(ADBC_OPTION_VALUE_DISABLED) ? sizeof(ADBC_OPTION_VALUE_ENABLED)        \
                                                                            : sizeof(ADBC_OPTION_VALUE_DISABLED))

namespace duckdb {
namespace adbc {

AdbcTransaction::AdbcTransaction(TransactionManager &manager, ClientContext &context) : Transaction(manager, context) {
}

AdbcTransactionManager::AdbcTransactionManager(AttachedDatabase &db, Catalog &catalog)
    : TransactionManager(db), catalog(catalog) {
}

Transaction &AdbcTransactionManager::StartTransaction(ClientContext &context) {
    // Ensure that auto-commit is enabled
    auto &adbc_catalog = catalog.Cast<AdbcCatalog>();

    // Ceate a buffer for the option value returned via ADBC
    char option_value[MAX_OPTION_LEN] = {0};
    size_t option_length = MAX_OPTION_LEN;
    Private::AdbcError error = {};
    CHECK_ADBC(AdbcConnectionGetOption(adbc_catalog.GetPooledConnection()->GetRawConnection(),
                                       ADBC_CONNECTION_OPTION_AUTOCOMMIT,
                                       option_value,
                                       &option_length,
                                       &error),
               IOException);

    bool auto_commit_enabled = (strcmp(option_value, ADBC_OPTION_VALUE_ENABLED) == 0);

    if (!auto_commit_enabled) {
        throw NotImplementedException("Running with the auto-commit option turned off is not yet supported "
                                      "with the ADBC extension");
    }

    // Create the transaction
    auto transaction = make_uniq<AdbcTransaction>(*this, context);
    auto &result = *transaction;
    {
        lock_guard<mutex> map_lock(map_mutex);
        transactions[result] = std::move(transaction);
    }
    return result;
}

ErrorData AdbcTransactionManager::CommitTransaction(ClientContext &context, Transaction &transaction) {
    // Remove the committed transaction and release the lock
    lock_guard<mutex> map_lock(map_mutex);
    transactions.erase(transaction);
    return ErrorData();
}

void AdbcTransactionManager::RollbackTransaction(Transaction &transaction) {
    // Remove the transaction we are rolling back and release the lock
    lock_guard<mutex> map_lock(map_mutex);
    transactions.erase(transaction);
}

void AdbcTransactionManager::Checkpoint(ClientContext &context, bool force) {
    // No-op for checkpointing
}

unique_ptr<TransactionManager> AdbcCreateTransactionManager(optional_ptr<StorageExtensionInfo> storage_info,
                                                            AttachedDatabase &db,
                                                            Catalog &catalog) {
    return make_uniq<AdbcTransactionManager>(db, catalog);
}

} // namespace adbc
} // namespace duckdb
