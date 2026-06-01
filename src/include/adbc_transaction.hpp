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

#pragma once

#include "adbc_catalog.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/original/std/memory.hpp"
#include "duckdb/storage/storage_extension.hpp"
#include "duckdb/transaction/transaction.hpp"
#include "duckdb/transaction/transaction_manager.hpp"

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
