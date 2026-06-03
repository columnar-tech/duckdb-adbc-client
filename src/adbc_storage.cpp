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

#include "adbc_storage.hpp"
#include "adbc_catalog.hpp"
#include "adbc_transaction.hpp"
#include "duckdb/main/settings.hpp"

namespace duckdb {
namespace adbc {

static unique_ptr<Catalog> AdbcAttach(optional_ptr<StorageExtensionInfo> storage_info,
                                      ClientContext &context,
                                      AttachedDatabase &db,
                                      const string &name,
                                      AttachInfo &info,
                                      AttachOptions &attach_options) {

#if DUCKDB_MAJOR_VERSION >= 1 && DUCKDB_MINOR_VERSION >= 5
    if (!Settings::Get<EnableExternalAccessSetting>(context)) {
#else
    auto &config = DBConfig::GetConfig(context);
    if (!config.options.enable_external_access) {
#endif
        throw PermissionException("Attaching via ADBC is turned off through configuration.");
    }

    auto uri = info.path;

    string delimiter = "\"\"";

    for (auto &[option, input_value] : attach_options.options) {
        if (StringUtil::Lower(option) == "delimiter") {
            auto value = input_value.ToString();
            if (value.size() != 2) {
                throw InvalidInputException("Invalid value \"%s\" for DELIMITER. It "
                                            "must be exactly two characters.",
                                            value);
            }
            delimiter = value;
            break;
        }
        throw BinderException("Unrecognized option for ADBC attach: %s", option);
    }

    return make_uniq<AdbcCatalog>(db, context, uri, delimiter);
}

static unique_ptr<TransactionManager> AdbcCreateTransactionManager(optional_ptr<StorageExtensionInfo> storage_info,
                                                                   AttachedDatabase &db,
                                                                   Catalog &catalog) {
    return make_uniq<AdbcTransactionManager>(db, catalog);
}

AdbcStorageExtension::AdbcStorageExtension() {
    attach = AdbcAttach;
    create_transaction_manager = AdbcCreateTransactionManager;
}

} // namespace adbc
} // namespace duckdb
