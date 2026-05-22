#include "adbc_storage.hpp"
#include "adbc_catalog.hpp"
#include "adbc_transaction.hpp"

namespace duckdb {
namespace adbc {

static unique_ptr<Catalog>
AdbcAttach(optional_ptr<StorageExtensionInfo> storage_info,
           ClientContext &context, AttachedDatabase &db, const string &name,
           AttachInfo &info, AttachOptions &attach_options) {
  auto uri = info.path;
  return make_uniq<AdbcCatalog>(db, context, uri);
}

static unique_ptr<TransactionManager>
AdbcCreateTransactionManager(optional_ptr<StorageExtensionInfo> storage_info,
                             AttachedDatabase &db, Catalog &catalog) {
  return make_uniq<AdbcTransactionManager>(db, catalog);
}

AdbcStorageExtension::AdbcStorageExtension() {
  attach = AdbcAttach;
  create_transaction_manager = AdbcCreateTransactionManager;
}

} // namespace adbc
} // namespace duckdb
