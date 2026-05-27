#include "adbc_storage.hpp"
#include "adbc_catalog.hpp"
#include "adbc_transaction.hpp"
#include "duckdb/main/settings.hpp"

namespace duckdb {
namespace adbc {

static unique_ptr<Catalog>
AdbcAttach(optional_ptr<StorageExtensionInfo> storage_info,
           ClientContext &context, AttachedDatabase &db, const string &name,
           AttachInfo &info, AttachOptions &attach_options) {

  auto &config = DBConfig::GetConfig(context);
  if (!config.options.enable_external_access) {
    throw PermissionException(
        "Attaching via ADBC is turned off through configuration.");
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
