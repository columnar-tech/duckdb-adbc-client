#pragma once

#include "duckdb/storage/storage_extension.hpp"

namespace duckdb {
namespace adbc {
class AdbcStorageExtension : public StorageExtension {
public:
	AdbcStorageExtension();
};
} // namespace adbc
} // namespace duckdb
