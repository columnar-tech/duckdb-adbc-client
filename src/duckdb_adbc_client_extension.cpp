#define DUCKDB_EXTENSION_MAIN

#include "duckdb_adbc_client_extension.hpp"
#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/function/scalar_function.hpp"
#include <duckdb/parser/parsed_data/create_scalar_function_info.hpp>

namespace duckdb {

inline void DuckdbAdbcClientScalarFun(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &name_vector = args.data[0];
	UnaryExecutor::Execute<string_t, string_t>(name_vector, result, args.size(), [&](string_t name) {
		return StringVector::AddString(result, "DuckdbAdbcClient " + name.GetString() + " 🐥");
	});
}

static void LoadInternal(ExtensionLoader &loader) {
	// Register a scalar function
	auto duckdb_adbc_client_scalar_function =
	    ScalarFunction("duckdb_adbc_client", {LogicalType::VARCHAR}, LogicalType::VARCHAR, DuckdbAdbcClientScalarFun);
	loader.RegisterFunction(duckdb_adbc_client_scalar_function);
}

void DuckdbAdbcClientExtension::Load(ExtensionLoader &loader) {
	LoadInternal(loader);
}
std::string DuckdbAdbcClientExtension::Name() {
	return "duckdb_adbc_client";
}

std::string DuckdbAdbcClientExtension::Version() const {
#ifdef EXT_VERSION_DUCKDB_ADBC_CLIENT
	return EXT_VERSION_DUCKDB_ADBC_CLIENT;
#else
	return "";
#endif
}

} // namespace duckdb

extern "C" {

DUCKDB_CPP_EXTENSION_ENTRY(duckdb_adbc_client, loader) {
	duckdb::LoadInternal(loader);
}
}
