#include "adbc_scan.hpp"
#include "duckdb/common/adbc/adbc.hpp"
#include "duckdb/function/table/arrow.hpp"

#define CHECK_ADBC(EXPR, EXCEPTION_TYPE)                                                                               \
	do {                                                                                                               \
		AdbcStatusCode status = (EXPR);                                                                                \
		if (status != ADBC_STATUS_OK) {                                                                                \
			if (error.message != nullptr) {                                                                            \
				throw EXCEPTION_TYPE(error.message);                                                                   \
			}                                                                                                          \
		}                                                                                                              \
	} while (false)

namespace duckdb {
namespace adbc {

// A factory class that holds the ADBC connection state and produces ArrowArrayStreamWrapper instances
struct AdbcArrowStreamFactory {

	AdbcArrowStreamFactory(string uri, string query_text)
	    : database(make_uniq<AdbcDatabase>()), connection(make_uniq<AdbcConnection>()),
	      statement(make_uniq<AdbcStatement>()) {

		AdbcError error = {};

		// Create the database instance (TODO: Remove the driver flag when it can be inferred from the URI)
		CHECK_ADBC(AdbcDatabaseNew(database.get(), &error), BinderException);
		CHECK_ADBC(AdbcDatabaseSetOption(database.get(), "driver", "postgresql", &error), BinderException);
		CHECK_ADBC(AdbcDatabaseSetOption(database.get(), "uri", uri.c_str(), &error), BinderException);
		CHECK_ADBC(AdbcDatabaseInit(database.get(), &error), BinderException);

		// Create the ADBC connection
		CHECK_ADBC(AdbcConnectionNew(connection.get(), &error), BinderException);
		CHECK_ADBC(AdbcConnectionInit(connection.get(), database.get(), &error), BinderException);

		// Create the ADBC statement
		CHECK_ADBC(AdbcStatementNew(connection.get(), statement.get(), &error), BinderException);
		CHECK_ADBC(AdbcStatementSetSqlQuery(statement.get(), query_text.c_str(), &error), BinderException);
	}

	~AdbcArrowStreamFactory() {

		AdbcError error = {};

		// Release the ADBC statement
		AdbcStatementRelease(statement.get(), &error);

		// Release the ADBC connection
		AdbcConnectionRelease(connection.get(), &error);

		// Release the ADBC database
		AdbcDatabaseRelease(database.get(), &error);
	}

	unique_ptr<AdbcDatabase> database;
	unique_ptr<AdbcConnection> connection;
	unique_ptr<AdbcStatement> statement;
};

unique_ptr<ArrowArrayStreamWrapper> AdbcProduceArrowScan(uintptr_t factory_ptr, ArrowStreamParameters &parameters) {
	// Reinterpret the factory pointer to the correct class
	auto factory = reinterpret_cast<AdbcArrowStreamFactory *>(factory_ptr);

	// Create the stream for the query result
	AdbcError error = {};
	ArrowArrayStream adbc_stream = {};
	int64_t rows_affected;
	CHECK_ADBC(AdbcStatementExecuteQuery(factory->statement.get(), &adbc_stream, &rows_affected, &error), IOException);

	// Create and return the wrapper owning the stream for DuckDB
	auto wrapper = make_uniq<ArrowArrayStreamWrapper>();
	wrapper->arrow_array_stream = adbc_stream;
	wrapper->number_of_rows = rows_affected;
	return wrapper;
}

// A wrapper class to take ownership of the factory object (and the corresponding ADBC state) during the scan
class AdbcArrowScanFunctionData : public ArrowScanFunctionData {
public:
	// Pass the factory and the factory function that creates an ArrowArrayStream
	AdbcArrowScanFunctionData(ClientContext &context, unique_ptr<AdbcArrowStreamFactory> factory)
	    : ArrowScanFunctionData(AdbcProduceArrowScan, reinterpret_cast<uintptr_t>(factory.get())),
	      adbc_arrow_stream_factory(std::move(factory)) {

		AdbcError error = {};

		// Retrieve and register the schema information from ADBC with DuckDB
		CHECK_ADBC(
		    AdbcStatementExecuteSchema(adbc_arrow_stream_factory->statement.get(), &schema_root.arrow_schema, &error),
		    BinderException);
		ArrowTableFunction::PopulateArrowTableSchema(DBConfig::GetConfig(context), arrow_table,
		                                             schema_root.arrow_schema);
	}

private:
	unique_ptr<AdbcArrowStreamFactory> adbc_arrow_stream_factory;
};

unique_ptr<FunctionData> AdbcScanBindFunction(ClientContext &context, TableFunctionBindInput &input,
                                              vector<LogicalType> &return_types, vector<string> &names) {

	// Validate that the function was provided exactly two input parameters
	if (input.inputs.size() != 2) {
		throw BinderException("read_adbc(...) requires two parameters: (1) the adbc URI (2) the SQL query string");
	}

	// Get the input parameters
	auto uri = input.inputs[0].GetValue<string>();
	auto query_text = input.inputs[1].GetValue<string>();

	// Create the factory object which holds the ADBC state for the lifetime of the scan
	auto adbc_arrow_stream_factory = make_uniq<AdbcArrowStreamFactory>(uri, query_text);

	// Create a function data object which registers the ADBC schema with DuckDB and owns the factory for the scan
	auto function_data = make_uniq<AdbcArrowScanFunctionData>(context, std::move(adbc_arrow_stream_factory));

	// Assign the column names and types
	names = function_data->arrow_table.GetNames();
	return_types = function_data->arrow_table.GetTypes();
	function_data->all_types = return_types;
	return function_data;
}
} // namespace adbc
} // namespace duckdb