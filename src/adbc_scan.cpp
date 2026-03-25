#include <mutex>
#include "adbc_scan.hpp"
#include "adbc-vendor/adbc.hpp"
#include "adbc-vendor/adbc_driver_manager.hpp"
#include "duckdb/function/table/arrow.hpp"
#include "duckdb/common/arrow/arrow.hpp"

namespace duckdb {
namespace adbc {

using namespace Private;

void InitializeDatabase(SharedAdbcConnection &shared_connection,
                        const string &uri) {
  // Initialize the database
  std::lock_guard<std::mutex> connection_lock(shared_connection.GetMutex());
  auto *database = shared_connection.GetDatabase();
  Private::AdbcError error = {};
  CHECK_ADBC(AdbcDatabaseNew(database, &error), BinderException);
  CHECK_ADBC(AdbcDatabaseSetOption(database, "uri", uri.c_str(), &error),
             BinderException);
  CHECK_ADBC(AdbcDriverManagerDatabaseSetLoadFlags(
                 database, ADBC_LOAD_FLAG_DEFAULT, &error),
             BinderException);
  CHECK_ADBC(AdbcDatabaseInit(database, &error), BinderException);
}

void InitializeConnection(SharedAdbcConnection &shared_connection) {
  // Initialize the connection
  std::lock_guard<std::mutex> connection_lock(shared_connection.GetMutex());
  auto *connection = shared_connection.GetConnection();
  auto *database = shared_connection.GetDatabase();
  Private::AdbcError error = {};
  CHECK_ADBC(AdbcConnectionNew(connection, &error), BinderException);
  CHECK_ADBC(AdbcConnectionInit(connection, database, &error), BinderException);
}

void InitializeStatement(SharedAdbcConnection &shared_connection,
                         Private::AdbcStatement *statement,
                         const string &query_text) {
  // Initialize the statement
  std::lock_guard<std::mutex> connection_lock(shared_connection.GetMutex());
  auto *connection = shared_connection.GetConnection();
  Private::AdbcError error = {};
  CHECK_ADBC(AdbcStatementNew(connection, statement, &error), BinderException);
  CHECK_ADBC(AdbcStatementSetSqlQuery(statement, query_text.c_str(), &error),
             BinderException);
}

// A factory class that holds the ADBC connection state and produces
// ArrowArrayStreamWrapper instances
class AdbcArrowStreamFactory {
public:
  AdbcArrowStreamFactory(const string &uri, const string &query_text)
      : shared_connection(make_shared_ptr<SharedAdbcConnection>()),
        statement() {
    InitializeDatabase(*shared_connection, uri);
    InitializeConnection(*shared_connection);
    InitializeStatement(*shared_connection, statement.get(), query_text);
  }

  std::mutex &GetMutex() { return shared_connection->GetMutex(); }
  AdbcStatement *GetStatement() { return statement.get(); }

private:
  shared_ptr<SharedAdbcConnection> shared_connection;
  Handle<Private::AdbcStatement> statement;
};

unique_ptr<ArrowArrayStreamWrapper>
AdbcProduceArrowScan(uintptr_t factory_ptr, ArrowStreamParameters &parameters) {
  // Reinterpret the factory pointer to the correct class
  auto factory = reinterpret_cast<AdbcArrowStreamFactory *>(factory_ptr);

  // Create the stream for the query result
  std::lock_guard<std::mutex> connection_lock(factory->GetMutex());
  AdbcError error = {};
  Private::ArrowArrayStream adbc_stream = {};
  int64_t rows_affected;
  CHECK_ADBC(AdbcStatementExecuteQuery(factory->GetStatement(), &adbc_stream,
                                       &rows_affected, &error),
             IOException);

  // Create and return the wrapper owning the stream for DuckDB
  auto wrapper = make_uniq<ArrowArrayStreamWrapper>();
  std::memcpy(&wrapper->arrow_array_stream, &adbc_stream,
              sizeof(Private::ArrowArrayStream));
  wrapper->number_of_rows = rows_affected;
  return wrapper;
}

// A wrapper class to take ownership of the factory object (and the
// corresponding ADBC state) during the scan
class AdbcArrowScanFunctionData : public ArrowScanFunctionData {
public:
  // Pass the factory and the factory function that creates an ArrowArrayStream
  AdbcArrowScanFunctionData(ClientContext &context,
                            unique_ptr<AdbcArrowStreamFactory> factory)
      : ArrowScanFunctionData(AdbcProduceArrowScan,
                              reinterpret_cast<uintptr_t>(factory.get())),
        adbc_arrow_stream_factory(std::move(factory)) {

    // Retrieve and register the schema information from ADBC with DuckDB
    std::lock_guard<std::mutex> connection_lock(
        adbc_arrow_stream_factory->GetMutex());
    AdbcError error = {};
    auto *statement = adbc_arrow_stream_factory->GetStatement();
    auto *schema =
        reinterpret_cast<Private::ArrowSchema *>(&schema_root.arrow_schema);
    CHECK_ADBC(AdbcStatementExecuteSchema(statement, schema, &error),
               BinderException);
    ArrowTableFunction::PopulateArrowTableSchema(
        DBConfig::GetConfig(context), arrow_table, schema_root.arrow_schema);
  }

private:
  unique_ptr<AdbcArrowStreamFactory> adbc_arrow_stream_factory;
};

void AdbcScanFunction(ClientContext &context, TableFunctionInput &input,
                      DataChunk &output) {

  // We closely follow the DuckDB Arrow extension's scan function from:
  // https://github.com/duckdb/arrow/
  if (!input.local_state) {
    return;
  }

  auto &function_data =
      input.bind_data->CastNoConst<AdbcArrowScanFunctionData>();
  auto &global_state = input.global_state->Cast<ArrowScanGlobalState>();
  auto &local_state = input.local_state->Cast<ArrowScanLocalState>();

  // Need more tuples in the current chunk
  if (local_state.chunk_offset >=
      static_cast<idx_t>(local_state.chunk->arrow_array.length)) {
    // Fetch them and exit if there are no more tuples left
    if (!ArrowTableFunction::ArrowScanParallelStateNext(
            context, input.bind_data.get(), local_state, global_state)) {
      return;
    }
  }

  // Compute the number of tuples read (and therefore output size)
  idx_t output_size = MinValue<idx_t>(STANDARD_VECTOR_SIZE,
                                      local_state.chunk->arrow_array.length -
                                          local_state.chunk_offset);
  function_data.lines_read += output_size;

  // Handle the case where we don't need all of the columns
  if (global_state.CanRemoveFilterColumns()) {
    local_state.all_columns.Reset();
    local_state.all_columns.SetCapacity(output_size);
    ArrowTableFunction::ArrowToDuckDB(
        local_state, function_data.arrow_table.GetColumns(),
        local_state.all_columns, function_data.lines_read - output_size, false);
    // Map the columns produced by the ADBC scan to the expected projection
    // column
    output.ReferenceColumns(local_state.all_columns,
                            global_state.projection_ids);
  } else {
    output.SetCardinality(output_size);
    ArrowTableFunction::ArrowToDuckDB(
        local_state, function_data.arrow_table.GetColumns(), output,
        function_data.lines_read - output_size, false);
  }

  output.Verify();
  local_state.chunk_offset += output.size();
}

unique_ptr<FunctionData> AdbcScanBindFunction(ClientContext &context,
                                              TableFunctionBindInput &input,
                                              vector<LogicalType> &return_types,
                                              vector<string> &names) {

  // Validate that the function was provided exactly two input parameters
  if (input.inputs.size() != 2) {
    throw BinderException("read_adbc(...) requires two parameters: (1) the "
                          "adbc URI (2) the SQL query string");
  }

  // Get the input parameters
  auto uri = input.inputs[0].GetValue<string>();
  auto query_text = input.inputs[1].GetValue<string>();

  // Create the factory object which holds the ADBC state for the lifetime of
  // the scan
  auto adbc_arrow_stream_factory =
      make_uniq<AdbcArrowStreamFactory>(uri, query_text);

  // Create a function data object which registers the ADBC schema with DuckDB
  // and owns the factory for the scan
  auto function_data = make_uniq<AdbcArrowScanFunctionData>(
      context, std::move(adbc_arrow_stream_factory));

  // Assign the column names and types
  names = function_data->arrow_table.GetNames();
  return_types = function_data->arrow_table.GetTypes();
  function_data->all_types = return_types;
  return function_data;
}

} // namespace adbc
} // namespace duckdb
