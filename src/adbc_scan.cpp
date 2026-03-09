#include "adbc_scan.hpp"
#include "adbc_raii.hpp"
#include "adbc-vendor/adbc.hpp"
#include "adbc-vendor/adbc_driver_manager.hpp"
#include "duckdb/function/table/arrow.hpp"
#include "duckdb/common/arrow/arrow.hpp"

#define CHECK_ADBC(EXPR, EXCEPTION_TYPE)                                       \
  do {                                                                         \
    AdbcStatusCode status = (EXPR);                                            \
    if (status != ADBC_STATUS_OK) {                                            \
      auto message = ToString(&error);                                         \
      throw EXCEPTION_TYPE(message);                                           \
    }                                                                          \
  } while (false)

namespace duckdb {
namespace adbc {

using namespace Private;

static void InitializeDatabase(Private::AdbcDatabase *database,
                               const string &uri) {
  // Initialize the database
  Private::AdbcError error = {};
  CHECK_ADBC(AdbcDatabaseNew(database, &error), BinderException);
  CHECK_ADBC(AdbcDatabaseSetOption(database, "uri", uri.c_str(), &error),
             BinderException);
  CHECK_ADBC(AdbcDriverManagerDatabaseSetLoadFlags(
                 database, ADBC_LOAD_FLAG_DEFAULT, &error),
             BinderException);
  CHECK_ADBC(AdbcDatabaseInit(database, &error), BinderException);
}

static void InitializeConnection(Private::AdbcDatabase *database,
                                 Private::AdbcConnection *connection) {
  // Initialize the connection
  Private::AdbcError error = {};
  CHECK_ADBC(AdbcConnectionNew(connection, &error), BinderException);
  CHECK_ADBC(AdbcConnectionInit(connection, database, &error), BinderException);
}

static void InitializeStatement(Private::AdbcConnection *connection,
                                Private::AdbcStatement *statement,
                                const string &query_text) {
  // Initialize the statement
  Private::AdbcError error = {};
  CHECK_ADBC(AdbcStatementNew(connection, statement, &error), BinderException);
  CHECK_ADBC(AdbcStatementSetSqlQuery(statement, query_text.c_str(), &error),
             BinderException);
}

static vector<string>
GetTableNamesFromConnection(Private::AdbcConnection *connection) {
  // Retrieve all table names from the connection
  vector<string> table_names;
  Private::AdbcError error = {};

  // Fetch the hierarchical objects from the connection
  Private::ArrowArrayStream stream;
  CHECK_ADBC(AdbcConnectionGetObjects(connection, ADBC_OBJECT_DEPTH_TABLES,
                                      nullptr, nullptr, nullptr, nullptr,
                                      nullptr, &stream, &error),
             BinderException);

  // Iterate the results
  while (true) {
    Private::ArrowArray batch = {};
    if (stream.get_next(&stream, &batch) != 0 || batch.release == nullptr) {
      break;
    }

    // Get the catalogs
    Private::ArrowArray *catalogs = &batch;
    Private::ArrowArray *catalog_schemas_list = batch.children[1];
    for (int64_t i = 0; i < catalogs->length; ++i) {
      const int32_t *schema_offsets =
          (const int32_t *)catalog_schemas_list->buffers[1];
      int32_t schema_start = schema_offsets[i];
      int32_t schema_end = schema_offsets[i + 1];

      Private::ArrowArray *schemas_struct = catalog_schemas_list->children[0];

      // Get the schemas for this catalog
      for (int32_t j = schema_start; j < schema_end; ++j) {
        Private::ArrowArray *tables_list = schemas_struct->children[1];
        const int32_t *table_offsets = (const int32_t *)tables_list->buffers[1];
        int32_t table_start = table_offsets[j];
        int32_t table_end = table_offsets[j + 1];

        Private::ArrowArray *table_struct = tables_list->children[0];
        Private::ArrowArray *table_names_array = table_struct->children[0];

        const int32_t *name_offsets =
            (const int32_t *)table_names_array->buffers[1];
        const char *name_data = (const char *)table_names_array->buffers[2];

        // Get the tables for each schema
        for (int32_t k = table_start; k < table_end; ++k) {
          int32_t start = name_offsets[k];
          int32_t end = name_offsets[k + 1];
          table_names.emplace_back(name_data + start, end - start);
        }
      }
    }
    batch.release(&batch);
  }
  stream.release(&stream);
  return table_names;
}

// A factory class that holds the ADBC connection state and produces
// ArrowArrayStreamWrapper instances
class AdbcArrowStreamFactory {
public:
  AdbcArrowStreamFactory(const string &uri, const string &query_text)
      : database(), connection(), statement() {
    InitializeDatabase(database.get(), uri);
    InitializeConnection(database.get(), connection.get());
    InitializeStatement(connection.get(), statement.get(), query_text);
  }

  AdbcStatement *GetStatement() { return statement.get(); }

private:
  Handle<Private::AdbcDatabase> database;
  Handle<Private::AdbcConnection> connection;
  Handle<Private::AdbcStatement> statement;
};

unique_ptr<ArrowArrayStreamWrapper>
AdbcProduceArrowScan(uintptr_t factory_ptr, ArrowStreamParameters &parameters) {
  // Reinterpret the factory pointer to the correct class
  auto factory = reinterpret_cast<AdbcArrowStreamFactory *>(factory_ptr);

  // Create the stream for the query result
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
