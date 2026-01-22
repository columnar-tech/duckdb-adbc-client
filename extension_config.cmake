# Load your extension in DuckDB CI
duckdb_extension_load(adbc
    SOURCE_DIR ${CMAKE_CURRENT_LIST_DIR}
    LOAD_TESTS
)