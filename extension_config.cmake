# extension_config.cmake
# DuckDB CI-compatible extension config

# Force C++20 for this extension
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

# Root of your extension
set(EXT_ROOT "${CMAKE_CURRENT_LIST_DIR}")

# Add headers to include path
include_directories("${EXT_ROOT}/src/include")
include_directories("${CMAKE_SOURCE_DIR}/src/include")

# Extension name
set(EXTENSION_NAME adbc)

# Extension sources (absolute paths)
set(EXTENSION_SOURCES
    "${EXT_ROOT}/src/adbc_extension.cpp"
    "${EXT_ROOT}/src/adbc_scan.cpp"
    "${EXT_ROOT}/src/adbc_driver_manager.cpp"
)

# DuckDB API version
add_definitions(-DDUCKDB_EXTENSION_NAME=${EXTENSION_NAME})

# Build static and loadable extensions
build_static_extension(${EXTENSION_NAME} ${EXTENSION_SOURCES})
build_loadable_extension(${EXTENSION_NAME} " " ${EXTENSION_SOURCES})
