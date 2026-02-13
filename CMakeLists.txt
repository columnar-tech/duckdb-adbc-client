cmake_minimum_required(VERSION 3.5)

# Extension name
set(TARGET_NAME adbc)
set(EXTENSION_NAME ${TARGET_NAME}_extension)
set(LOADABLE_EXTENSION_NAME ${TARGET_NAME}_loadable_extension)

project(${TARGET_NAME} LANGUAGES CXX)

# C++20 standard
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

# Include headers
include_directories(src/include)
include_directories(arrow-adbc/c/vendor)

# Sources
set(EXTENSION_SOURCES
    src/adbc_extension.cpp
    src/adbc_scan.cpp
    src/adbc_driver_manager.cpp
)

# DuckDB API version
add_definitions(-DDUCKDB_EXTENSION_NAME=${EXTENSION_NAME})

# Build both static and loadable extensions
build_static_extension(${TARGET_NAME} ${EXTENSION_SOURCES})
build_loadable_extension(${TARGET_NAME} ${LOADABLE_EXTENSION_NAME} ${EXTENSION_SOURCES})

# Hide symbols for release builds
set(CMAKE_CXX_VISIBILITY_PRESET hidden)
set(CMAKE_C_VISIBILITY_PRESET hidden)
set(VISIBILITY_INLINES_HIDDEN ON)
set(CMAKE_C_FLAGS_RELEASE "${CMAKE_C_FLAGS_RELEASE} -s")
set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} -s")

# Install rules
install(
    TARGETS ${EXTENSION_NAME}
    EXPORT "${DUCKDB_EXPORT_SET}"
    LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
    ARCHIVE DESTINATION "${INSTALL_LIB_DIR}"
)
