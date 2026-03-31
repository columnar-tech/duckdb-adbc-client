#!/bin/bash

# Run this script after bumping the ADBC version in the submodule
# This will vendor all of the dependent ADBC code from that submodule into the extension

ADBC_HEADER_PATH="./arrow-adbc/c/include/arrow-adbc"
DRIVER_MANAGER_PATH="./arrow-adbc/c/driver_manager"
VENDOR_PATH="./src/include/arrow-adbc"

# Copy the ADBC header
cp $ADBC_HEADER_PATH/adbc.h ./src/include/arrow-adbc/adbc.h

# Fix linkage and ensure Arrow structs are redefined
sed -i 's|extern "C"[[:space:]]*{|extern "C++" {\n#endif\n/*|g' ./src/include/arrow-adbc/adbc.h
sed -i 's|#ifndef ADBC|*/\n#ifndef ADBC|g' ./src/include/arrow-adbc/adbc.h

# Copy the driver manager header 
cp $ADBC_HEADER_PATH/adbc_driver_manager.h $VENDOR_PATH/adbc_driver_manager.h
# Again fix linkage
sed -i 's/extern "C"[[:space:]]*{/extern "C++" {/g' $VENDOR_PATH/adbc_driver_manager.h

# Copy a header file for the driver manager
cp $DRIVER_MANAGER_PATH/current_arch.h ./src/current_arch.h

# Copy the driver manager
cp $DRIVER_MANAGER_PATH/adbc_driver_manager.cc ./src/adbc_driver_manager.cpp
# Wrap it in namespace Private { ... }
sed -i 's|^\([[:space:]]*using namespace .*;\)|namespace Private {\n\1|' ./src/adbc_driver_manager.cpp
sed -i -e '$a } // namespace Private' ./src/adbc_driver_manager.cpp
# Rename headers to point to the vendored headers
sed -i 's|"arrow-adbc/\([^"]*\)\.h"|"adbc-vendor/\1.hpp"|g' ./src/adbc_driver_manager.cpp

# Add header for DuckDB's arrow headers
sed -i 's|#include "current_arch.h"|#include "duckdb/common/arrow/arrow.hpp"\n#include "current_arch.h"|g' ./src/adbc_driver_manager.cpp

# Clang-format everything to pass CI
python3 duckdb/scripts/format.py --all --fix --noconfirm --directories src test
