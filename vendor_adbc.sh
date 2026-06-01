#!/bin/bash

# Run this script after bumping the ADBC version in the submodule
# This will vendor all of the dependent ADBC code from that submodule into the extension

UTIL_PATH="./arrow-adbc/c/driver/common"
ADBC_HEADER_PATH="./arrow-adbc/c/include/arrow-adbc"
NANOARROW_PATH="./arrow-adbc/c/vendor/nanoarrow"
DRIVER_MANAGER_PATH="./arrow-adbc/c/driver_manager"
VENDOR_HEADER_PATH="./src/include/adbc-vendor"
VENDOR_IMPL_PATH="./src/adbc-vendor"

# Copy headers
cp $UTIL_PATH/utils.h $VENDOR_HEADER_PATH/adbc_utils.h
cp $ADBC_HEADER_PATH/adbc.h $VENDOR_HEADER_PATH/adbc.h
cp $ADBC_HEADER_PATH/adbc_driver_manager.h $VENDOR_HEADER_PATH/adbc_driver_manager.h
cp $DRIVER_MANAGER_PATH/adbc_driver_manager_internal.h $VENDOR_HEADER_PATH/adbc_driver_manager_internal.h

# Change extern C to extern C++ and inject DuckDB arrow includes
find "$VENDOR_HEADER_PATH" -type f -name "*.h" -exec sed -i 's/extern "C"[[:space:]]*{/#define ADBC_EXPORT\nextern "C++" {/g' {} +

# Move includes with adbc_ into arrow-adbc
find "$VENDOR_HEADER_PATH" -type f -name "*.h" -exec sed -i 's/#include "adbc_/#include "arrow-adbc\/adbc_/g' {} +

# Rename arrow-adbc to adbc-vendor
find "$VENDOR_HEADER_PATH" -type f -name "*.h" -exec sed -i 's/<arrow-adbc\([^"]*\)>/"adbc-vendor\1"/g' {} +
find "$VENDOR_HEADER_PATH" -type f -name "*.h" -exec sed -i 's/"arrow-adbc\([^"]*\)"/"adbc-vendor\1"/g' {} +

# Rename nanoarrow to adbc-vendor
find "$VENDOR_HEADER_PATH" -type f -name "*.h" -exec sed -i 's/#include "nanoarrow\/nanoarrow.h"/#include "adbc-vendor\/adbc_nanoarrow.h"/g' {} +

# Find the last #include statement and inject a namespace Private { ... }
find "$VENDOR_HEADER_PATH" -type f -name "*.h" | while read -r f; do
    last_include=$(grep -n "^#include" "$f" | tail -1 | cut -d: -f1)
    if [ -n "$last_include" ]; then
        sed -i "${last_include}a\\
namespace Private {" "$f"
        echo -e "\n} // namespace Private" >> "$f"
    fi
done

# Vendor Nanoarrow separately as it already uses namespace Private
cp $NANOARROW_PATH/nanoarrow.h $VENDOR_HEADER_PATH/adbc_nanoarrow.h

# Inject DuckDB Arrow header in adbc.h
sed -i 's|namespace Private {|#include "duckdb\/common\/arrow\/arrow.hpp"\nnamespace Private {|g' $VENDOR_HEADER_PATH/adbc.h

# Vendor the current_arch header
cp $DRIVER_MANAGER_PATH/current_arch.h $VENDOR_HEADER_PATH/adbc_current_arch.h

# Copy the driver manager implementation files
cp $UTIL_PATH/utils.c $VENDOR_IMPL_PATH/adbc_utils.cpp
cp $DRIVER_MANAGER_PATH/adbc_driver_manager.cc $VENDOR_IMPL_PATH/adbc_driver_manager.cpp
cp $DRIVER_MANAGER_PATH/adbc_driver_manager_api.cc $VENDOR_IMPL_PATH/adbc_driver_manager_api.cpp
cp $DRIVER_MANAGER_PATH/adbc_driver_manager_driver_loading.cc $VENDOR_IMPL_PATH/adbc_driver_manager_driver_loading.cpp
cp $DRIVER_MANAGER_PATH/adbc_driver_manager_profiles.cc $VENDOR_IMPL_PATH/adbc_driver_manager_profiles.cpp
# Fix struct return pattern for Windows CI
sed -i 's/(struct AdbcErrorDetail){/AdbcErrorDetail{/g' $VENDOR_IMPL_PATH/adbc_utils.cpp

# Rename utils.h to adbc_utils.h
find "$VENDOR_IMPL_PATH" -type f -name "*.cpp" -exec sed -i 's/utils\.h/adbc_utils\.h/g' {} +
# Rename current_arch.h to adbc_current_arch.h
find "$VENDOR_IMPL_PATH" -type f -name "*.cpp" -exec sed -i 's/current_arch\.h/adbc_current_arch\.h/g' {} +
# Move includes with adbc_ directly into adbc-vendor (no intermediate arrow-adbc step)
find "$VENDOR_IMPL_PATH" -type f -name "*.cpp" -exec sed -i 's/#include "adbc_/#include "adbc-vendor\/adbc_/g' {} +
# Rename arrow-adbc/ prefix to adbc-vendor/
find "$VENDOR_IMPL_PATH" -type f -name "*.cpp" -exec sed -i 's/<arrow-adbc\([^"]*\)>/"adbc-vendor\1"/g' {} +
find "$VENDOR_IMPL_PATH" -type f -name "*.cpp" -exec sed -i 's/"arrow-adbc\([^"]*\)"/"adbc-vendor\1"/g' {} +

# Fix malloc/calloc without type casts
find "$VENDOR_IMPL_PATH" -type f \( -name "*.cpp" \) | while read -r f; do
    sed -i -E 's/([a-zA-Z_][a-zA-Z0-9_ *]*\*+) *([a-zA-Z_][a-zA-Z0-9_]*) *= *(malloc|calloc)\(/\1 \2 = (\1)\3(/' "$f"
    sed -i -E 's/([^(])= *(malloc|calloc)\(/\1= (char*)\2(/' "$f"
done

# Find the last #include statement and inject a namespace Private { ... }
find "$VENDOR_IMPL_PATH" -type f -name "*.cpp" | while read -r f; do
    last_include=$(grep -n "^#include" "$f" | tail -1 | cut -d: -f1)
    if [ -n "$last_include" ]; then
        sed -i "${last_include}a\\
namespace Private {" "$f"
        echo -e "\n} // namespace Private" >> "$f"
    fi
done

# Vendor Nanoarrow separately is it already uses namespace Private
cp ./arrow-adbc/c/vendor/nanoarrow/nanoarrow.c $VENDOR_IMPL_PATH/adbc_nanoarrow.cpp
sed -i 's/nanoarrow.h/adbc-vendor\/adbc_nanoarrow.h/g' $VENDOR_IMPL_PATH/adbc_nanoarrow.cpp

# Clang-format everything to pass CI
python3 duckdb/scripts/format.py --all --fix --noconfirm --directories src test
