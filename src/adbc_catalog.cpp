// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "adbc_catalog.hpp"
#include "adbc_insert.hpp"
#include "adbc_schema_entry.hpp"
#include "adbc_table_entry.hpp"
#include "duckdb/execution/operator/scan/physical_table_scan.hpp"
#include "duckdb/parser/parsed_data/create_schema_info.hpp"
#include "duckdb/planner/operator/logical_create_table.hpp"
#include "duckdb/planner/operator/logical_insert.hpp"
#include <memory>

namespace duckdb {
namespace adbc {

unique_ptr<AdbcPooledConnection> AdbcCatalog::GetPooledConnection() {
    return pool->GetConnection();
}

string AdbcCatalog::FetchCatalogName() {

    // The catalog name has unbounded length
    // Fast path: try fititng the catalog name in a stack buffer
    char stack_buffer[4096] = {'\0'};
    size_t length = sizeof(stack_buffer);

    // Use GetOption(...) to fetch the catalog name
    Handle<Private::AdbcError> error = {};
    auto connection = pool->GetConnection();

    // Exit if there's no support for this option
    auto option_status = AdbcConnectionGetOption(connection->GetRawConnection(),
                                                 ADBC_CONNECTION_OPTION_CURRENT_CATALOG,
                                                 stack_buffer,
                                                 &length,
                                                 error.get());

    // If the option is not supported then exit here
    if (option_status == ADBC_STATUS_NOT_FOUND) {
        return "";
    }

    // Check that the call succeeded
    CHECK_ADBC(option_status, IOException);

    // Exit if the fast path succeeded
    if (length <= sizeof(stack_buffer)) {
        return stack_buffer;
    }

    // Otherwise we are in the slow path and need to heap allocate a buffer for the catalog name
    auto heap_buffer = std::make_unique<char[]>(length);

    // Fetch the catalog name again and return
    CHECK_ADBC(AdbcConnectionGetOption(connection->GetRawConnection(),
                                       ADBC_CONNECTION_OPTION_CURRENT_CATALOG,
                                       heap_buffer.get(),
                                       &length,
                                       error.get()),
               BinderException);
    return string(heap_buffer.get());
}

vector<string> AdbcCatalog::FetchTableNames(const string &schema_name) {

    // Collect all table names
    vector<string> table_names;
    auto internal_schema = GetInternalSchemaName(schema_name);
    ForEachCatalog(internal_schema.c_str(), ADBC_OBJECT_DEPTH_TABLES, [&table_names](ArrowArray *batch) {
        // Get the catalogs
        auto *catalogs = batch;
        auto *catalog_schemas_list = batch->children[1];
        for (int64_t i = 0; i < catalogs->length; ++i) {
            auto *schema_offsets = reinterpret_cast<const int32_t *>(catalog_schemas_list->buffers[1]);
            auto schema_start = schema_offsets[i];
            auto schema_end = schema_offsets[i + 1];

            auto *schemas_struct = catalog_schemas_list->children[0];

            // Get the schemas for this catalog
            for (int32_t j = schema_start; j < schema_end; ++j) {
                auto *tables_list = schemas_struct->children[1];
                auto *table_offsets = reinterpret_cast<const int32_t *>(tables_list->buffers[1]);
                auto table_start = table_offsets[j];
                auto table_end = table_offsets[j + 1];

                auto *table_struct = tables_list->children[0];
                auto *table_names_array = table_struct->children[0];

                auto *name_offsets = reinterpret_cast<const int32_t *>(table_names_array->buffers[1]);
                auto *name_data = reinterpret_cast<const char *>(table_names_array->buffers[2]);

                // Get the tables for each schema
                for (int32_t k = table_start; k < table_end; ++k) {
                    auto start = name_offsets[k];
                    auto end = name_offsets[k + 1];
                    table_names.emplace_back(name_data + start, end - start);
                }
            }
        }
        return true;
    });

    return table_names;
}

void AdbcCatalog::ClearCache() {
    // Delete all schemas
    std::unique_lock<std::mutex> schemas_lock(schemas_mutex);
    owned_schemas.clear();
    cached_schema_names.clear();
    schema_names_loaded = false;
}

void AdbcCatalog::ScanSchemas(ClientContext &context, std::function<void(SchemaCatalogEntry &)> callback) {
    // For each schema, create a catalog entry and execute the callback
    for (auto &schema_name : GetCachedSchemaNames()) {
        if (auto *entry = GetCatalogEntry(GetInternalSchemaName(schema_name))) {
            callback(*entry);
        } else {
            callback(*CreateCatalogEntry(GetInternalSchemaName(schema_name)));
        }
    }
}

optional_ptr<SchemaCatalogEntry> AdbcCatalog::LookupSchema(CatalogTransaction transaction,
                                                           const EntryLookupInfo &schema_lookup,
                                                           OnEntryNotFound if_not_found) {

    // Return the entry if it already exists
    auto internal_name = GetInternalSchemaName(schema_lookup.GetEntryName());
    if (auto *entry = GetCatalogEntry(internal_name)) {
        return entry;
    }

    auto &names = GetCachedSchemaNames();
    bool found = (std::find(names.begin(), names.end(), internal_name) != names.end());

    // Throw an exception if the schema doesn't exist
    if (!found) {
        if (if_not_found == OnEntryNotFound::RETURN_NULL) {
            return nullptr;
        }
        throw IOException("Unable to find schema with name: \"%s\"", schema_lookup.GetEntryName());
    }

    // Otherwise add it to the catalog and return it
    return CreateCatalogEntry(internal_name);
}

PhysicalOperator &AdbcCatalog::PlanCreateTableAs(ClientContext &context,
                                                 PhysicalPlanGenerator &planner,
                                                 LogicalCreateTable &op,
                                                 PhysicalOperator &plan) {

    // Ensure no IF NOT EXISTS or REPLACE qualifiers are included in the CTAS
    auto &info = op.info;
    if (info->Base().on_conflict != OnCreateConflict::ERROR_ON_CONFLICT) {
        throw BinderException("CREATE TABLE commands not yet supported with IF NOT "
                              "EXISTS or REPLACE with the ADBC extension");
    }

    // Check whether we can mix ADBC reads and writes
    Value option_value;
    context.TryGetCurrentSetting("adbc_mix_reads_writes", option_value);
    bool can_mix_reads_writes = option_value.GetValue<bool>();

    // If there are ADBC reads and we cannot mix reads and writes
    if (ContainsAdbcReads(plan) && !can_mix_reads_writes) {
        throw NotImplementedException(
            "This CREATE TABLE AS (SELECT ...) statement mixes ADBC reads and writes, which may cause "
            "concurrency issues "
            "depending on the underlying DBMS's transaction isolation level.\n"
            "If the reads and writes target different DBMSs, this is likely "
            "safe; "
            "if they target the same DBMS, consistency depends on that "
            "DBMS's isolation guarantees.\n"
            "If you believe this CREATE TABLE is safe, turn off this check by running "
            "\"SET adbc_mix_reads_writes = true;\"");
    }

    // Collect column names & types
    vector<LogicalType> column_types;
    vector<string> column_names;
    for (auto &col : info->Base().columns.Logical()) {
        column_types.push_back(col.GetType());
        column_names.push_back(col.GetName());
    }
    auto table_name = info->Base().table;
    auto internal_schema = GetInternalSchemaName(info->Base().schema);
    auto &insert =
        planner.Make<AdbcInsert>(op, column_types, column_names, table_name, internal_schema, pool, InsertMode::CTAS);
    insert.children.push_back(plan);
    auto &insert_node = insert.Cast<AdbcInsert>();

    // Reset the schema
    GetCatalogEntry(internal_schema)->Cast<AdbcSchemaEntry>().Reset();
    return insert;
}

PhysicalOperator &AdbcCatalog::PlanInsert(ClientContext &context,
                                          PhysicalPlanGenerator &planner,
                                          LogicalInsert &op,
                                          optional_ptr<PhysicalOperator> plan) {

    // Ensure no RETURNING clause or ON CONFLICT
    if (op.return_chunk) {
        throw BinderException("RETURNING clause not yet supported for INSERTs into an ADBC table");
    }

    if (op.on_conflict_info.action_type != OnConflictAction::THROW) {
        throw BinderException("ON CONFLICT clause not yet supported for "
                              "INSERTs into ADBC table");
    }

    D_ASSERT(plan);

    // Check whether we can mix ADBC reads and writes
    Value option_value;
    context.TryGetCurrentSetting("adbc_mix_reads_writes", option_value);
    bool can_mix_reads_writes = option_value.GetValue<bool>();

    // If there are ADBC reads and we cannot mix reads and writes
    if (ContainsAdbcReads(*plan) && !can_mix_reads_writes) {
        throw NotImplementedException("This INSERT statement mixes ADBC reads and writes, which may cause "
                                      "concurrency issues "
                                      "depending on the underlying DBMS's transaction isolation level.\n"
                                      "If the reads and writes target different DBMSs, this is likely "
                                      "safe; "
                                      "if they target the same DBMS, consistency depends on that "
                                      "DBMS's isolation guarantees.\n"
                                      "If you believe this INSERT is safe, turn off this check by running "
                                      "\"SET adbc_mix_reads_writes = true;\"");
    }

    // Collect column names & types
    vector<LogicalType> column_types;
    vector<string> column_names;
    auto &table = op.table;
    auto &columns = table.GetColumns();

    // Remap columns as required
    if (!op.column_index_map.empty()) {
        idx_t column_count = 0;
        vector<PhysicalIndex> column_indexes;
        column_indexes.resize(columns.LogicalColumnCount(), PhysicalIndex(DConstants::INVALID_INDEX));
        for (idx_t c = 0; c < op.column_index_map.size(); c++) {
            auto column_index = PhysicalIndex(c);
            auto mapped_index = op.column_index_map[column_index];
            if (mapped_index == DConstants::INVALID_INDEX) {
                continue;
            }
            column_indexes[mapped_index] = column_index;
            column_count++;
        }
        for (idx_t c = 0; c < column_count; c++) {
            auto &col = columns.GetColumn(column_indexes[c]);
            column_names.push_back(col.GetName());
            column_types.push_back(col.GetType());
        }
    } else {
        for (auto &col : columns.Logical()) {
            column_types.push_back(col.GetType());
            column_names.push_back(col.GetName());
        }
    }

    auto table_name = table.name;
    auto internal_schema = GetInternalSchemaName(table.schema.name);
    auto &insert =
        planner.Make<AdbcInsert>(op, column_types, column_names, table_name, internal_schema, pool, InsertMode::APPEND);
    insert.children.push_back(*plan);
    return insert;
}

void AdbcCatalog::ForEachCatalog(const char *schema_name,
                                 int depth,
                                 const std::function<bool(ArrowArray *)> &callback) {

    // Retrieve the catalog info from the ADBC connection
    auto connection = pool->GetConnection();
    Handle<Private::AdbcError> error = {};
    Handle<ArrowArrayStream> stream = {};
    CHECK_ADBC(AdbcConnectionGetObjects(connection->GetRawConnection(),
                                        depth,
                                        catalog_name.empty() ? nullptr : catalog_name.c_str(),
                                        schema_name,
                                        nullptr,
                                        nullptr,
                                        nullptr,
                                        stream.get(),
                                        error.get()),
               IOException);

    while (true) {
        Handle<ArrowArray> batch = {};
        if (stream->get_next(stream.get(), batch.get()) != 0 || batch->release == nullptr) {
            break;
        }
        // Execute the callback on the batch
        if (!callback(batch.get())) {
            return;
        }
    }
}

vector<string> AdbcCatalog::FetchSchemaNames() {
    // Collect all schema names from the result
    vector<string> schema_names;
    ForEachCatalog(nullptr, ADBC_OBJECT_DEPTH_DB_SCHEMAS, [&schema_names](ArrowArray *batch) {
        auto *catalog_schemas_list = batch->children[1];
        for (int64_t i = 0; i < batch->length; ++i) {
            auto *schema_offsets = reinterpret_cast<const int32_t *>(catalog_schemas_list->buffers[1]);
            auto start_idx = schema_offsets[i];
            auto end_idx = schema_offsets[i + 1];

            auto *schemas_struct = catalog_schemas_list->children[0];
            auto *name_array = schemas_struct->children[0];
            auto *name_offsets = reinterpret_cast<const int32_t *>(name_array->buffers[1]);
            auto *name_data = reinterpret_cast<const char *>(name_array->buffers[2]);

            for (int32_t j = start_idx; j < end_idx; ++j) {
                auto name_start = name_offsets[j];
                auto name_end = name_offsets[j + 1];
                schema_names.emplace_back(name_data + name_start, name_end - name_start);
            }
        }
        return true;
    });
    return schema_names;
}

const vector<string> &AdbcCatalog::GetCachedSchemaNames() {
    // Lazy load schema names
    std::unique_lock<std::mutex> schemas_lock(schemas_mutex);
    if (!schema_names_loaded) {
        cached_schema_names = FetchSchemaNames();
        schema_names_loaded = true;
    }
    return cached_schema_names;
}


SchemaCatalogEntry *AdbcCatalog::GetCatalogEntry(const string &schema_name) {
    // Look up the entry
    std::unique_lock<std::mutex> schemas_lock(schemas_mutex);
    auto it = owned_schemas.find(GetInternalSchemaName(schema_name));
    if (it != owned_schemas.end()) {
        return it->second.get();
    }
    return nullptr;
}

SchemaCatalogEntry *AdbcCatalog::CreateCatalogEntry(const string &schema_name) {
    // Return the entry if it already exists
    std::unique_lock<std::mutex> schemas_lock(schemas_mutex);
    auto internal_schema = GetInternalSchemaName(schema_name);
    auto it = owned_schemas.find(internal_schema);
    if (it != owned_schemas.end()) {
        return it->second.get();
    }

    // Create and insert the entry
    CreateSchemaInfo info;
    info.schema = GetExternalSchemaName(schema_name);
    auto schema_entry = make_uniq<AdbcSchemaEntry>(*this, info);
    auto ptr = schema_entry.get();
    owned_schemas[internal_schema] = std::move(schema_entry);
    return ptr;
}

bool AdbcCatalog::ContainsAdbcReads(PhysicalOperator &op) {

    // If this operator is a read_adbc function
    if (op.type == PhysicalOperatorType::TABLE_SCAN) {
        auto &table_scan = op.Cast<PhysicalTableScan>();
        if (table_scan.function.name == "read_adbc") {
            return true;
        }
    }

    // If any of this operator's children contains a read_adbc function call
    for (auto &child : op.children) {
        if (ContainsAdbcReads(child)) {
            return true;
        }
    }

    // No ADBC reads
    return false;
}

} // namespace adbc
} // namespace duckdb
