#pragma once

#include <climits>
#include <cstdint>
#include <memory>
#include <spdlog/spdlog.h>
#include <string>
#include <vector>

#include "Column.h"
#include "Executor.h"
#include "Logger.h"
#include "PageCache.h"
#include "PageDirectory.h"
#include "Schema.h"
#include "constants.h"
#include "InternalStructs.h"


namespace StorageEngine {
    using IndexTableType = std::unordered_map<uint16_t, std::unique_ptr<StorageEngine::Indexer<variant_data_t,std::pair<PAGE_ID_TYPE,SLOT_ID_TYPE>>>>;
    class PageOps{
        private:
            PageOps();
            std::filesystem::path db_path;
            std::filesystem::path table_path;
            Schema* tSchema;
            Utils::Logger* logger;
            StorageEngine::PageDirectory* pageDir = nullptr;
            StorageEngine::PageCache* pg_cache = nullptr;
            IndexTableType * idx_tbl = nullptr;

            size_t minBytesForColumns(const std::vector<Column>& cols, const size_t start_idx, const size_t string_header_size) const {
                size_t total = 0;
                for (size_t i = start_idx; i < cols.size(); i++) {
                    switch(cols[i].col_type)
                    {
                        case DataType::BOOLEAN:
                            total += sizeof(bool);
                            break;
                        case DataType::CHAR:
                            total += sizeof(char);
                            break;
                        case DataType::SHORT:
                            total += sizeof(short);
                            break;
                        case DataType::INT:
                            total += sizeof(int);
                            break;
                        case DataType::BIG_INT:
                            total += sizeof(uint64_t);
                            break;
                        case DataType::FLOAT:
                            total += sizeof(float);
                            break;
                        case DataType::DOUBLE:
                            total += sizeof(double);
                            break;
                        case DataType::STRING:
                            total += string_header_size;
                            break;
                        case DataType::INVALID:
                        default:
                            break;
                    }
                }
                return total;
            }

            void ensureCanRead(char* buffer_ptr, char* row_end, const size_t data_size) const {
                if (buffer_ptr > row_end || static_cast<size_t>(row_end - buffer_ptr) < data_size) {
                    throw std::runtime_error("Row data is shorter than schema expects");
                }
            }

            void bufferToValue(char* buffer_ptr, const uint16_t row_size, std::vector<Column>& cols) {
                char* row_end = buffer_ptr + row_size;
                for (size_t col_idx = 0; col_idx < cols.size(); col_idx++){
                    Column& col = cols[col_idx];
                    DataType dt = col.col_type;
                    switch(dt)
                    {
                        case DataType::CHAR: {
                            ensureCanRead(buffer_ptr, row_end, sizeof(char));
                            char cValue;
                            std::memcpy(&cValue, buffer_ptr, sizeof(char));
                            col.col_value = cValue;
                            buffer_ptr += sizeof(char);
                            break;
                        }
                        case DataType::BOOLEAN: {
                            ensureCanRead(buffer_ptr, row_end, sizeof(bool));
                            bool bValue;
                            std::memcpy(&bValue, buffer_ptr, sizeof(bool));
                            col.col_value = bValue;
                            buffer_ptr += sizeof(bool);
                            break;
                        }
                        case DataType::SHORT: {
                            ensureCanRead(buffer_ptr, row_end, sizeof(short));
                            short sValue;
                            std::memcpy(&sValue, buffer_ptr, sizeof(short));
                            col.col_value = sValue;
                            buffer_ptr += sizeof(short);
                            break;
                        }
                        case DataType::INT: {
                            ensureCanRead(buffer_ptr, row_end, sizeof(int));
                            int iValue;
                            std::memcpy(&iValue, buffer_ptr, sizeof(int));
                            col.col_value = iValue;
                            buffer_ptr += sizeof(int);
                            break;
                        }
                        case DataType::BIG_INT: {
                            ensureCanRead(buffer_ptr, row_end, sizeof(uint64_t));
                            uint64_t lValue;
                            std::memcpy(&lValue, buffer_ptr, sizeof(uint64_t));
                            col.col_value = lValue;
                            buffer_ptr += sizeof(uint64_t);
                            break;
                        }
                        case DataType::DOUBLE: {
                            ensureCanRead(buffer_ptr, row_end, sizeof(double));
                            double dValue;
                            std::memcpy(&dValue, buffer_ptr, sizeof(double));
                            col.col_value = dValue;
                            buffer_ptr += sizeof(double);
                            break;
                        }
                        case DataType::FLOAT: {
                            ensureCanRead(buffer_ptr, row_end, sizeof(float));
                            float fValue;
                            std::memcpy(&fValue, buffer_ptr, sizeof(float));
                            col.col_value = fValue;
                            buffer_ptr += sizeof(float);
                            break;
                        }
                        case DataType::STRING: {
                            const size_t remaining = static_cast<size_t>(row_end - buffer_ptr);
                            ensureCanRead(buffer_ptr, row_end, sizeof(uint16_t));

                            uint32_t modern_size = 0;
                            bool use_modern_size = false;
                            if (remaining >= sizeof(uint32_t)) {
                                std::memcpy(&modern_size, buffer_ptr, sizeof(uint32_t));
                                const size_t min_remaining = minBytesForColumns(cols, col_idx + 1, sizeof(uint32_t));
                                use_modern_size = modern_size <= remaining - sizeof(uint32_t)
                                    && remaining - sizeof(uint32_t) - modern_size >= min_remaining;
                            }

                            size_t str_size = modern_size;
                            size_t header_size = sizeof(uint32_t);
                            if (!use_modern_size) {
                                uint16_t legacy_size = 0;
                                std::memcpy(&legacy_size, buffer_ptr, sizeof(uint16_t));
                                const size_t min_remaining = minBytesForColumns(cols, col_idx + 1, sizeof(uint16_t));
                                if (legacy_size > remaining - sizeof(uint16_t)
                                    || remaining - sizeof(uint16_t) - legacy_size < min_remaining) {
                                    throw std::runtime_error("Invalid string length in row data");
                                }
                                str_size = legacy_size;
                                header_size = sizeof(uint16_t);
                            }

                            buffer_ptr += header_size;
                            ensureCanRead(buffer_ptr, row_end, str_size);
                            std::string str_data(str_size, '\0');
                            std::memcpy(str_data.data(), buffer_ptr, str_size);
                            col.col_value = str_data;
                            buffer_ptr += str_size;
                            break;
                        }
                        case DataType::INVALID:
                            throw std::runtime_error("Invalid data type");
                        default:
                            throw std::runtime_error("Unsupported data type");
                    }
                }
            }
            void addTotalBytes(const Column& column, uint16_t& total_bytes) const {
                if (column.col_type == DataType::BOOLEAN)
                    total_bytes += sizeof(bool);
                else if (column.col_type == DataType::CHAR)
                    total_bytes += sizeof(char);
                else if (column.col_type == DataType::SHORT)
                    total_bytes += sizeof(short);
                else if (column.col_type == DataType::INT)
                    total_bytes += sizeof(int);
                else if (column.col_type == DataType::BIG_INT)
                    total_bytes += sizeof(uint64_t);
                else if (column.col_type == DataType::FLOAT)
                    total_bytes += sizeof(float);
                else if (column.col_type == DataType::DOUBLE)
                    total_bytes += sizeof(double);
                else if (column.col_type == DataType::STRING)
                    total_bytes += std::get<std::string>(column.col_value).size() + sizeof(uint32_t);
            }
            void getAllDataRowsFromPage(PAGE_ID_TYPE& pg_id, std::vector<std::unique_ptr<char[]>>& raw_rows, std::vector<SLOT_ID_TYPE>& all_slots, std::unique_ptr<std::vector<ROW>>& result) {
                std::vector<uint16_t> row_lengths;
                std::shared_ptr<Page> page = pg_cache->getPageFromCache(pg_id);
                if(page)
                {
                    page->getAllRowsFromPage(&raw_rows, &all_slots);
                    for (const auto& slot : all_slots) {
                        row_lengths.push_back(page->getRowLength(slot));
                    }
                    pg_cache->unPinPage(pg_id);
                }
                for(size_t i = 0; i < raw_rows.size(); i++)
                {
                    std::vector<Column> cols = tSchema->getColumns();
                    try {
                        bufferToValue(raw_rows[i].get(), row_lengths[i], cols);
                        result->emplace_back(cols);
                    }
                    catch (const std::exception& e) {
                        logger->logError({"Skipping unreadable row: ", e.what()});
                    }
                }
            }
            void getAllDataOfColumnFromPage(PAGE_ID_TYPE& pg_id, COL_ID_TYPE col_id, std::unique_ptr<std::vector<std::pair<Column, ROW_ID>>>& all_col_data) {
                std::vector<std::unique_ptr<char[]>> raw_rows;
                std::vector<SLOT_ID_TYPE> all_slots;
                std::unique_ptr<std::vector<ROW>> result = std::make_unique<std::vector<ROW>>();
                getAllDataRowsFromPage(pg_id, raw_rows, all_slots, result);
                for(size_t i = 0; i < result->size(); i++)
                {
                    ROW_ID row_id = {pg_id, all_slots[i]};
                    all_col_data->emplace_back(std::make_pair(result->at(i)[col_id-1], row_id));
                }
            }
            void addToIndexTable(Column& col) {
                auto idx = std::make_unique<StorageEngine::Indexer<variant_data_t,std::pair<PAGE_ID_TYPE,SLOT_ID_TYPE>>>(db_path, table_path, col.col_name);
                std::unique_ptr<std::vector<std::pair<Column, ROW_ID>>> all_col_data = std::make_unique<std::vector<std::pair<Column, ROW_ID>>>();
                for(uint64_t i = 0; i <= pageDir->getCurrentLogicalPage(); i++)
                {
                    getAllDataOfColumnFromPage(i, col.col_id, all_col_data);
                }
                idx->createIndex(std::move(all_col_data));
                idx_tbl->insert({col.col_id,std::move(idx)});
                logger->logInfo({"Index created successfully"});
            }

            void valueToBuffer(const Column& column, std::vector<char>* buffer, uint16_t& remainingSize) const {
                try {
                    auto appendToBuffer = [&](const void* data, const size_t size){
                        if (remainingSize >= size) {
                            buffer->insert(buffer->end(), static_cast<const char*>(data), static_cast<const char*>(data) + size);
                            remainingSize -= size;
                        } else {
                            throw std::runtime_error("Insufficient buffer space!");
                        }
                    };

                    switch (column.col_type) {
                        case DataType::BOOLEAN: {
                            const auto value = std::get<bool>(column.col_value);
                            appendToBuffer(&value, sizeof(bool));
                            break;
                        }
                        case DataType::CHAR: {
                            const auto value = std::get<char>(column.col_value);
                            appendToBuffer(&value, sizeof(char));
                            break;
                        }
                        case DataType::SHORT: {
                            const auto value = std::get<short>(column.col_value);
                            appendToBuffer(&value, sizeof(short));
                            break;
                        }
                        case DataType::INT: {
                            const auto value = std::get<int>(column.col_value);
                            appendToBuffer(&value, sizeof(int));
                            break;
                        }
                        case DataType::FLOAT: {
                            const auto value = std::get<float>(column.col_value);
                            appendToBuffer(&value, sizeof(float));
                            break;
                        }
                        case DataType::BIG_INT: {
                            const auto value = std::get<uint64_t>(column.col_value);
                            appendToBuffer(&value, sizeof(uint64_t));
                            break;
                        }
                        case DataType::DOUBLE: {
                            const auto value = std::get<double>(column.col_value);
                            appendToBuffer(&value, sizeof(double));
                            break;
                        }
                        case DataType::STRING: {
                            const auto value = std::get<std::string>(column.col_value);
                            const uint32_t str_size = static_cast<uint32_t>(value.length());
                            appendToBuffer(&str_size, sizeof(str_size));
                            appendToBuffer(value.c_str(), value.size());
                            break;
                        }
                        default:
                            throw std::invalid_argument("Unsupported DataType!");
                    }
                }
                catch (const std::bad_variant_access& e) {
                    logger->logError({"Bad variant access: ", e.what()});
                }
                catch (const std::exception& e) {
                    logger->logError({"Unhandled exception: ", e.what()});
                }
            }
            bool isColumnTypeMatching(const Column& column, const DataType& colType) const {
                switch (colType) {
                    case DataType::BOOLEAN:
                        return std::holds_alternative<bool>(column.col_value);
                    case DataType::CHAR:
                        return std::holds_alternative<char>(column.col_value);
                    case DataType::SHORT:
                        return std::holds_alternative<short>(column.col_value);
                    case DataType::INT:
                        return std::holds_alternative<int>(column.col_value);
                    case DataType::BIG_INT:
                        return std::holds_alternative<uint64_t>(column.col_value);
                    case DataType::FLOAT:
                        return std::holds_alternative<float>(column.col_value);
                    case DataType::DOUBLE:
                        return std::holds_alternative<double>(column.col_value);
                    case DataType::STRING:
                        return std::holds_alternative<std::string>(column.col_value);
                    default:
                        return false;
                }
            }
            bool dataComp(QueryEngine::ExecCondition& cond, Column& col) {
                if(cond.op == ExecConds::EQUAL && cond.fil_type == cond.SINGLE_VALUE)
                {
                    /* This compares two values of the same type */
                    if (cond.value.index() != col.col_value.index())
                        return false;
                    return col.col_value == cond.value ;
                }
                if(cond.op == ExecConds::GREATER && cond.fil_type == cond.SINGLE_VALUE)
                {
                    /* This compares two values of the same type */
                    if (cond.value.index() != col.col_value.index())
                        return false;
                    return col.col_value > cond.value;
                }
                if(cond.op == ExecConds::LESS && cond.fil_type == cond.SINGLE_VALUE)
                {
                    /* This compares two values of the same type */
                    if (cond.value.index() != col.col_value.index())
                        return false;
                    return col.col_value < cond.value;
                }
                if(cond.op == ExecConds::GREATER_EQUAL && cond.fil_type == cond.SINGLE_VALUE)
                {
                    /* This compares two values of the same type */
                    if (cond.value.index() != col.col_value.index())
                        return false;
                    return col.col_value >= cond.value;
                }
                if(cond.op == ExecConds::LESS_EQUAL && cond.fil_type == cond.SINGLE_VALUE)
                {
                    /* This compares two values of the same type */
                    if (cond.value.index() != col.col_value.index())
                        return false;
                    return col.col_value <= cond.value;
                }
                if(cond.op == ExecConds::IN_BETWEEN && cond.fil_type == cond.RANGE_VALUE)
                {
                    /* This compares two values of the same type */
                    if (cond.value.index() != col.col_value.index())
                        return false;
                    return ((col.col_value >= cond.range.first) && (col.col_value <= cond.range.second));
                }
                return false;
            }
            void filterPage(PAGE_ID_TYPE& pg_id, std::vector<ROW_ID>* v, Filter& filter) {
                std::vector<std::unique_ptr<char[]>> raw_rows;
                std::vector<SLOT_ID_TYPE> all_slots;
                std::unique_ptr<std::vector<ROW>> all_rows = std::make_unique<std::vector<ROW>>();
                getAllDataRowsFromPage(pg_id, raw_rows, all_slots, all_rows);
                for(size_t i = 0; i < all_rows->size(); i++)
                {
                    if (filter.col_filter.empty())
                    {
                        ROW_ID row_id{pg_id, all_slots[i]};
                        v->emplace_back(row_id);
                        continue;
                    }
                    for(auto cond : filter.col_filter)
                    {
                        if(dataComp(cond, all_rows->at(i)[cond.col_idx-1]))
                        {
                            ROW_ID row_id{pg_id,all_slots[i]};
                            v->emplace_back(row_id);
                        }
                    }
                }
            }

            /*
             * The Search ranges are not correct they should be made wrt the data type
             * for now its ok.
             */
            std::pair<variant_data_t,variant_data_t> getSearchRangeFromCond(QueryEngine::ExecCondition& cond) {
                if(cond.op == ExecConds::EQUAL && cond.fil_type == cond.SINGLE_VALUE)
                {
                    return {cond.value,cond.value} ;
                }
                if(cond.op == ExecConds::GREATER && cond.fil_type == cond.SINGLE_VALUE)
                {
                    return {cond.value,UINT64_MAX} ;
                }
                if(cond.op == ExecConds::LESS && cond.fil_type == cond.SINGLE_VALUE)
                {
                    return {INT_MIN, cond.value};
                }
                if(cond.op == ExecConds::GREATER_EQUAL && cond.fil_type == cond.SINGLE_VALUE)
                {
                    return {cond.value,UINT64_MAX} ;
                }
                if(cond.op == ExecConds::LESS_EQUAL && cond.fil_type == cond.SINGLE_VALUE)
                {
                    return {INT_MIN, cond.value};
                }
                return cond.range;
            }

            StorageEngine::Indexer<variant_data_t,std::pair<PAGE_ID_TYPE,SLOT_ID_TYPE>>* getIndexFromIndexTable(uint16_t col_idx) {
                if (idx_tbl == nullptr) {
                    logger->logWarn({"Index not loaded or dosen't exist"});
                    return nullptr;
                }
                if(idx_tbl->find(col_idx) != idx_tbl->end())
                {
                    return (idx_tbl->at(col_idx)).get();
                }
                return nullptr;
            }

            std::vector<Column> getRowColumns(const ROW_ID& rid) {
                std::shared_ptr<Page> page = pg_cache->getPageFromCache(rid.pg_id);
                const uint16_t row_length = page->getRowLength(rid.slot_id);
                std::unique_ptr<char[]> raw_data = page->getRowFromPage(rid.slot_id);
                pg_cache->unPinPage(rid.pg_id);

                std::vector<Column> cols = tSchema->getColumns();
                bufferToValue(raw_data.get(), row_length, cols);
                return cols;
            }

            void removeIndexedValuesForRow(std::vector<Column>& cols, const ROW_ID& rid) {
                if (idx_tbl == nullptr || idx_tbl->empty()) {
                    return;
                }

                std::vector<ROW_ID> rows{rid};
                for (auto& col : cols) {
                    if (!col.is_indexed) {
                        continue;
                    }
                    auto index = getIndexFromIndexTable(col.col_id);
                    if (index == nullptr) {
                        continue;
                    }
                    const variant_data_t key = col.col_value;
                    index->updateOnDelete(key, key, &rows);
                }
            }

            void removeModifiedIndexedValuesForRow(std::vector<Column>& cols, const ROW_ID& rid, const std::unordered_map<COL_ID_TYPE, variant_data_t>& updates) {
                if (idx_tbl == nullptr || idx_tbl->empty()) {
                    return;
                }

                std::vector<ROW_ID> rows{rid};
                for (auto& col : cols) {
                    if (!col.is_indexed || updates.find(col.col_id) == updates.end()) {
                        continue;
                    }
                    auto index = getIndexFromIndexTable(col.col_id);
                    if (index == nullptr) {
                        continue;
                    }
                    const variant_data_t key = col.col_value;
                    index->updateOnModify(key, key, &rows);
                }
            }

            void insertIndexedValuesForRow(std::vector<Column>& cols, const ROW_ID& rid) {
                if (idx_tbl == nullptr || idx_tbl->empty()) {
                    return;
                }

                std::pair<PAGE_ID_TYPE, SLOT_ID_TYPE> pg_slot{rid.pg_id, rid.slot_id};
                for (auto& col : cols) {
                    if (!col.is_indexed) {
                        continue;
                    }
                    auto index = getIndexFromIndexTable(col.col_id);
                    if (index == nullptr) {
                        continue;
                    }
                    index->updateOnInsert(col, pg_slot);
                }
            }

            void insertModifiedIndexedValuesForRow(std::vector<Column>& cols, const ROW_ID& rid, const std::unordered_map<COL_ID_TYPE, variant_data_t>& updates) {
                if (idx_tbl == nullptr || idx_tbl->empty()) {
                    return;
                }

                std::pair<PAGE_ID_TYPE, SLOT_ID_TYPE> pg_slot{rid.pg_id, rid.slot_id};
                for (auto& col : cols) {
                    if (!col.is_indexed || updates.find(col.col_id) == updates.end()) {
                        continue;
                    }
                    auto index = getIndexFromIndexTable(col.col_id);
                    if (index == nullptr) {
                        continue;
                    }
                    index->updateOnInsert(col, pg_slot);
                }
            }

        public:
            PageOps(Schema* schema,PageDirectory* pg_dir, QueryEngine::IndexTableType* it);
            PageOps(const std::filesystem::path& db_path, const std::filesystem::path& table_path, Schema* schema,PageDirectory* pg_dir, QueryEngine::IndexTableType* it);
            /* call this if u have indexed column */
            QueryEngine::ExecResults IndexTableScan(Filter& filter);
            /* call this for a full table scan */
            QueryEngine::ExecResults FullTableScan(Filter& filter);
            /* call this to project the rows into columns*/
            QueryEngine::ExecResults ProjectOnRows (QueryEngine::ExecResults& result, std::vector<COL_ID_TYPE>& proj_cols);
            void                     DeleteRows    (QueryEngine::ExecResults& result);
            void                     UpdateRows    (QueryEngine::ExecResults& result, std::unordered_map<COL_ID_TYPE, variant_data_t>& updates);
            ROW_ID                   InsertRow     (std::vector<variant_data_t>& values);
            void                     IndexColumn   (COL_ID_TYPE col_id);
    };
}
