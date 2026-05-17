#pragma once

#include <climits>
#include <cstdint>
#include <vector>

#include "Column.h"
#include "Executor.h"
#include "Logger.h"
#include "PageCache.h"
#include "PageDirectory.h"
#include "Schema.h"
#include "InternalStructs.h"


namespace StorageEngine {
    class PageOps{
        private:
            PageOps();
            Schema* tSchema;
            Utils::Logger* logger;
            StorageEngine::PageDirectory* pageDir = nullptr;
            StorageEngine::PageCache* pg_cache = nullptr;
            QueryEngine::IndexTableType * idx_tbl = nullptr; 
            void BIG_SWITCH(char* buffer_ptr, std::vector<Column>& cols) {
            for (Column& col : cols){
                DataType dt = col.col_type;
                uint16_t data_size = 0;
                switch(dt)
                {
                    case DataType::BIG_INT:
                        data_size = sizeof(uint64_t);
                        uint64_t lValue;
                        std::memcpy(&lValue, buffer_ptr, data_size);
                        col.col_value = lValue;
                        buffer_ptr += data_size;
                        break;
                    case DataType::INT:
                        data_size = sizeof(int);
                        int iValue;
                        std::memcpy(&iValue, buffer_ptr, data_size);
                        col.col_value = iValue;
                        buffer_ptr += data_size;
                        break;
                    case DataType::SHORT:
                        data_size = sizeof(short);
                        short sValue;
                        std::memcpy(&sValue, buffer_ptr, data_size);
                        col.col_value = sValue;
                        buffer_ptr += data_size;
                        break;
                    case DataType::CHAR:
                        data_size = sizeof(char);
                        char cValue;
                        std::memcpy(&cValue, buffer_ptr, data_size);
                        col.col_value = cValue;
                        buffer_ptr += data_size;
                        break;
                    case DataType::FLOAT:
                        data_size = sizeof(float);
                        float fValue;
                        std::memcpy(&fValue, buffer_ptr, data_size);
                        col.col_value = fValue;
                        buffer_ptr += data_size;
                        break;
                    case DataType::DOUBLE:
                        data_size = sizeof(double);
                        double dValue;
                        std::memcpy(&dValue, buffer_ptr, data_size);
                        col.col_value = dValue;
                        buffer_ptr += data_size;
                        break;
                    case DataType::BOOLEAN:
                        data_size = sizeof(bool);
                        bool bValue;
                        std::memcpy(&bValue, buffer_ptr, data_size);
                        col.col_value = bValue;
                        buffer_ptr += data_size;
                        break;
                    case DataType::STRING:{
                        int str_size;
                        std::memcpy(&str_size, buffer_ptr, sizeof(uint16_t));
                        buffer_ptr += sizeof(uint16_t);
                        data_size = str_size;
                        std::string strValue(buffer_ptr, data_size);
                        col.col_value = strValue;
                        buffer_ptr += data_size;
                        break;
                    }
                    default:
                        throw std::runtime_error("Invalid DataType");
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
                    total_bytes += strlen(std::any_cast<std::string>(column.col_value).c_str()) + 2; //2 bytes added to store length of string
            }
            /*bool isColumnTypeMatching(const Column& column, const DataType& colType) const {
                if (!column.col_value.has_value())
                {
                    logger->logError({"Column has no value"});
                    return false;
                }
                const std::type_info& columnType = column.col_value.type();
                if (columnType == typeid(bool) && colType != DataType::BOOLEAN)
                    return false;
                if (columnType == typeid(char) && colType != DataType::CHAR)
                    return false;
                if (columnType == typeid(short) && colType != DataType::SHORT)
                    return false;
                if (columnType == typeid(int) && colType != DataType::INT)
                    return false;
                if (columnType == typeid(long long) && colType != DataType::BIG_INT)
                    return false;
                if (columnType == typeid(float) && colType != DataType::FLOAT)
                    return false;
                if (columnType == typeid(double) && colType != DataType::DOUBLE)
                    return false;
                if (columnType == typeid(std::string) && colType != DataType::STRING)
                    return false;
                return true;
            }*/
            void valueToBuffer(const Column& column, std::vector<char>* buffer, uint16_t& remainingSize) const
            {
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
                            const auto value = std::any_cast<bool>(column.col_value);
                            appendToBuffer(&value, sizeof(bool));
                            break;
                        }
                        case DataType::CHAR: {
                            const auto value = std::any_cast<char>(column.col_value);
                            appendToBuffer(&value, sizeof(char));
                            break;
                        }
                        case DataType::SHORT: {
                            const auto value = std::any_cast<short>(column.col_value);
                            appendToBuffer(&value, sizeof(short));
                            break;
                        }
                        case DataType::INT: {
                            const auto value = std::any_cast<int>(column.col_value);
                            appendToBuffer(&value, sizeof(int));
                            break;
                        }
                        case DataType::FLOAT: {
                            const auto value = std::any_cast<float>(column.col_value);
                            appendToBuffer(&value, sizeof(float));
                            break;
                        }
                        case DataType::BIG_INT: {
                            const auto value = std::any_cast<uint64_t>(column.col_value);
                            appendToBuffer(&value, sizeof(uint64_t));
                            break;
                        }
                        case DataType::DOUBLE: {
                            const auto value = std::any_cast<double>(column.col_value);
                            appendToBuffer(&value, sizeof(double));
                            break;
                        }
                        case DataType::STRING: {
                            const auto value = std::any_cast<std::string>(column.col_value);
                            const size_t str_size = value.length();
                            appendToBuffer(&str_size, 2);
                            appendToBuffer(value.c_str(), value.size());
                            break;
                        }
                        default:
                            throw std::invalid_argument("Unsupported DataType!");
                    }
                } 
                catch (const std::bad_any_cast& e) {
                    logger->logError({"Bad any_cast: ", e.what()});
                } 
                catch (const std::exception& e) {
                    logger->logError({"Unhandled exception: ", e.what()});
                }
            }
            bool dataComp(QueryEngine::ExecCondition& cond, Column& col)
            {
                if(cond.op == ExecConds::EQUAL && cond.fil_type == cond.SINGLE_VALUE)
                {
                    if (cond.value.index() != col.col_value.index())
                        return false;
                    return col.col_value == cond.value ;
                }
                if(cond.op == ExecConds::GREATER && cond.fil_type == cond.SINGLE_VALUE)
                {
                    if (cond.value.index() != col.col_value.index())
                        return false;
                    return col.col_value > cond.value;
                }
                if(cond.op == ExecConds::LESS && cond.fil_type == cond.SINGLE_VALUE)
                {
                    if (cond.value.index() != col.col_value.index())
                        return false;
                    return col.col_value < cond.value;
                }
                if(cond.op == ExecConds::GREATER_EQUAL && cond.fil_type == cond.SINGLE_VALUE)
                {
                    if (cond.value.index() != col.col_value.index())
                        return false;
                    return col.col_value >= cond.value;
                }
                if(cond.op == ExecConds::LESS_EQUAL && cond.fil_type == cond.SINGLE_VALUE)
                {
                    if (cond.value.index() != col.col_value.index())
                        return false;
                    return col.col_value <= cond.value;
                }
                if(cond.op == ExecConds::IN_BETWEEN && cond.fil_type == cond.RANGE_VALUE)
                {
                    if (cond.value.index() != col.col_value.index())
                        return false;
                    return ((col.col_value >= cond.range.first) && (col.col_value <= cond.range.second));
                }
                return false;
            }
            void filterPage(PAGE_ID_TYPE& pg_id, std::vector<ROW_ID>* v, Filter& filter)
            {
                std::shared_ptr<Page> page = pg_cache->getPageFromCache(pg_id);
                std::vector<std::unique_ptr<char[]>> raw_rows; 
                std::vector<SLOT_ID_TYPE> all_slots;
                page->getAllRowsFromPage(&raw_rows,&all_slots);
                pg_cache->unPinPage(pg_id);
                std::vector<Column> cols = tSchema->getColumns();
                for(auto& row_data_raw : raw_rows)
                {
                    static int i = 0;
                    char* buffer_ptr = row_data_raw.get();
                    BIG_SWITCH(buffer_ptr,cols);
                    for(auto cond : filter.col_filter)
                    {
                        ROW_ID row_id{pg_id,all_slots[i]};
                        if(dataComp(cond, cols[cond.col_idx-1]))
                        {
                            v->emplace_back(row_id);
                        }
                    }
                }
            }
            
            /*
             * The Search ranges are not correct they should be made wrt the data type
             * for now its ok.
             */
            std::pair<variant_data_t,variant_data_t> getSearchRangeFromCond(QueryEngine::ExecCondition& cond)
            {
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

        public:
            PageOps(Schema* schema,PageDirectory* pg_dir, QueryEngine::IndexTableType* it);
            /* call this if u have indexed column */ 
            QueryEngine::ExecResults IndexTableScan(Filter& filter);
            /* call this for a full table scan */
            QueryEngine::ExecResults FullTableScan(Filter& filter);
            /* call this to project the rows into columns*/
            QueryEngine::ExecResults ProjectOnRows(QueryEngine::ExecResults& result,std::vector<COL_ID_TYPE>& proj_cols);
            void                     DeleteRows(QueryEngine::ExecResults& result);
            void                     UpdateRows(QueryEngine::ExecResults& result, std::unordered_map<COL_ID_TYPE, variant_data_t>& updates);
            ROW_ID                   InsertRow(std::vector<variant_data_t>& values);
            /*
                ... will define the others later.
            */
    };
}
