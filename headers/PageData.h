//
// Created by Riju Mukherjee on 1/26/26.
//

#ifndef NEBULADB_PAGEDATA_H
#define NEBULADB_PAGEDATA_H

#include <string>
#include <vector>

#include "Column.h"
#include "PageDirectory.h"
#include "constants.h"
#include "PageCache.h"
#include "Schema.h"

namespace StorageEngine {
    class PageData {
    private:
        PageData();
        inline static Schema* schema;
        inline static Utils::Logger* logger;
        PageDirectory* pageDirectory = nullptr;
        PageCache* pg_cache;
        void BIG_SWITCH(char* buffer_ptr, std::vector<Column>& cols) {
            for (Column& col : cols){
                DataType dt = col.col_type;
                uint16_t data_size = 0;
                switch(dt)
                {
                    case DataType::BIG_INT:
                        data_size = sizeof(uint64_t);
                        long long lValue;
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
            const std::type_info& columnType = column.col_value.type();
            if (columnType == typeid(bool))
                total_bytes += sizeof(bool);
            else if (columnType == typeid(char))
                total_bytes += sizeof(char);
            else if (columnType == typeid(short))
                total_bytes += sizeof(short);
            else if (columnType == typeid(int))
                total_bytes += sizeof(int);
            else if (columnType == typeid(long long))
                total_bytes += sizeof(long long);
            else if (columnType == typeid(float))
                total_bytes += sizeof(float);
            else if (columnType == typeid(double))
                total_bytes += sizeof(double);
            else if (columnType == typeid(std::string))
                total_bytes += strlen(std::any_cast<std::string>(column.col_value).c_str()) + 2; //2 bytes added to store length of string
        }
        bool isColumnTypeMatching(const Column& column, const DataType& colType) const {
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
        }
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

    public:
        PageData(Schema* schema);
        Column singleData(PAGE_ID_TYPE page_id, SLOT_ID_TYPE slot_id);
        std::unique_ptr<std::vector<Column>> singleRowData(PAGE_ID_TYPE page_id, SLOT_ID_TYPE slot_id);
        std::unique_ptr<std::vector<std::unique_ptr<std::vector<Column>>>> multiRowData(std::vector<std::pair<PAGE_ID_TYPE,SLOT_ID_TYPE>>&);
        std::unique_ptr<std::vector<std::pair<Column,SLOT_ID_TYPE>>> columnData(PAGE_ID_TYPE page_id, std::string& col_name);
        std::unique_ptr<std::vector<std::pair<Column,SLOT_ID_TYPE>>> multiColumnData(PAGE_ID_TYPE page_id, std::vector<std::string&>& col_name);
        std::unique_ptr<std::vector<std::vector<Column>>> allRowsData(PAGE_ID_TYPE page_id);
        void delRows(std::vector<std::pair<PAGE_ID_TYPE,SLOT_ID_TYPE>>&);
        void insertIntoTable(std::vector<Column>& cols, std::pair<PAGE_ID_TYPE,SLOT_ID_TYPE>& pg_slot) const;
        void fullTableScan();
    };
}


#endif //NEBULADB_PAGEDATA_H