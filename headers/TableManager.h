//
// Created by Riju Mukherjee on 08-02-2025.
//

#ifndef TABLEMANAGER_H
#define TABLEMANAGER_H

#include "Schema.h"
#include "Logger.h"
#include "PageCache.h"
#include "PageDirectory.h"

namespace Manager
{
    class TableManager {
    private:
        Schema tSchema;
        Utils::Logger* logger;
        std::filesystem::path currSelectedTablePath;
        std::filesystem::path currSelectedDBPath;
        StorageEngine::PageDirectory* pageDirectory;
        StorageEngine::PageCache* pageCache;
    public:
        TableManager(const std::filesystem::path& currSelectedDBPath, const std::filesystem::path& currSelectedTablePath, const Schema* schema);
        void insertIntoTable(std::vector<Column>& columns) const;
        void selectAllFromTable() const;
        void flushAll() const;

    private:
        void valueToBuffer(const Column& column, std::vector<char>* buffer, uint16_t& remainingSize) const;
        bool isColumnTypeMatching(const Column& column, const DataType& colType) const
        {
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
            if (columnType == typeid(float) && colType != DataType::FLOAT)
                return false;
            if (columnType == typeid(std::string) && colType != DataType::STRING)
                return false;
            return true;
        }

        void addTotalBytes(const Column& column, uint16_t& total_bytes) const
        {
            const std::type_info& columnType = column.col_value.type();
            if (columnType == typeid(bool))
                total_bytes += sizeof(bool);
            else if (columnType == typeid(char))
                total_bytes += sizeof(char);
            else if (columnType == typeid(short))
                total_bytes += sizeof(short);
            else if (columnType == typeid(int))
                total_bytes += sizeof(int);
            else if (columnType == typeid(float))
                total_bytes += sizeof(float);
            else if (columnType == typeid(std::string))
                total_bytes += strlen(std::any_cast<std::string>(column.col_value).c_str()) + 2; //2 bytes added to store length of string
        }


        void printTableData(std::vector<std::unique_ptr<char[]>>* table) const
        {
            const std::filesystem::path file_path = currSelectedTablePath/(tSchema.schema_name+".json");
            json schema_json = tSchema.loadFromFile(file_path);
            logger->logInfo({"Table::",schema_json["table_name"]});
            std::string col_names;
            std::vector<Column> row;
            for (const auto& col : schema_json["columns"])
            {
                Column column;
                column.col_name = col["col_name"];
                column.col_type = col["col_type"];
                column.col_value = 0;
                column.is_primary_key = col["is_primary_key"];
                column.is_null = col["is_null"];
                std::string colName = col["col_name"];
                col_names += colName + "    ";
                row.push_back(column);
            }
            logger->logInfo({col_names});
            for(auto& row_data : *table)
            {
                char* buffer_ptr = row_data.get();
                std::string print_data;
                for(auto col : row)
                {
                    DataType dt = col.col_type;
                    uint16_t data_size = 0;
                    switch(dt)
                    {
                    case DataType::INT:
                        data_size = sizeof(int);
                        int iValue;
                        std::memcpy(&iValue, buffer_ptr, data_size);
                        col.col_value = iValue;
                        print_data += std::to_string(iValue)+"    ";
                        buffer_ptr += data_size;
                        break;
                    case DataType::SHORT:
                        data_size = sizeof(short);
                        short sValue;
                        std::memcpy(&sValue, buffer_ptr, data_size);
                        col.col_value = sValue;
                        print_data += std::to_string(sValue)+"    ";
                        buffer_ptr += data_size;
                        break;
                    case DataType::CHAR:
                        data_size = sizeof(char);
                        char cValue;
                        std::memcpy(&cValue, buffer_ptr, data_size);
                        col.col_value = cValue;
                        print_data += std::to_string(cValue)+"    ";
                        buffer_ptr += data_size;
                        break;
                    case DataType::FLOAT:
                        data_size = sizeof(float);
                        float fValue;
                        std::memcpy(&fValue, buffer_ptr, data_size);
                        col.col_value = fValue;
                        print_data += std::to_string(fValue)+"    ";
                        buffer_ptr += data_size;
                        break;
                    case DataType::BOOLEAN:
                        data_size = sizeof(bool);
                        bool bValue;
                        std::memcpy(&bValue, buffer_ptr, data_size);
                        col.col_value = bValue;
                        print_data += std::to_string(bValue)+"    ";
                        buffer_ptr += data_size;
                        break;
                    case DataType::STRING:{
                            int str_size;
                            std::memcpy(&str_size, buffer_ptr, 2);
                            buffer_ptr += 2;
                            data_size = str_size;
                            std::string strValue(buffer_ptr, data_size);
                            col.col_value = strValue;
                            print_data += strValue+"    ";
                            buffer_ptr += data_size;
                            break;
                    }
                    default:
                        throw std::runtime_error("Invalid DataType");
                    }

                }
                logger->logInfo({print_data});
            }
            logger->logInfo({"________________________________"});
        }
    };
} //MANAGER




#endif //TABLEMANAGER_H
