//
// Created by Riju Mukherjee on 08-02-2025.
//

#ifndef TABLEMANAGER_H
#define TABLEMANAGER_H

#include <cstdint>
#include <memory>
#include <string>
#include <utility>
#include <vector>
#include <unordered_map>

#include "Schema.h"
#include "Logger.h"
#include "PageCache.h"
#include "PageDirectory.h"
#include "Indexer.h"
#include "constants.h"
#include "InternalQuery.h"
#include "InternalStructs.h"

namespace Manager
{
    class TableManager {
    private:
        Schema* tSchema;
        Utils::Logger* logger;
        std::filesystem::path db_path;
        std::filesystem::path table_path;
        StorageEngine::PageDirectory* pageDirectory;
        StorageEngine::PageCache* pageCache;
        // will add support for string later
        using IndexTableType = std::unordered_map<uint16_t, std::unique_ptr<StorageEngine::Indexer<variant_data_t,std::pair<PAGE_ID_TYPE,SLOT_ID_TYPE>>>>;
        IndexTableType* index_table = nullptr;

    public:
        TableManager(const std::filesystem::path& currSelectedDBPath, const std::filesystem::path& currSelectedTablePath, Schema* schema);
        void ExecuteQuery(InternalQuery::TableQuery& tbl_query);
        void flushAll() const;

    private:
        ROW_ID insertIntoTable(InternalQuery::Query* insertQuery);
        std::vector<ROW_ID> updateRowsInTable(InternalQuery::Query* updateQuery);
        std::vector<ROW_ID> deleteRowsFromTable(InternalQuery::Query* deleteQuery);
        std::vector<ROW_ID> selectRowsFromTable(InternalQuery::Query* selectQuery);
        bool createIndexOnCol(InternalQuery::Query* indexQuery);
        void printTableData(QueryEngine::ExecResults& results) const;
        std::string columnValueToString(const Column& column) const;

        void populateIndexTable() {
            index_table = new std::unordered_map<uint16_t, std::unique_ptr<StorageEngine::Indexer<variant_data_t,std::pair<PAGE_ID_TYPE,SLOT_ID_TYPE>>>>();
            for (Column col : tSchema->getColumns()) {
                if (col.is_indexed) {
                    auto idx = std::make_unique<StorageEngine::Indexer<variant_data_t,std::pair<PAGE_ID_TYPE,SLOT_ID_TYPE>>>(db_path, table_path, col.col_name);
                    idx->loadIndex();
                    index_table->insert({col.col_id,std::move(idx)});
                }
            }
        }
        /*void printTableRow(std::unique_ptr<std::vector<Column>>& row_data) {
            const std::filesystem::path file_path = currSelectedTablePath/(tSchema->schema_name+".json");
            json schema_json = tSchema->loadFromFile(file_path);
            logger->logInfo({"Table::",schema_json["table_name"]});
            std::string col_names = "";
            std::string data = "";
            for (const auto& col : *row_data) {
                col_names += col.col_name;
                col_names += " ";
                switch (col.col_type) {
                    case DataType::BOOLEAN:
                        data += std::to_string(std::any_cast<bool>(col.col_value));
                        data += " ";
                        break;
                    case DataType::CHAR:
                        data += std::to_string(std::any_cast<char>(col.col_value));
                        data += " ";
                        break;
                    case DataType::SHORT:
                        data += std::to_string(std::any_cast<short>(col.col_value));
                        data += " ";
                        break;
                    case DataType::INT:
                        data += std::to_string(std::any_cast<int>(col.col_value));
                        data += " ";
                        break;
                    case DataType::BIG_INT:
                        data += std::to_string(std::any_cast<uint64_t>(col.col_value));
                        data += " ";
                        break;
                    case DataType::FLOAT:
                        data += std::to_string(std::any_cast<float>(col.col_value));
                        data += " ";
                        break;
                    case DataType::DOUBLE:
                        data += std::to_string(std::any_cast<double>(col.col_value));
                        data += " ";
                        break;
                    case DataType::STRING:
                        data += std::any_cast<std::string>(col.col_value);
                        data += " ";
                        break;
                    case DataType::INVALID:
                        logger->logCritical({"Invalid Data type"});
                        break;
                    default:
                        logger->logCritical({"Unknown Data Type"});
                        break;
                }
            }
            logger->logInfo({col_names});
            logger->logInfo({data});
        }
        void printTableData(std::vector<std::unique_ptr<char[]>>* table) const{
            const std::filesystem::path file_path = currSelectedTablePath/(tSchema->schema_name+".json");
            json schema_json = tSchema->loadFromFile(file_path);
            logger->logInfo({"Table::",schema_json["table_name"]});
            std::string col_names = "";
            std::vector<Column> row;
            for (const auto& col : schema_json["columns"])
            {
                Column column;
                column.col_id = col["col_id"];
                column.col_name = col["col_name"];
                column.col_type = col["col_type"];
                column.col_value = 0;
                column.is_primary_key = col["is_primary_key"];
                column.is_null = col["is_null"];
                column.is_indexed = col["is_indexed"];
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
                        case DataType::BIG_INT:
                            data_size = sizeof(long long);
                            long long lValue;
                            std::memcpy(&lValue, buffer_ptr, data_size);
                            col.col_value = lValue;
                            print_data += std::to_string(lValue)+"    ";
                            buffer_ptr += data_size;
                            break;
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
                        case DataType::DOUBLE:
                            data_size = sizeof(double);
                            double dValue;
                            std::memcpy(&dValue, buffer_ptr, data_size);
                            col.col_value = dValue;
                            print_data += std::to_string(dValue)+"    ";
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
            }*/
    };
} //MANAGER




#endif //TABLEMANAGER_H
