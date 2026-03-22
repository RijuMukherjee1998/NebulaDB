//
// Created by Riju Mukherjee on 08-02-2025.
//

#ifndef TABLEMANAGER_H
#define TABLEMANAGER_H

#include "Schema.h"
#include "Logger.h"
#include "PageCache.h"
#include "PageDirectory.h"
#include "Indexer.h"
#include "PageData.h"
#include "constants.h"

namespace Manager
{
    class TableManager {
    private:
        Schema* tSchema;
        Utils::Logger* logger;
        std::filesystem::path currSelectedTablePath;
        std::filesystem::path currSelectedDBPath;
        StorageEngine::PageDirectory* pageDirectory;
        StorageEngine::PageCache* pageCache;
        StorageEngine::PageData* pgData;


        //using variant_value_t = std::variant<StorageEngine::Indexer<short,std::pair<PAGE_ID_TYPE,SLOT_ID_TYPE>>*, StorageEngine::Indexer<int,std::pair<PAGE_ID_TYPE,SLOT_ID_TYPE>>*, StorageEngine::Indexer<uint64_t,std::pair<PAGE_ID_TYPE,SLOT_ID_TYPE>>*,
        //StorageEngine::Indexer<char,std::pair<PAGE_ID_TYPE,SLOT_ID_TYPE>>*, StorageEngine::Indexer<float,std::pair<PAGE_ID_TYPE,SLOT_ID_TYPE>>*, StorageEngine::Indexer<double,std::pair<PAGE_ID_TYPE,SLOT_ID_TYPE>>*>;
        // will add support for string later
        std::vector<std::unique_ptr<StorageEngine::Indexer<variant_data_t,std::pair<PAGE_ID_TYPE,SLOT_ID_TYPE>>>>* index_table = nullptr;
    public:
        TableManager(const std::filesystem::path& currSelectedDBPath, const std::filesystem::path& currSelectedTablePath, Schema* schema);
        void insertIntoTable(std::vector<Column>& columns) const;
        void selectAllFromTable() const;
        void createIndexOnCol(const std::string& idx_name);
        void getRowByIndex(const std::string& idx_name,variant_data_t& key);
        void getRowsByIndexRange(const std::string& idx_name,variant_data_t& key1, variant_data_t& key2);
        void flushAll() const;

    private:
        /*void BIG_SWITCH_INDEXER(Column& col, bool new_idx) {
            switch (col.col_type) {
                        case DataType::BOOLEAN: {
                            logger->logCritical({"Boolean values can't be indexed"});
                            break;
                        }
                        case DataType::SHORT: {
                            std::unique_ptr<StorageEngine::Indexer<short,std::pair<PAGE_ID_TYPE,SLOT_ID_TYPE>>> idx = std::make_unique<StorageEngine::Indexer<short,std::pair<PAGE_ID_TYPE,SLOT_ID_TYPE>>>(currSelectedDBPath, currSelectedTablePath, col.col_name);
                            if (new_idx) {
                                idx->createIndex();
                            }
                            else {
                                idx->loadIndex();
                            }
                            index_table->emplace_back(idx);
                            break;
                        }
                        case DataType::INT: {
                            std::unique_ptr<StorageEngine::Indexer<int,std::pair<PAGE_ID_TYPE,SLOT_ID_TYPE>>> idx = std::make_unique<StorageEngine::Indexer<int,std::pair<PAGE_ID_TYPE,SLOT_ID_TYPE>>>(currSelectedDBPath, currSelectedTablePath, col.col_name);
                            if (new_idx) {
                                idx->createIndex();
                            }
                            else {
                                idx->loadIndex();
                            }
                            index_table->emplace_back(idx);
                            break;
                        }
                        case DataType::BIG_INT: {
                            StorageEngine::Indexer<uint64_t,std::pair<PAGE_ID_TYPE,SLOT_ID_TYPE>>* idx = new StorageEngine::Indexer<uint64_t,std::pair<PAGE_ID_TYPE,SLOT_ID_TYPE>>(currSelectedDBPath, currSelectedTablePath, col.col_name);
                            if (new_idx) {
                                idx->createIndex();
                            }
                            else {
                                idx->loadIndex();
                            }
                            index_table->emplace_back(idx);
                            break;
                        }
                        case DataType::CHAR: {
                            StorageEngine::Indexer<char,std::pair<PAGE_ID_TYPE,SLOT_ID_TYPE>>* idx = new StorageEngine::Indexer<char,std::pair<PAGE_ID_TYPE,SLOT_ID_TYPE>>(currSelectedDBPath, currSelectedTablePath, col.col_name);
                            if (new_idx) {
                                idx->createIndex();
                            }
                            else {
                                idx->loadIndex();
                            }
                            index_table->emplace_back(idx);
                            break;
                        }
                        case DataType::FLOAT: {
                            StorageEngine::Indexer<float,std::pair<PAGE_ID_TYPE,SLOT_ID_TYPE>>* idx = new StorageEngine::Indexer<float,std::pair<PAGE_ID_TYPE,SLOT_ID_TYPE>>(currSelectedDBPath, currSelectedTablePath, col.col_name);
                            if (new_idx) {
                                idx->createIndex();
                            }
                            else {
                                idx->loadIndex();
                            }
                            index_table->emplace_back(idx);
                            break;
                        }
                        case DataType::DOUBLE: {
                            StorageEngine::Indexer<double,std::pair<PAGE_ID_TYPE,SLOT_ID_TYPE>>* idx = new StorageEngine::Indexer<double,std::pair<PAGE_ID_TYPE,SLOT_ID_TYPE>>(currSelectedDBPath, currSelectedTablePath, col.col_name);
                            if (new_idx) {
                                idx->createIndex();
                            }
                            else {
                                idx->loadIndex();
                            }
                            index_table->emplace_back(idx);
                            break;
                        }
                        // currently my bplus tree will break if string is passed as an index.(Till now comparisons are thought only on a number base)
                        case DataType::STRING: {
                            logger->logError({"Strinng index still nopt supported"});
                            break;
                        }
                        default:
                            logger->logCritical({"Invalid Data Type"});
                    }
        }*/
        void addToIndexTable(Column& col) {
            //BIG_SWITCH_INDEXER(col, true);
            auto idx = std::make_unique<StorageEngine::Indexer<variant_data_t,std::pair<PAGE_ID_TYPE,SLOT_ID_TYPE>>>(currSelectedDBPath, currSelectedTablePath, col.col_name);
            idx->createIndex();
            index_table->emplace_back(std::move(idx));
            logger->logInfo({"Index created successfully"});
        }
        void populateIndexTable() {
            index_table = new std::vector<std::unique_ptr<StorageEngine::Indexer<variant_data_t,std::pair<PAGE_ID_TYPE,SLOT_ID_TYPE>>>>();
            for (Column col : tSchema->getColumns()) {
                if (col.is_indexed) {
                    //BIG_SWITCH_INDEXER(col, false);
                    auto idx = std::make_unique<StorageEngine::Indexer<variant_data_t,std::pair<PAGE_ID_TYPE,SLOT_ID_TYPE>>>(currSelectedDBPath, currSelectedTablePath, col.col_name);
                    idx->loadIndex();
                    index_table->emplace_back(std::move(idx));
                }
            }
        }
        StorageEngine::Indexer<variant_data_t,std::pair<PAGE_ID_TYPE,SLOT_ID_TYPE>>* getIndexFromIndexTable(Column& col) {
            if (index_table == nullptr) {
                logger->logWarn({"Index not loaded or dosen't exist"});
                return nullptr;
            }
            for (auto& ptr : *index_table) {
                if (ptr.get()->getIndxColName() == col.col_name) {
                    return ptr.get();
                }
            }
            return nullptr;
        }
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
            else if (columnType == typeid(long long))
                total_bytes += sizeof(long long);
            else if (columnType == typeid(float))
                total_bytes += sizeof(float);
            else if (columnType == typeid(double))
                total_bytes += sizeof(double);
            else if (columnType == typeid(std::string))
                total_bytes += strlen(std::any_cast<std::string>(column.col_value).c_str()) + 2; //2 bytes added to store length of string
        }

        void printTableRow(std::unique_ptr<std::vector<Column>> row_data) {
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
        void printTableData(std::vector<std::unique_ptr<char[]>>* table) const
        {
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
        }
    };
} //MANAGER




#endif //TABLEMANAGER_H
