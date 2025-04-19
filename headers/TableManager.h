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
        Utils::Logger* logger;
        std::filesystem::path currSelectedTablePath;
        std::filesystem::path currSelectedDBPath;
        StorageEngine::PageDirectory* pageDirectory;
        StorageEngine::PageCache* pageCache;
    public:
        TableManager(const std::filesystem::path& currSelectedDBPath, const std::filesystem::path& currSelectedTablePath);
        void insertIntoTable(std::vector<Column>& columns) const;
        void selectAllFromTable(std::vector<std::unique_ptr<char[]>>* rows, size_t row_size) const;
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
            if (columnType == typeid(char))
                total_bytes += sizeof(char);
            if (columnType == typeid(short))
                total_bytes += sizeof(short);
            if (columnType == typeid(int))
                total_bytes += sizeof(int);
            if (columnType == typeid(float))
                total_bytes += sizeof(float);
            if (columnType == typeid(std::string))
                total_bytes += strlen(std::any_cast<std::string>(column.col_value).c_str()) + 2; //2 bytes added to store length of string
        }
        
    };
} //MANAGER




#endif //TABLEMANAGER_H
