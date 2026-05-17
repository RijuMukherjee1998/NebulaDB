//
// Created by Riju Mukherjee on 08-02-2025.
//

#ifndef SCHEMA_H
#define SCHEMA_H

#include <cstdint>
#include <string>
#include <nlohmann/json.hpp>
#include <vector>
#include <filesystem>

#include "Logger.h"
#include "Column.h"

class Schema {
public:
    std::string schema_name;
    Schema(std::string tableName, std::vector<Column> columns);
    void saveToFile(const std::filesystem::path& table_path) const;
    json loadFromFile(const std::filesystem::path& filePath) const;
    void updateSchemaFile(const std::filesystem::path& table_path) const;
    static Schema* loadFromFileSchema(const std::filesystem::path& filePath);
    std::vector<Column> getColumns() {
        return columns;
    }
    bool isColIndexed(uint16_t& col_idx)
    {
        for(Column& column : columns){
            if(column.col_id == col_idx)
            {
                return column.is_indexed;
            }
        }
        return false;
    }
    uint16_t getColumnID(const std::string& column_name)
    {
        for(Column& column : columns){
            if(column.col_name == column_name)
            {
                return column.col_id;
            }
        }
        // 0 column_id means no column found
        return 0;
    }
    Column& getColumn(const uint16_t col_id) {
        for (Column& column : columns) {
            if (column.col_id == col_id) {
                return column;
            }
        }
        logger->logError({"No such column"});
        return *(new Column());
    }
    // for now update column support updating index
    void updateColumn(const uint16_t& column_id, bool is_indexed) {
        for (Column &column : columns) {
            if (column.col_id == column_id) {
                column.is_indexed = is_indexed;
                return;
            }
        }
        logger->logError({"No such column"});
    }
private:
    Utils::Logger* logger;
    std::string tableName;
    std::vector<Column> columns;
    json toJson() const;
};



#endif //SCHEMA_H
