//
// Created by Riju Mukherjee on 08-02-2025.
//

#ifndef SCHEMA_H
#define SCHEMA_H
#include <any>
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
    Column& getColumn(const std::string& column_name) {
        for (Column& column : columns) {
            if (column.col_name == column_name) {
                return column;
            }
        }
        logger->logCritical({"No such column"});
        return *(new Column());
    }
    // for now update column support updating index
    void updateColumn(const std::string& column_name, bool is_indexed) {
        for (Column &column : columns) {
            if (column.col_name == column_name) {
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
