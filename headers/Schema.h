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
    static Schema* loadFromFileSchema(const std::filesystem::path& filePath);
private:
    Utils::Logger* logger;
    std::string tableName;
    std::vector<Column> columns;
    json toJson() const;
};



#endif //SCHEMA_H
