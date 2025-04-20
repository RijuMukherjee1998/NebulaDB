//
// Created by Riju Mukherjee on 08-02-2025.
//

#include <utility>
#include <fstream>
#include "../headers/Schema.h"

Schema::Schema(std::string tableName, std::vector<Column> columns)
{
    this->schema_name = tableName;
    this->tableName = std::move(tableName);
    this->columns = std::move(columns);
    logger = Utils::Logger::getInstance();
}

json Schema::toJson() const
{
    json j;
    j["table_name"] = tableName;
    for (const auto& column : columns)
    {
        j["columns"].push_back(column.toJson());
    }
    return j;
}

json Schema::loadFromFile(const std::filesystem::path& filePath) const
{
    if (!std::filesystem::exists(filePath))
    {
        logger->logCritical({"Unable to find file: ", filePath.string()});
    }

    std::ifstream file(filePath);
    if (!file)
    {
        logger->logCritical({"Unable to open schema file: ", filePath.string()});
    }

    json j;
    file >> j;
    return j;
}

Schema* Schema::loadFromFileSchema(const std::filesystem::path& filePath)
{
    static Utils::Logger* logger = Utils::Logger::getInstance();
    if (!std::filesystem::exists(filePath))
    {
        logger->logCritical({"Unable to find file: ", filePath.string()});
    }
    std::ifstream file(filePath);
    if (!file)
    {
        logger->logCritical({"Unable to open schema file: ", filePath.string()});
    }
    json j;
    file >> j;
    std::string table_name = j["table_name"];
    std::vector<Column> columns;
    for (const auto& column : j["columns"])
    {
        columns.push_back(Column(column["col_name"], column["col_type"], column["is_primary_key"]
            ,column["is_null"]));
    }
    return new Schema(table_name, columns);
}

void Schema::saveToFile(const std::filesystem::path& table_path) const
{
    const std::filesystem::path file_name = (tableName + ".json");
    const std::filesystem::path filePath = table_path/file_name;

    if (std::filesystem::exists(filePath))
    {
        logger->logCritical({"Unable to create another table schema"});
        throw std::runtime_error("Unable to create another table schema");
    }
    if (std::ofstream file(filePath); file)
    {
        file << toJson().dump();
        file.close();
    }
    else
    {
        logger->logCritical({"Unable to create ",tableName,".json file"});
        throw std::runtime_error("Unable to create file");
    }
}