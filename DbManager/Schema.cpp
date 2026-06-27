//
// Created by Riju Mukherjee on 08-02-2025.
//

#include <utility>
#include <fstream>
#include <stdexcept>
#include "../headers/Schema.h"

namespace {
DataType parseDataType(const json& column, const bool legacy_schema)
{
    const int type_code = column.value("col_type", static_cast<int>(DataType::INVALID));

    if (legacy_schema)
    {
        if (type_code == 3)
            return DataType::INT;
        if (type_code == 5)
            return DataType::STRING;
    }

    return static_cast<DataType>(type_code);
}
}

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
    if (!j.contains("columns") || !j["columns"].is_array())
    {
        logger->logCritical({"Invalid schema file: missing columns array: ", filePath.string()});
        throw std::runtime_error("Invalid schema file");
    }

    std::string table_name = j.value("table_name", filePath.stem().string());
    std::vector<Column> columns;
    uint16_t fallback_col_id = 1;
    for (const auto& column : j["columns"])
    {
        const bool legacy_schema = !column.contains("col_id") || !column.contains("is_indexed");
        columns.push_back(Column{
            column.value("col_id", fallback_col_id),
            column.value("col_name", std::string{}),
            parseDataType(column, legacy_schema),
            column.value("is_primary_key", false),
            column.value("is_null", false),
            column.value("is_indexed", false)
        });
        fallback_col_id++;
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

void Schema::updateSchemaFile(const std::filesystem::path& table_path) const {
    const std::filesystem::path file_name = (tableName + ".json");
    const std::filesystem::path filePath = table_path/file_name;
    if (std::ofstream file(filePath); file)
    {
        file << toJson().dump();
        file.close();
    }
    else
    {
        logger->logCritical({"Unable to update ",tableName,".json file"});
        throw std::runtime_error("Unable to update schema file");
    }
}