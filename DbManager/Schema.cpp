//
// Created by Riju Mukherjee on 08-02-2025.
//

#include <utility>
#include <fstream>
#include "../headers/Schema.h"

Schema::Schema(std::string tableName, std::vector<Column> columns)
{
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



