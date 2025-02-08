//
// Created by Riju Mukherjee on 08-02-2025.
//

#ifndef SCHEMA_H
#define SCHEMA_H
#include <string>
#include <nlohmann/json.hpp>

#include "Logger.h"

using json = nlohmann::json;
enum class DataType
{
    BOOLEAN,
    CHAR,
    SHORT,
    INT,
    FLOAT,
    STRING,
};

struct Column
{
    std::string col_name;
    DataType col_type;
    bool is_primary_key = false;
    bool is_null = false;

    json toJson() const {
        return {
                {"col_name", col_name},
                {"col_type", col_type},
                {"is_primary_key", is_primary_key},
                {"is_null", is_null}
        };
    }
};
class Schema {
public:
    Schema(std::string tableName, std::vector<Column> columns);
    void saveToFile(const std::filesystem::path& table_path) const;
private:
    Utils::Logger* logger;
    std::string tableName;
    std::vector<Column> columns;
    json toJson() const;
};



#endif //SCHEMA_H
