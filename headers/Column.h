//
// Created by teama on 20-04-2025.
//

#ifndef COLUMN_H
#define COLUMN_H
#include <any>
#include <string>
#include <nlohmann/json.hpp>

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
    std::any col_value;

    json toJson() const {
        return {
                    {"col_name", col_name},
                    {"col_type", col_type},
                    {"is_primary_key", is_primary_key},
                    {"is_null", is_null}
        };
    }
};
#endif //COLUMN_H
