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
    INVALID,
    BOOLEAN ,
    CHAR,
    SHORT,       // 16-bit signed
    INT,        //32-bit signed
    BIG_INT,   //64-bit unsigned
    FLOAT,    //32-bit floating signed
    DOUBLE,  // 64-bit floating signed
    STRING,
};

struct Column
{
    uint16_t col_id;
    std::string col_name;
    DataType col_type = DataType::INVALID;
    bool is_primary_key = false;
    bool is_null = false;
    bool is_indexed = false;
    std::any col_value;

    json toJson() const {
        return {
                    {"col_id", col_id},
                    {"col_name", col_name},
                    {"col_type", col_type},
                    {"is_primary_key", is_primary_key},
                    {"is_null", is_null},
                    {"is_indexed", is_indexed}
        };
    }
};
#endif //COLUMN_H
