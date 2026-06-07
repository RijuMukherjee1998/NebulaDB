#pragma once

#include <cstdint>
#include<vector>
#include <unordered_map>
#include <memory>

#include "constants.h"

namespace InternalQuery {
enum QueryType {
    SELECT_QUERY,
    UPDATE_QUERY,
    DELETE_QUERY,
    INSERT_QUERY,
    INDEX_QUERY
};
class Query {
public:
    QueryType qtype;
    virtual ~Query() = default;
};

enum Conditions {
    EQUAL = 0,
    LESS,
    LESS_EQUAL,
    GREATER,
    GREATER_EQUAL,
    IN_BETWEEN
};

class Condition {
    enum Filtype{
        SINGLE_VALUE,
        RANGE_VALUE
    };
    public:
        uint16_t col_idx;
        Conditions op;
        Filtype fil_type;
        variant_data_t value;
        std::pair<variant_data_t, variant_data_t> range;
};

class AndQuery {
public:
    std::vector<Condition> conditions;
};

class OrQuery {
public:
    std::vector<AndQuery> and_groups;
};

class SelectQuery : public Query {
public:
    QueryType qtype = QueryType::SELECT_QUERY;
    OrQuery predicate;
    std::vector<uint16_t> projection;
};

class UpdateQuery : public Query {
public:
    QueryType qtype = QueryType::UPDATE_QUERY;
    OrQuery predicate;
    std::unordered_map<uint16_t, variant_data_t> updates;
};

class DeleteQuery : public Query {
public:
    QueryType qtype = QueryType::DELETE_QUERY;
    OrQuery predicate;
};


class InsertQuery : public Query {
public:
    QueryType qtype = QueryType::INSERT_QUERY;
    std::vector<variant_data_t> values;
};

class IndexQuery : public Query {
public:
    QueryType qtype = QueryType::INDEX_QUERY;
    uint16_t col_id;
};

class TableQuery {
public:
    enum TableQueryType {
        SELECT,
        INSERT,
        UPDATE,
        DELETE,
        INDEX_COL
    };
    std::string table_name;
    TableQueryType type;
    std::unique_ptr<Query> query;
};

}
