#ifndef INTERNALSTRUCTS_H
#define INTERNALSTRUCTS_H

#include <vector>
#include <variant>
#include <memory>
#include "constants.h"
#include "Column.h"

struct ROW_ID;
namespace QueryEngine {
    using ROW = std::vector<Column>;
    /* See the TableManager public API's returns */
    using ExecResults = std::variant<std::unique_ptr<std::vector<ROW_ID>>,
                                     std::unique_ptr<std::vector<ROW>>>;
}


enum ExecConds
{
    EQUAL = 0,
    LESS,
    LESS_EQUAL,
    GREATER,
    GREATER_EQUAL,
    IN_BETWEEN
};

namespace QueryEngine{
struct ExecCondition{
    enum Filtype{
        SINGLE_VALUE,
        RANGE_VALUE
    };
    public:
        uint16_t col_idx;
        ExecConds op;
        Filtype fil_type;
        variant_data_t value;
        std::pair<variant_data_t, variant_data_t> range;
};
}
struct ROW_ID{
    PAGE_ID_TYPE pg_id;
    SLOT_ID_TYPE slot_id;
};

struct Filter{
    std::vector<QueryEngine::ExecCondition> col_filter;
};

#endif 