//
// Created by Riju Mukherjee on 08-02-2025.
//

#include <memory>
#include <type_traits>
#include "../headers/TableManager.h"
#include "../headers/PageDirectory.h"
#include "../headers/Planner.h"
#include "../headers/Schema.h"
#include "../headers/PageCache.h"


Manager::TableManager::TableManager(const std::filesystem::path& currSelectedDBPath, const std::filesystem::path& currSelectedTablePath, Schema* schema):
    tSchema(schema)
{
    this->db_path = currSelectedDBPath;
    this->table_path = currSelectedTablePath;
    pageDirectory = new StorageEngine::PageDirectory(currSelectedDBPath, currSelectedTablePath);
    pageCache = StorageEngine::PageCache::getInstance(currSelectedTablePath, pageDirectory);
    logger = Utils::Logger::getInstance();
    pageDirectory->loadPageDirectory();
    populateIndexTable();
}

void Manager::TableManager::ExecuteQuery(InternalQuery::TableQuery& tbl_query)
{
    auto _query = tbl_query.query.get();
    if(_query == nullptr)
    {
        logger->logError({"Empty Query"});
        return;
    }
    if(tbl_query.type == InternalQuery::TableQuery::TableQueryType::INSERT)
    {
        insertIntoTable(_query);
    }
    else if(tbl_query.type == InternalQuery::TableQuery::TableQueryType::INDEX_COL)
    {
        createIndexOnCol(_query);
    }
    else if(tbl_query.type == InternalQuery::TableQuery::TableQueryType::UPDATE)
    {
        updateRowsInTable(_query);
    }
    else if(tbl_query.type == InternalQuery::TableQuery::TableQueryType::DELETE)
    {
        deleteRowsFromTable(_query);
    }
    else if(tbl_query.type == InternalQuery::TableQuery::TableQueryType::SELECT)
    {
        selectRowsFromTable(_query);
    }
}

void Manager::TableManager::flushAll() const
{
    tSchema->updateSchemaFile(table_path);
    pageCache->flushDirtyPages();
    pageDirectory->savePageDirectory(true);
}

ROW_ID Manager::TableManager::insertIntoTable(InternalQuery::Query* insertQuery)
{
    QueryEngine::Planner planner(db_path, table_path, tSchema, pageDirectory, index_table);
    QueryEngine::PlanType plan = planner.GeneratePlan(insertQuery);
    planner.ExecutePlan(plan);
    return {};
}

std::vector<ROW_ID> Manager::TableManager::updateRowsInTable(InternalQuery::Query* updateQuery)
{
    QueryEngine::Planner planner(db_path, table_path, tSchema, pageDirectory, index_table);
    QueryEngine::PlanType plan = planner.GeneratePlan(updateQuery);
    planner.ExecutePlan(plan);
    return {};
}

std::vector<ROW_ID> Manager::TableManager::deleteRowsFromTable(InternalQuery::Query* deleteQuery)
{
    QueryEngine::Planner planner(db_path, table_path, tSchema, pageDirectory, index_table);
    QueryEngine::PlanType plan = planner.GeneratePlan(deleteQuery);
    planner.ExecutePlan(plan);
    return {};
}

std::vector<ROW_ID> Manager::TableManager::selectRowsFromTable(InternalQuery::Query* selectQuery)
{
    QueryEngine::Planner planner(db_path, table_path, tSchema, pageDirectory, index_table);
    QueryEngine::PlanType plan = planner.GeneratePlan(selectQuery);
    QueryEngine::ExecResults results = planner.ExecutePlan(plan);
    printTableData(results);
    return {};
}

bool Manager::TableManager::createIndexOnCol(InternalQuery::Query* indexQuery)
{
    QueryEngine::Planner planner(db_path, table_path, tSchema, pageDirectory, index_table);
    QueryEngine::PlanType plan = planner.GeneratePlan(indexQuery);
    planner.ExecutePlan(plan);
    return true;
}

std::string Manager::TableManager::columnValueToString(const Column& column) const
{
    return std::visit([](const auto& value) -> std::string {
        using T = std::decay_t<decltype(value)>;
        if constexpr (std::is_same_v<T, bool>) {
            return value ? "true" : "false";
        }
        else if constexpr (std::is_same_v<T, char>) {
            return std::string(1, value);
        }
        else if constexpr (std::is_same_v<T, std::string>) {
            return value;
        }
        else {
            return std::to_string(value);
        }
    }, column.col_value);
}

void Manager::TableManager::printTableData(QueryEngine::ExecResults& results) const
{
    logger->logInfo({"Table::", tSchema->schema_name});

    std::visit([this](auto& ptr) {
        if (!ptr || ptr->empty()) {
            logger->logInfo({"No rows selected"});
            logger->logInfo({"________________________________"});
            return;
        }

        using T = std::decay_t<decltype(*ptr)>;
        if constexpr (std::is_same_v<T, std::vector<QueryEngine::ROW>>) {
            std::string col_names;
            for (const auto& col : ptr->front()) {
                col_names += col.col_name + "    ";
            }
            logger->logInfo({col_names});

            for (const auto& row : *ptr) {
                std::string print_data;
                for (const auto& col : row) {
                    print_data += columnValueToString(col) + "    ";
                }
                logger->logInfo({print_data});
            }
        }
        else if constexpr (std::is_same_v<T, std::vector<ROW_ID>>) {
            logger->logInfo({"Selected row IDs: ", std::to_string(ptr->size())});
        }

        logger->logInfo({"________________________________"});
    }, results);
}
