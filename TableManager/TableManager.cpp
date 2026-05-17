//
// Created by Riju Mukherjee on 08-02-2025.
//

#include <memory>
#include "../headers/TableManager.h"
#include "../headers/PageDirectory.h"
#include "../headers/Schema.h"
#include "../headers/PageCache.h"


Manager::TableManager::TableManager(const std::filesystem::path& currSelectedDBPath, const std::filesystem::path& currSelectedTablePath, Schema* schema):
    tSchema(schema)
{
    this->currSelectedDBPath = currSelectedDBPath;
    this->currSelectedTablePath = currSelectedTablePath;
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
    tSchema->updateSchemaFile(currSelectedTablePath);
    pageCache->flushDirtyPages();
    pageDirectory->savePageDirectory(true);
}

