//
// Created by Riju Mukherjee on 1/26/26.
// PageData will help us to get a page from cache and to get the data
// The raw page data will be translated by this class to give the data
// This abstracts the need to explicitly access the page cache, get raw pages
// manipulate the contents to turn it into real data.
//

#include "../headers/PageData.h"

#include "../headers/PageCache.h"

StorageEngine::PageData::PageData(Schema *schema) {
    this->schema = schema;
    logger = Utils::Logger::getInstance();
    pg_cache = PageCache::getNonNullInstance();
}
StorageEngine::PageData* StorageEngine::PageData::getNonNullInstance() {
    if (pg_data_instance== nullptr) {
        logger->logCritical({"Don't call this function until PageCache is initialized"});
    }
    return pg_data_instance;
}
StorageEngine::PageData* StorageEngine::PageData::getPageDataInstance(Schema *schema) {
    std::lock_guard<std::recursive_mutex> inst_lock(pg_data_instance_mtx);
    if (pg_data_instance == nullptr) {
        pg_data_instance = new PageData(schema);
    }
    return pg_data_instance;
}

std::unique_ptr<std::vector<Column>> StorageEngine::PageData::singleRowData(PAGE_ID_TYPE page_id, SLOT_ID_TYPE slot_id) {
    std::shared_ptr<Page> page = pg_cache->getPageFromCache(page_id);
    auto row_data_raw = page->getRowFromPage(slot_id);
    pg_cache->unPinPage(page_id);
    char* buffer_ptr = row_data_raw.get();
    std::vector<Column> cols = schema->getColumns();
    BIG_SWITCH(buffer_ptr, cols);
    return std::make_unique<std::vector<Column>>(cols);
}
std::unique_ptr<std::vector<std::unique_ptr<std::vector<Column>>>> StorageEngine::PageData::multiRowData(std::vector<std::pair<PAGE_ID_TYPE,SLOT_ID_TYPE>>& pgs_slots) {
    std::unique_ptr<std::vector<std::unique_ptr<std::vector<Column>>>> rows = std::make_unique<std::vector<std::unique_ptr<std::vector<Column>>>>();
    for (const auto& pg_slot : pgs_slots) {
        std::shared_ptr<Page> page = pg_cache->getPageFromCache(pg_slot.first);
        auto row_data_raw = page->getRowFromPage(pg_slot.second);
        pg_cache->unPinPage(pg_slot.first);
        char* buffer_ptr = row_data_raw.get();
        std::vector<Column> cols = schema->getColumns();
        BIG_SWITCH(buffer_ptr,cols);
        rows->push_back(std::move(std::make_unique<std::vector<Column>>(cols)));
    }
    return std::move(rows);
}

// has the data of the entire column per page
std::unique_ptr<std::vector<std::pair<Column,SLOT_ID_TYPE>>> StorageEngine::PageData::columnData(PAGE_ID_TYPE page_id, std::string& col_name) {
    std::shared_ptr<Page> page = pg_cache->getPageFromCache(page_id);
    auto rows = std::make_unique<std::vector<std::unique_ptr<char[]>>>();
    auto slots = std::make_unique<std::vector<SLOT_ID_TYPE>>();
    page->getAllRowsFromPage(rows.get(), slots.get());
    pg_cache->unPinPage(page_id);
    std::vector<Column> cols = schema->getColumns();
    uint16_t col_id = schema->getColumn(col_name).col_id;
    std::unique_ptr<std::vector<std::pair<Column,SLOT_ID_TYPE>>> colData = std::make_unique<std::vector<std::pair<Column,SLOT_ID_TYPE>>>();
    int i = 0;
    for (auto& row_data : *rows) {
        char* buffer_ptr = row_data.get();
        BIG_SWITCH(buffer_ptr, cols);
        Column curr_col = cols.at(col_id-1);
        std::pair<Column,SLOT_ID_TYPE> cd = {curr_col,slots->at(i)};
        colData->emplace_back(cd);
        i++;
    }
    return colData;
}
