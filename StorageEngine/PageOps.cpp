#include <cstdint>
#include <memory>
#include <vector>
#include "../headers/PageOps.h"

StorageEngine::PageOps::PageOps(Schema* schema, PageDirectory* pg_dir, QueryEngine::IndexTableType* it)
{
    pageDir = pg_dir;
    tSchema = schema;
    idx_tbl = it;
    logger = Utils::Logger::getInstance();
    pg_cache = PageCache::getNonNullInstance();
}

void StorageEngine::PageOps::UpdateRows(QueryEngine::ExecResults& result, std::unordered_map<COL_ID_TYPE, variant_data_t>& updates)
{
    std::visit([&](auto& ptr) {
        if (!ptr || ptr->empty())
            return;
        using T = std::decay_t<decltype(*ptr)>; // vector<ROW_ID> or vector<ROW>
        std::vector<Column> cols = tSchema->getColumns();
        if constexpr (std::is_same_v<T, std::vector<ROW_ID>>) {
            for (const auto& rid : *ptr) {
                std::shared_ptr<Page> page = pg_cache->getPageFromCache(rid.pg_id);
                //Get the data then put the updates
                auto raw_data = page->getRowFromPage(rid.slot_id);
                char* buffer_ptr = raw_data.get();
                BIG_SWITCH(buffer_ptr, cols);
                uint16_t totalBytes = 0;
                for(auto& col : cols){
                    col.col_value = updates[col.col_id];
                    addTotalBytes(col, totalBytes);
                }
                std::vector<char> buffer;
                std::vector<char>* dataBufferPtr = &buffer;
                const std::vector<char>* currBufferPtr = dataBufferPtr;
                uint16_t currBytesLeft = totalBytes;
                for (auto& col : cols)
                {
                    valueToBuffer(col, dataBufferPtr, currBytesLeft);
                }
                page->updateIntoPage(rid.slot_id,currBufferPtr,totalBytes);
                pg_cache->unPinPage(rid.pg_id);
            }
        }
    }, result);
}
void StorageEngine::PageOps::DeleteRows(QueryEngine::ExecResults& result)
{
    std::visit([&](auto& ptr) {
        if (!ptr || ptr->empty())
            return;

        using T = std::decay_t<decltype(*ptr)>; // vector<ROW_ID> or vector<ROW>

        if constexpr (std::is_same_v<T, std::vector<ROW_ID>>) {
            for (const auto& rid : *ptr) {
                std::shared_ptr<Page> page = pg_cache->getPageFromCache(rid.pg_id);
                page->deleteFromPage(rid.slot_id);
                pg_cache->unPinPage(rid.pg_id);
            }
        }
    }, result);
}

QueryEngine::ExecResults StorageEngine::PageOps::ProjectOnRows(QueryEngine::ExecResults& result, std::vector<COL_ID_TYPE>& proj)
{
    std::unique_ptr<std::vector<QueryEngine::ROW>> real_data = std::make_unique<std::vector<QueryEngine::ROW>>();
    std::visit([&](auto& ptr) {
        if (!ptr || ptr->empty())
            return;

        using T = std::decay_t<decltype(*ptr)>; // vector<ROW_ID> or vector<ROW>

        if constexpr (std::is_same_v<T, std::vector<ROW_ID>>) {
            for (const auto& rid : *ptr) {
                std::shared_ptr<Page> page = pg_cache->getPageFromCache(rid.pg_id);

                std::unique_ptr<char[]> raw_data = page->getRowFromPage(rid.slot_id);
                pg_cache->unPinPage(rid.pg_id);

                auto cols = tSchema->getColumns();
                char* buffer_ptr = raw_data.get();

                BIG_SWITCH(buffer_ptr, cols);

                for (const auto& col_id : proj) {
                    if (cols[col_id - 1].col_id == col_id) {
                        real_data->emplace_back(cols);
                    }
                }
            }
        }
    }, result);
    return real_data;
}

QueryEngine::ExecResults StorageEngine::PageOps::IndexTableScan(Filter& filter)
{
    std::unique_ptr<std::vector<ROW_ID>> v = std::make_unique<std::vector<ROW_ID>>();
    for(auto& cond : filter.col_filter)
    {
        // get the index first from the
        auto index = getIndexFromIndexTable(cond.col_idx);
        auto range = getSearchRangeFromCond(cond);
        index->searchIndexRange(range.first,range.second, v.get());       
    }
    return v;
}

QueryEngine::ExecResults StorageEngine::PageOps::FullTableScan(Filter& filter)
{
    std::unique_ptr<std::vector<ROW_ID>> v = std::make_unique<std::vector<ROW_ID>>();
    for(uint64_t curr_page=0; curr_page < pageDir->getCurrentLogicalPage(); curr_page++)
    {
        filterPage(curr_page, v.get(), filter);
    }
    return v;
}