#include <cstdint>
#include <memory>
#include <vector>
#include "../headers/PageOps.h"

StorageEngine::PageOps::PageOps(Schema* schema, PageDirectory* pg_dir, QueryEngine::IndexTableType* it)
    : PageOps({}, {}, schema, pg_dir, it)
{
}

StorageEngine::PageOps::PageOps(const std::filesystem::path& db_path, const std::filesystem::path& table_path, Schema* schema, PageDirectory* pg_dir, QueryEngine::IndexTableType* it)
{
    this->db_path = db_path;
    this->table_path = table_path;
    pageDir = pg_dir;
    tSchema = schema;
    idx_tbl = it;
    logger = Utils::Logger::getInstance();
    pg_cache = PageCache::getNonNullInstance();
}


void StorageEngine::PageOps::IndexColumn(COL_ID_TYPE col_id)
{
    std::vector<Column> cols = tSchema->getColumns();
    if(col_id == 0 || col_id > cols.size()) {
        logger->logCritical({"Column ID out of bounds"});
        return;
    }
    if (tSchema->isColIndexed(col_id)) {
        logger->logWarn({"Column already indexed"});
        return;
    }
    tSchema->updateColumn(col_id, true);
    Column col = tSchema->getColumn(col_id);
    addToIndexTable(col);
}

ROW_ID StorageEngine::PageOps::InsertRow(std::vector<variant_data_t>& values)
{
    std::vector<Column> cols = tSchema->getColumns();
    if(values.size() != cols.size()) {
        logger->logCritical({"Number of values does not match number of columns"});
        return ROW_ID();
    }
    for (size_t i = 0; i < cols.size(); ++i) {
        cols[i].col_value = values[i];
        if(!isColumnTypeMatching(cols[i], cols[i].col_type)) {
            logger->logCritical({"Column type mismatch"});
            return ROW_ID();
        }
    }
    ROW_ID rid;
    std::vector<char> dataBuffer;
    uint16_t totalBytes = 0;
    for (const auto& val : cols) {
        addTotalBytes(val, totalBytes);
    }

    uint16_t remainingBytes = totalBytes;
    for (auto& val : cols) {
        valueToBuffer(val, &dataBuffer, remainingBytes);
    }

    pageDir->updateOnInsert(totalBytes);
    rid.pg_id = pageDir->getCurrentLogicalPage();
    std::shared_ptr<StorageEngine::Page> currPage = pg_cache->getPageFromCache(rid.pg_id);
    currPage->insertIntoPage(&dataBuffer, totalBytes, rid);
    pg_cache->markPageDirty(rid.pg_id);
    pg_cache->unPinPage(rid.pg_id);
    insertIndexedValuesForRow(cols, rid);
    return rid;
}
void StorageEngine::PageOps::UpdateRows(QueryEngine::ExecResults& result, std::unordered_map<COL_ID_TYPE, variant_data_t>& updates)
{
    std::visit([&](auto& ptr) {
        if (!ptr || ptr->empty())
            return;
        using T = std::decay_t<decltype(*ptr)>; // vector<ROW_ID> or vector<ROW>
        if constexpr (std::is_same_v<T, std::vector<ROW_ID>>) {
            for (const auto& rid : *ptr) {
                std::shared_ptr<Page> page = pg_cache->getPageFromCache(rid.pg_id);
                const uint16_t row_length = page->getRowLength(rid.slot_id);
                auto raw_data = page->getRowFromPage(rid.slot_id);
                std::vector<Column> old_cols = tSchema->getColumns();
                bufferToValue(raw_data.get(), row_length, old_cols);

                std::vector<Column> new_cols = old_cols;
                uint16_t totalBytes = 0;
                for(auto& col : new_cols){
                    auto update = updates.find(col.col_id);
                    if (update != updates.end()) {
                        col.col_value = update->second;
                    }
                    addTotalBytes(col, totalBytes);
                }

                std::vector<char> buffer;
                std::vector<char>* dataBufferPtr = &buffer;
                uint16_t currBytesLeft = totalBytes;
                for (auto& col : new_cols)
                {
                    valueToBuffer(col, dataBufferPtr, currBytesLeft);
                }

                bool newPageInsert = false;
                page->updateIntoPage(rid.slot_id, dataBufferPtr, totalBytes, newPageInsert);
                pg_cache->markPageDirty(rid.pg_id);
                pg_cache->unPinPage(rid.pg_id);
                if (newPageInsert) {
                    removeIndexedValuesForRow(old_cols, rid);
                    std::vector<variant_data_t> values;
                    for (auto& col : new_cols) {
                        values.push_back(col.col_value);
                    }
                    InsertRow(values);
                }
                else {
                    removeModifiedIndexedValuesForRow(old_cols, rid, updates);
                    insertModifiedIndexedValuesForRow(new_cols, rid, updates);
                }
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
                std::vector<Column> cols = getRowColumns(rid);
                removeIndexedValuesForRow(cols, rid);

                std::shared_ptr<Page> page = pg_cache->getPageFromCache(rid.pg_id);
                page->deleteFromPage(rid.slot_id);
                pg_cache->markPageDirty(rid.pg_id);
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
                std::vector<Column> cols = getRowColumns(rid);
                if (proj.empty()) {
                    real_data->emplace_back(cols);
                    continue;
                }

                QueryEngine::ROW projected_row;
                for (const auto& col_id : proj) {
                    if (col_id == 0 || col_id > cols.size()) {
                        logger->logWarn({"Projection column out of bounds"});
                        continue;
                    }
                    projected_row.emplace_back(cols[col_id - 1]);
                }
                real_data->emplace_back(projected_row);
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
    for(uint64_t curr_page=0; curr_page <= pageDir->getCurrentLogicalPage(); curr_page++)
    {
        filterPage(curr_page, v.get(), filter);
    }
    return v;
}
