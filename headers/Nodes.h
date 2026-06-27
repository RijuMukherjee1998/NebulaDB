/*      void indexedInsert(Column& col, std::pair<PAGE_ID_TYPE,SLOT_ID_TYPE>& pg_slot) {
            if(col.is_indexed)
            {
                auto idx = getIndexFromIndexTable(col);
                if(idx == nullptr)
                {
                    logger->logError({"No index found for that column"});
                    return;
                }
                idx->updateOnInsert(col,pg_slot);
            }
        }
        std::unique_ptr<StorageEngine::PageData> Manager::TableManager::selectRowsByIndexRange(const uint16_t col_id, variant_data_t& start_key, variant_data_t& end_key, bool printable=false) {
            if (tSchema->getColumn(col_id).is_indexed == false) {
                logger->logWarn({"Column ", col_id, " not indexed"});
                return nullptr;
            }
            Column col = tSchema->getColumn(col_id);
            auto indexer = getIndexFromIndexTable(col);
            auto rows = indexer->searchIndexRange(start_key, end_key);
            if(printable){
                for (auto& row : *rows) {
                    printTableRow(row);
                }
            }
        }
        void modifyRowsByIndexRange(const std::string& idx_name,variant_data_t& start_key, variant_data_t& end_key, bool delete_rows){
            if(tSchema->getColumn(idx_name).is_indexed == false){
                logger->logWarn({"Column ", idx_name, " not indexed"});
                return;
            }
            Column col = tSchema->getColumn(idx_name);
            auto indexer = getIndexFromIndexTable(col);
            if(delete_rows){
                auto mod_rows = indexer->updateOnDelete(start_key, end_key);
                logger->logInfo({"Deleted Rows Returned = ",std::to_string(mod_rows)});
            } else{
                auto mod_rows = indexer->updateOnModify( const std::variant<char, short, int, unsigned long, float, double> &startKey, const std::variant<char, short, int, unsigned long, float, double> &endKey)
            }
        }
       
       
        


*/

/*
bool Manager::TableManager::createIndexOnCol(InternalQuery::IndexQuery* index_query) {
    uint16_t col_id = index_query->col_id;
    if (tSchema->getColumn(col_id).is_indexed == true) {
        logger->logWarn({"Column ", std::to_string(col_id), " already indexed"});
        return false;
    }
    tSchema->updateColumn(col_id, true);
    Column col = tSchema->getColumn(col_id);
    addToIndexTable(col);
}


void Manager::TableManager::insertIntoTable(InternalQuery::InsertQuery& insertQuery)
{
    std::unique_ptr<StorageEngine::PageData> pgData = std::make_unique<StorageEngine::PageData>(tSchema);
    std::pair<PAGE_ID_TYPE,SLOT_ID_TYPE> pg_slot;
    pgData->insertIntoTable(insertQuery.cols,pg_slot);
    for (auto& col :insertQuery.cols) {
        indexedInsert(col,pg_slot);
    }
}
void Manager::TableManager::deleteRowsFromTable(InternalQuery::OrQuery& selectQuerys)
{
    
}
*/