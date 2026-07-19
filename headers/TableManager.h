//
// Created by Riju Mukherjee on 08-02-2025.
//

#ifndef TABLEMANAGER_H
#define TABLEMANAGER_H

#include <cstdint>
#include <memory>
#include <string>
#include <utility>
#include <vector>
#include <unordered_map>

#include "Schema.h"
#include "Logger.h"
#include "PageCache.h"
#include "PageDirectory.h"
#include "Indexer.h"
#include "constants.h"
#include "InternalQuery.h"
#include "InternalStructs.h"

namespace Manager
{
    class TableManager {
    private:
        std::unique_ptr<Schema> tSchema;
        Utils::Logger* logger;
        std::filesystem::path db_path;
        std::filesystem::path table_path;
        StorageEngine::PageDirectory* pageDirectory;
        StorageEngine::PageCache* pageCache;
        // will add support for string later
        using IndexTableType = std::unordered_map<uint16_t, std::unique_ptr<StorageEngine::Indexer<variant_data_t,std::pair<PAGE_ID_TYPE,SLOT_ID_TYPE>>>>;
        IndexTableType* index_table = nullptr;

    public:
        TableManager(const std::filesystem::path& currSelectedDBPath, const std::filesystem::path& currSelectedTablePath, Schema* schema);
        ~TableManager(){}
        void ExecuteQuery(InternalQuery::TableQuery& tbl_query);
        void flushAll() const;

    private:
        ROW_ID insertIntoTable(InternalQuery::Query* insertQuery);
        std::vector<ROW_ID> updateRowsInTable(InternalQuery::Query* updateQuery);
        std::vector<ROW_ID> deleteRowsFromTable(InternalQuery::Query* deleteQuery);
        std::vector<ROW_ID> selectRowsFromTable(InternalQuery::Query* selectQuery);
        bool createIndexOnCol(InternalQuery::Query* indexQuery);
        void printTableData(QueryEngine::ExecResults& results) const;
        std::string columnValueToString(const Column& column) const;

        void populateIndexTable() {
            index_table = new std::unordered_map<uint16_t, std::unique_ptr<StorageEngine::Indexer<variant_data_t,std::pair<PAGE_ID_TYPE,SLOT_ID_TYPE>>>>();
            for (Column col : tSchema->getColumns()) {
                if (col.is_indexed) {
                    auto idx = std::make_unique<StorageEngine::Indexer<variant_data_t,std::pair<PAGE_ID_TYPE,SLOT_ID_TYPE>>>(db_path, table_path, col.col_name);
                    idx->loadIndex();
                    index_table->insert({col.col_id,std::move(idx)});
                }
            }
        }
    };
} //MANAGER




#endif //TABLEMANAGER_H
