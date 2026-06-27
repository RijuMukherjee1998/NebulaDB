//
// Created by Riju Mukherjee on 1/18/26.
//

#include <fstream>
#include <memory>
#include "../headers/Indexer.h"
#include "../headers/BPlusTree.h"

template<typename Key , typename Value>
StorageEngine::Indexer<Key,Value>::Indexer(const std::filesystem::path &selectedDBPath, const std::filesystem::path &selectedTablePath, const std::string& idx_name) {
    currSelectedDBPath = selectedDBPath;
    currSelectedTablePath = selectedTablePath;
    logger = Utils::Logger::getInstance();
    index_name = idx_name;
    indexPath = currSelectedTablePath/idx_name.c_str();
    changeCounter = 0;
    bp_tree = std::make_shared<BPlusTree<Key,Value,BP_TREE_ORDER>>();
}

template<typename Key, typename Value>
void StorageEngine::Indexer<Key, Value>::createIndex(std::unique_ptr<std::vector<std::pair<Column,ROW_ID>>> col_datas) {
    for(auto data : *col_datas) {
        insertDataIntoBtree(data.first,data.second.pg_id, data.second.slot_id);
    }
    saveIndex(true);
}



template<typename Key, typename Value>
void StorageEngine::Indexer<Key, Value>::searchIndexRange(Key& startKey, Key& endKey, std::vector<ROW_ID>* rows)
{
    if (rows == nullptr) {
        return;
    }

    bool found = false;
    std::unique_ptr<std::vector<Value>> values = bp_tree->searchRange(startKey, endKey, found);
    if (!found || values == nullptr) {
        return;
    }

    for (const auto& value : *values) {
        rows->push_back({value.first, value.second});
    }
}

template<typename Key, typename Value>
void StorageEngine::Indexer<Key, Value>::updateOnInsert(Column& updated_col, std::pair<PAGE_ID_TYPE,SLOT_ID_TYPE>& pg_slot)
{
    insertDataIntoBtree(updated_col, pg_slot.first, pg_slot.second);
    changeCounter++;
    saveIndex(false);
}

template<typename Key, typename Value>
size_t StorageEngine::Indexer<Key, Value>::updateOnModify(const Key& startKey, const Key& endKey, std::vector<ROW_ID>* modifiedRows)
{
    return updateOnDelete(startKey, endKey, modifiedRows);
}

template<typename Key, typename Value>
size_t StorageEngine::Indexer<Key, Value>::updateOnDelete(const Key& startKey, const Key& endKey, std::vector<ROW_ID>* deletedRows)
{
    if (deletedRows == nullptr || deletedRows->empty()) {
        return 0;
    }

    auto isTargetRow = [&](const Value& value) {
        for (const auto& row : *deletedRows) {
            if (value.first == row.pg_id && value.second == row.slot_id) {
                return true;
            }
        }
        return false;
    };

    size_t changedRows = 0;
    std::vector<std::pair<Key, Value>> deletedEntries = bp_tree->deleteRange(startKey, endKey);
    for (const auto& [key, value] : deletedEntries) {
        if (isTargetRow(value)) {
            changedRows++;
            continue;
        }
        bp_tree->insert(key, value);
    }

    if (changedRows > 0) {
        changeCounter += changedRows;
        saveIndex(false);
    }
    return changedRows;
}

template<typename Key , typename Value>
void StorageEngine::Indexer<Key,Value>::serialize() {
    // want to rewrite the entire file not append to it so std::ios::truc option is used
    std::ofstream id_file(indexPath.string(), std::ios::binary|std::ios::trunc);
    if (!id_file.good()) {
        logger->logCritical({"Index file could not be opened for write: ", currSelectedTablePath.string()});
    }
    auto all_indices = bp_tree->getAllIndices();
    const uint64_t count = all_indices->size();
    id_file.write(reinterpret_cast<const char *>(&count), sizeof(count));
    for (const auto& [key, value] : *all_indices) {
        id_file.write(reinterpret_cast<const char*>(&key), sizeof(key));
        id_file.write(reinterpret_cast<const char *>(&value.first), sizeof(value.first));
        id_file.write(reinterpret_cast<const char *>(&value.second), sizeof(value.second));
    }
    id_file.close();
}
template<typename Key , typename Value>
std::vector<std::pair<Key,Value>>* StorageEngine::Indexer<Key,Value>::deserialize() {
    std::ifstream id_file(indexPath.string(), std::ios::binary);
    if (!id_file.good()) {
        logger->logCritical({"Index file could not be opened for read: ", currSelectedTablePath.string()});
    }
    // read the no of entries
    uint64_t count = 0;
    id_file.read(reinterpret_cast<char*>(&count), sizeof(count));
    auto* all_indices = new std::vector<std::pair<Key,Value>>(count);
    for (auto& entry : *all_indices) {
        Key key;
        Value val;
        id_file.read(reinterpret_cast<char*>(&key), sizeof(Key));
        id_file.read(reinterpret_cast<char *>(&val.first), sizeof(val.first));
        id_file.read(reinterpret_cast<char *>(&val.second), sizeof(val.second));
        entry.first = key;
        entry.second = val;
    }
    return all_indices;
}

template<typename Key , typename Value>
void StorageEngine::Indexer<Key,Value>::saveIndex(bool forcedSave) {
    if (changeCounter > 1024 || forcedSave) {
        changeCounter = 0;
        serialize();
        logger->logInfo({"Index Saved Successfully"});
    }
}

template<typename Key, typename Value>
void StorageEngine::Indexer<Key, Value>::loadIndex() {
    if (!std::filesystem::exists(indexPath)) {
        logger->logCritical({"Index Table not found ... Critical index not saved on last indexing.",indexPath.c_str()});
        return;
    }
    std::vector<std::pair<Key,Value>>*all_vals = deserialize();
    long unsigned int i = 0;
    while (i < all_vals->size()) {
        std::pair<Key, Value> kv_pair = all_vals->at(i);
        bp_tree->insert(kv_pair.first, kv_pair.second);
        i++;
    }
    logger->logInfo({"Successfully loaded index tree"});
}
