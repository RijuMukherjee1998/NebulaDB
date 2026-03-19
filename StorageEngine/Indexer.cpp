//
// Created by Riju Mukherjee on 1/18/26.
//

#include <fstream>
#include "../headers/Indexer.h"
#include "../headers/BPlusTree.h"
#include "../headers/PageCache.h"
#include "../headers/PageData.h"

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
void StorageEngine::Indexer<Key, Value>::createIndex() {
    PageData* pg_data = PageData::getNonNullInstance();
    uint64_t currLogicalPageId = PageDirectory::getCurrentLogicalPage();
    for (uint64_t i=0; i<currLogicalPageId; i++) {
        auto col_data = pg_data->columnData(i, index_name);
        if (col_data->empty())
            logger->logError({"Sorry no such index col available"});
        for(auto data : *col_data) {
            insertDataIntoBtree(data.first, i, data.second);
        }
    }
    saveIndex(true);
}

template<typename Key, typename Value>
std::unique_ptr<std::vector<Column>> StorageEngine::Indexer<Key, Value>::searchIndex(Key& key) {
    PageData* pg_data = PageData::getNonNullInstance();
    bool found = false;
    std::unique_ptr<std::vector<Column>> row = nullptr;
    std::unique_ptr<Value> val = bp_tree->searchKey(key,found);
    if (found) {
        PAGE_ID_TYPE pg_id = val.get()->first;
        SLOT_ID_TYPE slot_id = val.get()->second;
        row = pg_data->singleRowData(pg_id, slot_id);
    }
    return row;
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
    int i = 0;
    while (i < all_vals->size()) {
        std::pair<Key, Value> kv_pair = all_vals->at(i);
        bp_tree->insert(kv_pair.first, kv_pair.second);
        i++;
    }
    logger->logInfo({"Successfully loaded index tree"});
}

template class StorageEngine::Indexer<variant_data_t,  std::pair<PAGE_ID_TYPE, SLOT_ID_TYPE>>;
/*
template class StorageEngine::Indexer<char,  std::pair<PAGE_ID_TYPE, SLOT_ID_TYPE>>;
template class StorageEngine::Indexer<short, std::pair<PAGE_ID_TYPE, SLOT_ID_TYPE>>;
template class StorageEngine::Indexer<int,   std::pair<PAGE_ID_TYPE, SLOT_ID_TYPE>>;
template class StorageEngine::Indexer<uint64_t, std::pair<PAGE_ID_TYPE, SLOT_ID_TYPE>>;
template class StorageEngine::Indexer<float, std::pair<PAGE_ID_TYPE, SLOT_ID_TYPE>>;
template class StorageEngine::Indexer<double, std::pair<PAGE_ID_TYPE, SLOT_ID_TYPE>>;*/