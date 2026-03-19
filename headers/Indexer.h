//
// Created by Riju Mukherjee on 1/18/26.
//

#ifndef NEBULADB_INDEXER_H
#define NEBULADB_INDEXER_H

#include <filesystem>
#include <type_traits>
#include "Logger.h"
#include "BPlusTree.h"
#include "ISerializable.h"
#include "constants.h"
#include "Column.h"

#define CAST 0
namespace StorageEngine
{
    // class IndexType {
    // public:
    //
    // };
    template<typename Key , typename Value>
    class Indexer final: public ISerializable<std::pair<Key,Value>,std::vector<std::pair<Key,Value>>*> {
    public:
        Indexer(const std::filesystem::path& selectedDBPath, const std::filesystem::path& selectedTablePath, const std::string&);
        std::string indexed_col_name;
        void loadIndex();
        void saveIndex(bool forcedSave);
        void createIndex();
        void updateOnInsert();
        void updateOnDelete();
        std::unique_ptr<std::vector<Column>> searchIndex(Key& key);
        std::string getIndxColName() {
            return index_name;
        }
    private:
        std::shared_ptr<BPlusTree<Key,Value,BP_TREE_ORDER>> bp_tree;
        std::filesystem::path currSelectedDBPath;
        std::filesystem::path currSelectedTablePath;
        std::string index_name;
        std::filesystem::path indexPath;
        uint16_t changeCounter = 0;
        Utils::Logger* logger;
        void serialize() override;
        std::vector<std::pair<Key,Value>>* deserialize();
        void insertDataIntoBtree(Column& data, PAGE_ID_TYPE& pg_id, SLOT_ID_TYPE& slot_id) {
            switch (data.col_type){
                case DataType::CHAR: {
                    char cdata = std::any_cast<char>(data.col_value);
                    bp_tree->insert(cdata, std::pair<PAGE_ID_TYPE,SLOT_ID_TYPE>(pg_id,slot_id));
                    break;
                }
                case DataType::SHORT: {
                    short sdata = std::any_cast<short>(data.col_value);
                    bp_tree->insert(sdata, std::pair<PAGE_ID_TYPE,SLOT_ID_TYPE>(pg_id,slot_id));
                    break;
                }
                case DataType::INT: {
                    int idata = std::any_cast<int>(data.col_value);
                    bp_tree->insert(idata, std::pair<PAGE_ID_TYPE,SLOT_ID_TYPE>(pg_id,slot_id));
                    break;
                }
                case DataType::BIG_INT: {
                    uint64_t bidata = std::any_cast<uint64_t>(data.col_value);
                    bp_tree->insert(bidata, std::pair<PAGE_ID_TYPE,SLOT_ID_TYPE>(pg_id,slot_id));
                    break;
                }
                case DataType::FLOAT: {
                    float fdata = std::any_cast<float>(data.col_value);
                    bp_tree->insert(fdata, std::pair<PAGE_ID_TYPE,SLOT_ID_TYPE>(pg_id,slot_id));
                    break;
                }
                case DataType::DOUBLE: {
                    double ddata = std::any_cast<double>(data.col_value);
                    bp_tree->insert(ddata, std::pair<PAGE_ID_TYPE,SLOT_ID_TYPE>(pg_id,slot_id));
                    break;
                }
                default:
                    logger->logError({"Invalid Data"});

            }
        }
    };
}


#endif //NEBULADB_INDEXER_H
