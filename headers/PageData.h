//
// Created by Riju Mukherjee on 1/26/26.
//

#ifndef NEBULADB_PAGEDATA_H
#define NEBULADB_PAGEDATA_H

#include <string>
#include <vector>

#include "Column.h"
#include "constants.h"
#include "PageCache.h"
#include "Schema.h"

namespace StorageEngine {
    class PageData {
    private:
        PageData();
        PageData(Schema* schema);
        inline static PageData* pg_data_instance;
        inline static Schema* schema;
        inline static std::recursive_mutex pg_data_instance_mtx;
        inline static Utils::Logger* logger;
        PageCache* pg_cache;
        void BIG_SWITCH(char* buffer_ptr, std::vector<Column>& cols) {
            for (Column& col : cols){
                DataType dt = col.col_type;
                uint16_t data_size = 0;
                switch(dt)
                {
                    case DataType::BIG_INT:
                        data_size = sizeof(uint64_t);
                        long long lValue;
                        std::memcpy(&lValue, buffer_ptr, data_size);
                        col.col_value = lValue;
                        buffer_ptr += data_size;
                        break;
                    case DataType::INT:
                        data_size = sizeof(int);
                        int iValue;
                        std::memcpy(&iValue, buffer_ptr, data_size);
                        col.col_value = iValue;
                        buffer_ptr += data_size;
                        break;
                    case DataType::SHORT:
                        data_size = sizeof(short);
                        short sValue;
                        std::memcpy(&sValue, buffer_ptr, data_size);
                        col.col_value = sValue;
                        buffer_ptr += data_size;
                        break;
                    case DataType::CHAR:
                        data_size = sizeof(char);
                        char cValue;
                        std::memcpy(&cValue, buffer_ptr, data_size);
                        col.col_value = cValue;
                        buffer_ptr += data_size;
                        break;
                    case DataType::FLOAT:
                        data_size = sizeof(float);
                        float fValue;
                        std::memcpy(&fValue, buffer_ptr, data_size);
                        col.col_value = fValue;
                        buffer_ptr += data_size;
                        break;
                    case DataType::DOUBLE:
                        data_size = sizeof(double);
                        double dValue;
                        std::memcpy(&dValue, buffer_ptr, data_size);
                        col.col_value = dValue;
                        buffer_ptr += data_size;
                        break;
                    case DataType::BOOLEAN:
                        data_size = sizeof(bool);
                        bool bValue;
                        std::memcpy(&bValue, buffer_ptr, data_size);
                        col.col_value = bValue;
                        buffer_ptr += data_size;
                        break;
                    case DataType::STRING:{
                        int str_size;
                        std::memcpy(&str_size, buffer_ptr, sizeof(uint16_t));
                        buffer_ptr += sizeof(uint16_t);
                        data_size = str_size;
                        std::string strValue(buffer_ptr, data_size);
                        col.col_value = strValue;
                        buffer_ptr += data_size;
                        break;
                    }
                    default:
                        throw std::runtime_error("Invalid DataType");
                }
            }
        }

    public:
        static PageData* getPageDataInstance(Schema* schema);
        static PageData* getNonNullInstance();
        Column singleData(PAGE_ID_TYPE page_id, SLOT_ID_TYPE slot_id);
        std::unique_ptr<std::vector<Column>> singleRowData(PAGE_ID_TYPE page_id, SLOT_ID_TYPE slot_id);
        std::unique_ptr<std::vector<std::vector<Column>>> rowDatas(PAGE_ID_TYPE page_id, SLOT_ID_TYPE slot_id);
        std::unique_ptr<std::vector<std::pair<Column,SLOT_ID_TYPE>>> columnData(PAGE_ID_TYPE page_id, std::string& col_name);
        std::unique_ptr<std::vector<std::vector<Column>>> allRowsData(PAGE_ID_TYPE page_id);
    };
}


#endif //NEBULADB_PAGEDATA_H