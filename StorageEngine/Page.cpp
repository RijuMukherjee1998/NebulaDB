//
// Created by Riju Mukherjee on 01-03-2025.
//
#include <vector>
#include "../headers/Page.h"

namespace StorageEngine
{
    void Page::insertIntoPage(std::vector<char>* new_data, const uint16_t new_data_len, ROW_ID& pg_slot)
    {
        if (new_data == nullptr || new_data_len == 0)
        {
            logger->logCritical({"Weird data is null"});
            return;
        }
        if(new_data_len > data_container_size)
        {
            logger->logCritical({"Data is too big to fit in a single page"});
            return;
        }
        if (header.free_space_offset - new_data_len < header.currSlotIdx)
        {
            logger->logCritical({"Page is full...PageDirectory should handle this condition"});
            return;
        }

        Slot new_slot = {};
        const int start_index = header.free_space_offset - new_data_len;
        const int end_index = header.free_space_offset;
        new_slot.offset = static_cast<uint16_t>(start_index);
        new_slot.length = static_cast<uint16_t>(new_data_len);
        //always keep newer items in the front so that you don't have to reverse the slot later
        slots.push_front(new_slot);
        //Copy the data into the page_data array
        int c = 0;
        for (int i = start_index; i < end_index; i++)
        {
            page_data[i] = new_data->at(c);
            c++;
        }
        header.num_slots ++;
        header.free_space_offset = start_index;
        header.currSlotIdx += sizeof(Slot);
        pg_slot.slot_id = header.num_slots;
    }

    void Page::updateIntoPage(const uint16_t slot_idx, std::vector<char>* new_data, const uint16_t new_data_len, bool& newPageInsert)
    {
        if (new_data == nullptr || new_data_len == 0){
            logger->logCritical({"Weird Can't Update data is null"});
            return;
        }
        if(new_data_len > data_container_size){
            logger->logCritical({"Data is too big to fit in a single page"});
            return;
        }
        auto slot_it = slots.begin();
        std::advance(slot_it, slot_idx);
        //uint16_t offset = slot_it->offset;
        const uint16_t length = slot_it->length;
        const uint16_t start_index = slot_it->offset;
        uint16_t end_index = 0;
        if (new_data_len == length){
            end_index = slot_it->offset + new_data_len;
        }
        // this will create fragmentation, but it's better than wasting a lot of space
        else if (new_data_len < length){
            end_index = start_index + new_data_len;
            slot_it->length = new_data_len;
        }
        else{
            slot_it->isSlotValid = false;
            newPageInsert = true;
            return;
        }
        //copy the data to update
        for (int i = start_index; i < end_index; i++){
            page_data[i] = new_data->at(i);
        }
    }

    void Page::deleteFromPage(const uint16_t slot_idx)
    {
        auto slot_it = slots.begin();
        std::advance(slot_it, slot_idx);
        if(slot_it->isSlotValid)
        {
            slot_it->isSlotValid = false;
            return;
        }
        logger->logCritical({"Weird Page Already Deleted"});
    }

    std::unique_ptr<char[]> Page::getRowFromPage(const uint16_t slot_idx)
    {
        auto slot_it = slots.begin();
        std::advance(slot_it, slot_idx);
        std::unique_ptr<char[]> data = std::make_unique<char[]>(slot_it->length);
        std::memcpy(data.get(), (page_data+slot_it->offset), slot_it->length);
        return data;
    }

    uint16_t Page::getRowLength(const uint16_t slot_idx) const
    {
        auto slot_it = slots.begin();
        std::advance(slot_it, slot_idx);
        return slot_it->length;
    }

    std::unique_ptr<std::vector<std::unique_ptr<char[]>>> Page::getRowsFromPage(const std::vector<uint16_t>& slot_idxs) {
        std::unique_ptr<std::vector<std::unique_ptr<char[]>>> rows = std::make_unique<std::vector<std::unique_ptr<char[]>>>();
        for (const auto& slot_idx : slot_idxs) {
            auto slot_it = slots.begin();
            std::advance(slot_it,slot_idx);
            std::unique_ptr<char[]> data = std::make_unique<char[]>(slot_it->length);
            std::memcpy(data.get(), (page_data+slot_it->offset), slot_it->length);
            rows->push_back(std::move(data));
        }
        return rows;
    }

    void Page::getAllRowsFromPage(std::vector<std::unique_ptr<char[]>>* rows, std::vector<SLOT_ID_TYPE>* all_slots) const
    {
        SLOT_ID_TYPE slot_idx = 0;
        for (const auto& slot : slots)
        {
            auto row = std::make_unique<char[]>(slot.length);
            std::memset(row.get(), 0, slot.length);
            char* buffer = row.get();
            if (slot.isSlotValid) {
                int c = 0;
                for (int i = slot.offset; i < (slot.offset + slot.length); i++)
                {
                    *(buffer+c) = page_data[i];
                    c++;
                }
                rows->push_back(std::move(row));
                all_slots->push_back(slot_idx);
            }
            slot_idx++;
        }

    }
}
