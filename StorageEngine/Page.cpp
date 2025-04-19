//
// Created by Riju Mukherjee on 01-03-2025.
//
#include "../headers/Page.h"
#include "../headers/PageDirectory.h"
#include <iostream>
namespace StorageEngine
{
    void Page::insertIntoPage(const std::vector<char>* new_data, const uint16_t new_data_len)
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
            throw std::runtime_error("Page is full");
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
        // std::cout << "Row bytes write: ";
        for (int i = start_index; i < end_index; i++)
        {
            page_data[i] = new_data->at(c);
            //printf("%02X ", static_cast<unsigned char>(page_data[i]));
            c++;
        }
        // std::cout << std::endl;
        header.num_slots ++;
        header.free_space_offset = start_index;
        header.currSlotIdx += sizeof(Slot);
    }

    void Page::updateIntoPage(const uint16_t slot_idx, std::vector<char>* new_data, const uint16_t new_data_len)
    {
        if (new_data == nullptr || new_data_len == 0)
        {
            logger->logCritical({"Weird Can't Update data is null"});
            return;
        }
        if(new_data_len > data_container_size)
        {
            logger->logCritical({"Data is too big to fit in a single page"});
            return;
        }
        auto slot_it = slots.begin();
        std::advance(slot_it, slot_idx);
        uint16_t offset = slot_it->offset;
        const uint16_t length = slot_it->length;
        const uint16_t start_index = slot_it->offset;
        uint16_t end_index = 0;
        if (new_data_len == length)
        {
            end_index = slot_it->offset + new_data_len;
        }
        // this will create fragmentation, but it's better than wasting a lot of space
        else if (new_data_len < length)
        {
            end_index = start_index + new_data_len;
            slot_it->length = new_data_len;
        }
        else
        {
            slot_it->isSlotValid = false;
            insertIntoPage(new_data, new_data_len);
            return;
        }
        //copy the data to update
        for (int i = start_index; i < end_index; i++)
        {
            page_data[i] = new_data->at(i);
        }
    }

    void Page::deleteFromPage(const uint16_t slot_idx)
    {
        auto slot_it = slots.begin();
        std::advance(slot_it, slot_idx);
        slot_it->isSlotValid = false;
    }

    void Page::getAllDataFromPage(std::vector<std::unique_ptr<char[]>>* rows, const size_t row_size) const
    {
        for (const auto& slot : slots)
        {
            auto row = std::make_unique<char[]>(row_size);
            std::memset(row.get(), 0, row_size);
            char* buffer = row.get();
            if (slot.isSlotValid)
            {
                int c = 0;
                for (int i = slot.offset; i < (slot.offset + slot.length); i++)
                {
                    *(buffer+c) = page_data[i];
                    c++;
                }
            }
            // std::cout << "Row bytes read: ";
            // for (size_t i = 0; i < row_size; ++i)
            // {
            //     printf("%02X ", static_cast<unsigned char>(row[i]));
            // }
            // std::cout << std::endl;
            rows->push_back(std::move(row));
        }

    }
}