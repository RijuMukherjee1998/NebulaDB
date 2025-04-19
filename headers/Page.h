//
// Created by Riju Mukherjee on 26-02-2025.
//

#ifndef PAGE_H
#define PAGE_H

#include <cstdint>
#include <list>
#include "constants.h"
#include "Logger.h"

namespace StorageEngine
{
    struct PageHeader
    {
        uint16_t num_slots;
        uint16_t free_space_offset;
        uint16_t currSlotIdx;
    };
    struct Slot
    {
        bool isSlotValid{true};
        uint16_t offset = -1;
        uint16_t length = 0;
    };
    class Page
    {
    private:
        Utils::Logger* logger;
    public:
        PageHeader header{0, PAGE_SIZE, sizeof(PageHeader)};
        std::list<Slot> slots;
        char page_data[PAGE_SIZE];
        const uint16_t data_container_size = PAGE_SIZE;
    public:
        bool dirty = false;
        Page()
        {
            header.num_slots = 0;
            header.free_space_offset = data_container_size;
            for (int i = 0; i < data_container_size; i++)
            {
                page_data[i] = 0;
            }
            logger = Utils::Logger::getInstance();
        };
        void insertIntoPage(const std::vector<char>* new_data, uint16_t new_data_len);
        void updateIntoPage(uint16_t slot_idx, std::vector<char>* new_data, uint16_t new_data_len);
        void deleteFromPage(uint16_t slot_idx);
        void getAllDataFromPage(std::vector<std::unique_ptr<char[]>>* rows, size_t row_size) const;

    };
}

#endif //PAGE_H
