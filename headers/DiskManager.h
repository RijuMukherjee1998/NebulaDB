//
// Created by Riju Mukherjee on 01-03-2025.
//

#ifndef PAGEMANAGER_H
#define PAGEMANAGER_H

#include <cstdint>
#include <filesystem>
#include "Page.h"

namespace StorageEngine
{
    class DiskManager {
        std::recursive_mutex pg_mut;
        std::recursive_mutex file_mut;
        std::filesystem::path currTablePath;
        Utils::Logger* logger = Utils::Logger::getInstance();

        void fileWrite(uint32_t file_id, uint64_t pg_offset, const std::unique_ptr<char[]>&);
        std::shared_ptr<Page> fileRead(uint32_t file_id, uint64_t pg_offset);
        std::unique_ptr<char[]> convertPageToBuffer(const std::shared_ptr<Page>&);
        std::unique_ptr<Page> convertBufferToPage(const std::unique_ptr<char[]>&);
    public:
        DiskManager(const std::filesystem::path& currTablePath);
        void writePageToDisk(uint32_t file_id, uint64_t page_offset, const std::shared_ptr<Page>&);
        std::shared_ptr<Page> readPageFromDisk(uint32_t file_id, uint64_t page_offset);
    };
}


#endif //PAGEMANAGER_H
