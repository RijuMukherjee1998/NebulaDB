//
// Created by Riju Mukherjee on 08-02-2025.
//

#ifndef PAGEDIRECTORYENTRY_H
#define PAGEDIRECTORYENTRY_H
#include <cstdint>
#include <vector>
#include <filesystem>

#include "Logger.h"
#include "ISerializable.h"
#include "Page.h"

namespace StorageEngine
{
    struct PDEntry
    {
        uint64_t logicalPage = 0;
        uint32_t fileId = 0;
        uint64_t pageOffset = 0;
        uint16_t freeSpace = PAGE_SIZE - sizeof(PageHeader);
        bool exists = false;
    };
    // struct PDEntryHash
    // {
    //     uint64_t operator()(const PDEntry& entry) const
    //     {
    //         return entry.logicalPage;
    //     }
    // };
    class PageDirectory : public ISerializable<PDEntry,std::vector<PDEntry>>
    {
    private:
        std::filesystem::path currSelectedDBPath;
        std::filesystem::path currSelectedTablePath;
        std::filesystem::path pgDirPath;
        Utils::Logger* logger;
        std::unique_ptr<std::unordered_map<uint64_t, StorageEngine::PDEntry>>  pd_map;
        std::array<std::mutex, PG_DIR_NUM_LATCHES> pd_latches;
        uint16_t changeCounter = 0;
    public:
        PageDirectory(const std::filesystem::path& selectedDBPath, const std::filesystem::path& selectedTablePath);
        static uint64_t getCurrentLogicalPage() {
            /* Locking is needed as the set can alter it mid-way
             * leading to getting the wrong value.
             */
            std::unique_lock<std::mutex> get_set_lock(mtx_get_set);
            return currentLogicalPage;
        }
        void loadPageDirectory() ;
        StorageEngine::PDEntry lookUpPage(uint64_t);
        void updateOnInsert(const uint16_t);
        void updateOnDelete(const uint64_t logicalPage,const uint16_t);
        void savePageDirectory(bool forcedSave);
        void lookIntoPDMap()const;

    private:
        bool findFreeSpace(uint64_t&);
        void serialize() override;
        std::vector<PDEntry> deserialize() override;
        static inline std::mutex mtx_get_set;
        std::mutex mtx_save_load;
    private:
        static uint64_t currentLogicalPage;
        static void setCurrentLogicalPage(const std::vector<PDEntry>& entries)
        {
            /* Locking is needed as the get can get the
             * old value or even some odd value mid way of set .
             */
            std::unique_lock<std::mutex> get_set_lock(mtx_get_set);
            currentLogicalPage = 0;
            for (const auto& entry : entries)
            {
                if (entry.logicalPage >= currentLogicalPage)
                {
                    currentLogicalPage = entry.logicalPage;
                }
            }
        }

    };
}


#endif //PAGEDIRECTORYENTRY_H
