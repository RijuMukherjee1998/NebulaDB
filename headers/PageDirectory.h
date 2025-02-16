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

namespace StorageEngine
{
    struct PDEntry
    {
        uint64_t logicalPage = 0;
        uint32_t fileId = 0;
        uint64_t pageOffset = 0;
        uint16_t freeSpace = PAGE_SIZE;
        bool exists = false;
        // bool operator == (const PDEntry& rhs) const
        // {
        //     return logicalPage == rhs.logicalPage;
        // }
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
        std::unique_ptr<std::unordered_map<uint64_t, StorageEngine::PDEntry>> pd_map;
        uint16_t changeCounter = 0;
        uint64_t currentLogicalPage = 0;
    public:
        uint64_t currLogicalId = 0;
        PageDirectory(const std::filesystem::path& selectedDBPath, const std::filesystem::path& selectedTablePath);
        void loadPageDirectory() ;
        StorageEngine::PDEntry lookUpPage(uint64_t) const;
        void updateOnInsert(const uint16_t);
        void updateOnDelete(const uint64_t logicalPage,const uint16_t) const;
        void savePageDirectory(bool forcedSave);
        void lookIntoPDMap()const;

    private:
        bool findFreeSpace(const uint16_t, uint64_t&) const;
        void serialize() override;
        std::vector<PDEntry> deserialize() override;
    private:
        void setCurrentLogicalPage(std::vector<PDEntry>& entries)
        {
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
