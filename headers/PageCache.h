//
// Created by Riju Mukherjee on 26-02-2025.
//

#ifndef PAGECACHE_H
#define PAGECACHE_H

#include <deque>

#include "Page.h"
#include "PageDirectory.h"
#include "Logger.h"
#include "DiskManager.h"

namespace StorageEngine
{
    class PageCache {
        std::recursive_mutex cache_mtx;
        Utils::Logger* logger;
        std::unique_ptr<std::unordered_map<uint64_t, std::shared_ptr<StorageEngine::Page>>> page_cache;
        PageDirectory* pageDirectory;
        std::unique_ptr<std::deque<uint64_t>> lru_list{};
        const std::filesystem::path& currTablePath;
        uint16_t dirty_page_count = 0;
        std::unique_ptr<DiskManager> disk_manager;
        void loadPageIntoCache(uint64_t logical_id) const;

        void updateLRU(uint64_t) const;

    public:
        PageCache(const std::filesystem::path& currTablePath, PageDirectory* pageDirectory);
        std::shared_ptr<Page> getPageFromCache(uint64_t logical_id) const;
        void flushDirtyPages();
        void markPageDirty(uint64_t logical_id);

    };
}


#endif //PAGECACHE_H
