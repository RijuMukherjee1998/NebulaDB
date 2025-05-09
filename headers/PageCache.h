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
#include "LRUK.h"

// Forward declaration to break circular dependency
class LRU_K;

namespace StorageEngine
{
    class PageCache {
        std::recursive_mutex pg_cache_mtx;
        Utils::Logger* logger;
        std::shared_ptr<std::unordered_map<uint64_t, std::shared_ptr<StorageEngine::Page>>> page_cache;
        PageDirectory* pageDirectory;
        const std::filesystem::path& currTablePath;
        uint16_t dirty_page_count = 0;
        std::shared_ptr<DiskManager> disk_manager;
        std::unique_ptr<LRU_K> lru_k;
        uint64_t time_stamp = 0;
        void pinPage(const uint64_t& logicalId);
        void loadPageIntoCache(uint64_t logical_id);
        void updateLRU(uint64_t);

    public:
        PageCache(const std::filesystem::path& currTablePath, PageDirectory* pageDirectory);
        std::shared_ptr<Page> getPageFromCache(uint64_t logical_id);
        void flushDirtyPages();
        void markPageDirty(uint64_t logical_id);
        void unPinPage(const uint64_t& logical_id);

    };
}


#endif //PAGECACHE_H
