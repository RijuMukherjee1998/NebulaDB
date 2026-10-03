//
// Created by Riju Mukherjee on 26-02-2025.
//

#ifndef PAGECACHE_H
#define PAGECACHE_H

#include <deque>
#include <shared_mutex>

#include "Page.h"
#include "PageDirectory.h"
#include "Logger.h"
#include "DiskManager.h"
#include "LRUK.h"
#include "constants.h"
#include "PageGuard.h"

// Forward declaration to break circular dependency
class LRU_K;

namespace StorageEngine
{
    class PageCache {
    private:
        static PageCache* pg_cache_instance;
        static std::recursive_mutex instance_cache_mtx;
        std::recursive_mutex pg_cache_mtx;
        static Utils::Logger* logger;
        std::shared_ptr<std::unordered_map<uint64_t, std::shared_ptr<StorageEngine::Page>>> page_cache;
        PageDirectory* pageDirectory;
        const std::filesystem::path& currTablePath;
        uint16_t dirty_page_count = 0; /* Note this indicates total no of dirty pages in the page cache. */
        std::shared_ptr<DiskManager> disk_manager;
        std::unique_ptr<LRU_K> lru_k;
        uint64_t time_stamp = 0;
        std::array<std::recursive_mutex,PG_CACHE_NUM_LATCHES> pg_cache_latches;
        std::array<std::shared_mutex,PG_CACHE_NUM_LATCHES> pg_rw_latches;
        void pinPage(const uint64_t& logicalId);
        void loadPageIntoCache(uint64_t logical_id);
        void updateLRU(uint64_t);
        PageCache(const std::filesystem::path& currTablePath, PageDirectory* pageDirectory);
    public:
        static PageCache* getNonNullInstance();
        static PageCache* getInstance(const std::filesystem::path& currTablePath, PageDirectory* pageDirectory);
        std::unique_ptr<PageGuard> getPageFromCache(uint64_t logical_id, PAGE_MODE pg_mode);
        void flushDirtyPages();
        void markPageDirty(uint64_t logical_id);
        void unPinPage(const uint64_t& logical_id);

    };
}


#endif //PAGECACHE_H
