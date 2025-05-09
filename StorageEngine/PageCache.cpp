//
// Created by Riju Mukherjee on 26-02-2025.
//

#include <utility>

#include "../headers/PageCache.h"
#include "../headers/DiskManager.h"
#include "../headers/ThreadPool.h"
namespace StorageEngine
{
    PageCache::PageCache(const std::filesystem::path& currTablePath, PageDirectory* pageDirectory): currTablePath(
        currTablePath), lru_k(std::make_unique<LRU_K>(2))
    {
        this->pageDirectory = pageDirectory;
        page_cache = std::make_shared<std::unordered_map<uint64_t, std::shared_ptr<StorageEngine::Page>>>();
        logger = Utils::Logger::getInstance();
        disk_manager = std::make_shared<DiskManager>(currTablePath);
    }
    void StorageEngine::PageCache::pinPage(const uint64_t& logicalId)
    {
        std::lock_guard<std::recursive_mutex> cache_lock(pg_cache_mtx);
        if (page_cache->find(logicalId) == page_cache->end())
        {
            logger->logCritical({"Pin Failed -> Page can't be found in cache with logical id = ", std::to_string(logicalId)});
        }
        (*page_cache)[logicalId]->pin_count += 1;
    }
    void StorageEngine::PageCache::unPinPage(const uint64_t& logicalId)
    {
        std::lock_guard<std::recursive_mutex> cache_lock(pg_cache_mtx);
        if (page_cache->find(logicalId) == page_cache->end())
        {
            logger->logCritical({"Unpin Failed -> Page can't be found in cache with logical id = ", std::to_string(logicalId)});
        }
        (*page_cache)[logicalId]->pin_count -= 1;
    }
    std::shared_ptr<Page> PageCache::getPageFromCache(const uint64_t logical_id)
    {
        if (page_cache->empty() || page_cache->find(logical_id) == page_cache->end())
        {
            loadPageIntoCache(logical_id);
        }
        pinPage(logical_id);
        updateLRU(logical_id);
        if (page_cache->find(logical_id) == page_cache->end())
            logger->logCritical({"Page can't be found in cache with logical id = ", std::to_string(logical_id)});
        std::shared_ptr<Page> page= (*page_cache)[logical_id];
        return page;
    }

    void StorageEngine::PageCache::loadPageIntoCache(uint64_t logical_id)
    {
        const PDEntry pde =  pageDirectory->lookUpPage(logical_id);
        std::shared_ptr<Page> page = nullptr;
        if (pde.exists == true)
        {
            page = disk_manager->readPageFromDisk(pde.fileId, pde.pageOffset);
            if (page == nullptr)
            {
                page = std::make_shared<Page>();
                //disk_manager->writePageToDisk(pde.fileId, pde.pageOffset, page);
            }
        }
        else
        {
            page = std::make_shared<Page>();
            logger->logWarn({"Creating a new page for logical id = ", std::to_string(logical_id)});
        }
        std::lock_guard<std::recursive_mutex> cache_lock(pg_cache_mtx);
        page_cache->emplace(logical_id, std::move(page));
    }

    void PageCache::markPageDirty(uint64_t logical_id)
    {
        if (page_cache->find(logical_id) == page_cache->end())
        {
            logger->logWarn({"Page can't be marked dirty Weird, page dosen't exists in cache"});
            return;
        }
        dirty_page_count += 1;
        (*page_cache)[logical_id]->dirty = true;

        if (dirty_page_count >= DIRTY_PAGE_TOLERANCE)
        {
            flushDirtyPages();
            dirty_page_count = 0;
        }
    }

    void PageCache::flushDirtyPages()
    {
        std::lock_guard<std::recursive_mutex> cache_lock(pg_cache_mtx);
        for (const auto& it : *page_cache)
        {
            if (it.second != nullptr && it.second->dirty)
            {
                it.second->dirty = false;
                const PDEntry pde =  pageDirectory->lookUpPage(it.first);
                disk_manager->writePageToDisk(pde.fileId, pde.pageOffset, it.second);
            }
        }
    }

    void StorageEngine::PageCache::updateLRU(const uint64_t logical_id)
    {
        time_stamp += 1;
        lru_k->accessPage(logical_id, time_stamp);
        if (page_cache->size() == MAX_PAGES_IN_CACHE)
        {
            std::lock_guard<std::recursive_mutex> cache_lock(pg_cache_mtx);
            const uint64_t lId = lru_k->evictPage(page_cache);
            if (lId == UINT64_MAX)
                logger->logCritical({"Weird LRU-K dosen't have the page"});
            std::shared_ptr<Page> page_copy = nullptr;
            if (page_cache->find(lId) != page_cache->end()) {
                const auto it = page_cache->find(lId);
                page_copy = it->second;
                page_cache->erase(it);
            }
            if (page_copy != nullptr && page_copy->dirty)
            {
                logger->logInfo({"Found page ... Going for a write"});
                const PDEntry pde =  pageDirectory->lookUpPage(lId);
                auto disk_manager_copy = disk_manager;
                ThreadPool::getInstance()->enqueue([disk_manager_copy, page_copy, pde]
                {
                    disk_manager_copy->writePageToDisk(pde.fileId, pde.pageOffset, page_copy);
                });
            }
        }
    }
}