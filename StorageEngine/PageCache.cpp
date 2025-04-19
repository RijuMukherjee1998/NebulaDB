//
// Created by Riju Mukherjee on 26-02-2025.
//

#include <utility>

#include "../headers/PageCache.h"
#include "../headers/DiskManager.h"
namespace StorageEngine
{
    PageCache::PageCache(const std::filesystem::path& currTablePath, PageDirectory* pageDirectory): currTablePath(currTablePath)
    {
        this->pageDirectory = pageDirectory;
        page_cache = std::make_unique<std::unordered_map<uint64_t, std::shared_ptr<StorageEngine::Page>>>();
        lru_list = std::make_unique<std::deque<uint64_t>>();
        logger = Utils::Logger::getInstance();
        disk_manager = std::make_unique<DiskManager>(currTablePath);
    }

    std::shared_ptr<Page> PageCache::getPageFromCache(const uint64_t logical_id) const
    {
        if (page_cache->empty() || page_cache->find(logical_id) == page_cache->end())
        {
            loadPageIntoCache(logical_id);
        }
        updateLRU(logical_id);
        return (*page_cache)[logical_id];
    }

    void StorageEngine::PageCache::loadPageIntoCache(uint64_t logical_id) const
    {
        const PDEntry pde =  pageDirectory->lookUpPage(logical_id);
        std::shared_ptr<Page> page;
        if (pde.exists == true)
        {
            page = disk_manager->readPageFromDisk(pde.fileId, pde.pageOffset);
            if (page == nullptr)
            {
                page = std::make_shared<Page>();
                disk_manager->writePageToDisk(pde.fileId, pde.pageOffset, page);
            }
            page_cache->emplace(logical_id, std::move(page));
        }
        else
        {
            logger->logCritical({"Unable to load page from disk as there is no entry"});
        }
    }

    void PageCache::markPageDirty(uint64_t logical_id)
    {
        if (page_cache->find(logical_id) == page_cache->end())
        {
            logger->logCritical({"Page can't be marked dirty Weird, page dosen't exists in cache"});
            return;
        }
        std::lock_guard<std::recursive_mutex> cache_lock(cache_mtx);
        dirty_page_count += 1;
        (*page_cache)[logical_id]->dirty = true;

        if (dirty_page_count >= DIRTY_PAGE_TOLERANCE)
            flushDirtyPages();
    }

    void PageCache::flushDirtyPages()
    {
        for (const auto& it : *page_cache)
        {
            if (it.second->dirty)
            {
                std::lock_guard<std::recursive_mutex> cache_lock(cache_mtx);
                it.second->dirty = false;
                const PDEntry pde =  pageDirectory->lookUpPage(it.first);
                disk_manager->writePageToDisk(pde.fileId, pde.pageOffset, it.second);
            }
        }

    }

    void StorageEngine::PageCache::updateLRU(const uint64_t logical_id) const
    {
        if (lru_list->empty())
        {
            lru_list->push_front(logical_id);
            return;
        }
        if (lru_list->size() == MAX_PAGES_IN_CACHE)
        {
            if (const std::shared_ptr<Page> page = page_cache->at(lru_list->back()); page->dirty)
            {
                const PDEntry pde =  pageDirectory->lookUpPage(lru_list->back());
                disk_manager->writePageToDisk(pde.fileId, pde.pageOffset, page);
            }
            page_cache->erase(lru_list->back());
            lru_list->pop_back();
            lru_list->push_front(logical_id);
            return;
        }

        if (auto elem = std::find(lru_list->begin(), lru_list->end(), logical_id); elem != lru_list->end())
        {
            lru_list->erase(elem);
            lru_list->push_front(logical_id);
        }
        else
        {
            lru_list->push_front(logical_id);
        }
    }
}