//
// Created by Riju Mukherjee on 8/16/26.
//

#ifndef NEBULADB_PAGEGUARD_H
#define NEBULADB_PAGEGUARD_H

#include <mutex>
#include <shared_mutex>

#include "InternalStructs.h"
#include "Page.h"

namespace StorageEngine {
    class PageCache;
}

namespace StorageEngine {

    class PageGuard {
    public:
        PageGuard(
            PageCache& pg_cache,
            std::shared_ptr<Page>& page,
            PAGE_ID_TYPE pg_id,
            std::shared_mutex& latch,
            PAGE_MODE pg_mode);

        ~PageGuard();
        StorageEngine::Page & operator*() {
            return *_page;
        }
        StorageEngine::Page* operator->() {
            return _page.get();
        }
        std::shared_ptr<Page> getPage() {
            return _page;
        }
        PageGuard() = delete;
        PageGuard(const PageGuard&) = delete;
        PageGuard& operator=(const PageGuard&) = delete;
    private:
        Utils::Logger* logger = nullptr;
        std::shared_ptr<StorageEngine::Page>& _page;
        PAGE_ID_TYPE _id;
        PageCache& _cache;
        std::shared_mutex& rw_latch_mtx;
        std::shared_lock<std::shared_mutex> read_lock;
        std::unique_lock<std::shared_mutex> write_lock;

    };
}
#endif //NEBULADB_PAGEGUARD_H