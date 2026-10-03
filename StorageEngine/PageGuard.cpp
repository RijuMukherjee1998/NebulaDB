//
// Created by Riju Mukherjee on 8/29/26.
//

#include "../headers/PageGuard.h"
#include "../headers/PageCache.h"
#include "../headers/Page.h"

namespace StorageEngine {

    PageGuard::PageGuard(
        PageCache& pg_cache,
        std::shared_ptr<Page>& page,
        PAGE_ID_TYPE pg_id,
        std::shared_mutex& latch,
        PAGE_MODE pg_mode)
        :
         _page(page),
         _id(pg_id),
         _cache(pg_cache),
         rw_latch_mtx(latch)
    {
        logger = Utils::Logger::getInstance();
        if (pg_mode == PAGE_MODE::READ) {
            read_lock = std::shared_lock<std::shared_mutex>(rw_latch_mtx);
        } else if (pg_mode == PAGE_MODE::WRITE) {
            write_lock = std::unique_lock<std::shared_mutex>(rw_latch_mtx);
        } else {
            logger->logCritical({"Weird no such page mode available"});
        }
    }

    PageGuard::~PageGuard()
    {
        _cache.unPinPage(_id);
    }

}