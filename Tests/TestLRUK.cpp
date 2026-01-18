//
// Created by Riju Mukherjee on 02-05-2025.
//

#include <gtest/gtest.h>
#include "../headers/LRUK.h"
#include "../headers/PageCache.h"

LRU_K lru_k(2);
TEST(LRU_TEST, LRU_ACCESS_EVICT_PAGE)
{
    uint64_t logical_id = 0;
    uint64_t timeStamp  = 0;
    while (logical_id != 101)
    {
        lru_k.accessPage(logical_id++,++timeStamp);
    }
    lru_k.accessPage(100,++timeStamp);
    lru_k.accessPage(50,++timeStamp);
    lru_k.accessPage(25,++timeStamp);
    lru_k.accessPage(0,++timeStamp);
    lru_k.accessPage(1,++timeStamp);
    lru_k.accessPage(2,++timeStamp);

    uint64_t evicted_page = lru_k.evictPage();
    EXPECT_EQ(evicted_page, 99);
    evicted_page = lru_k.evictPage();
    EXPECT_EQ(evicted_page, 98);
    evicted_page = lru_k.evictPage();
    EXPECT_EQ(evicted_page, 97);
}