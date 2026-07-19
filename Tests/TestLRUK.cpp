#include <gtest/gtest.h>

#include "../headers/LRUK.h"

TEST(LRUKTest, EvictsPagesWithOldestKDistanceFirst)
{
    LRU_K lru(2);
    uint64_t timestamp = 0;
    for (uint64_t logical_id = 0; logical_id <= 100; ++logical_id) {
        lru.accessPage(logical_id, ++timestamp);
    }

    lru.accessPage(100, ++timestamp);
    lru.accessPage(50, ++timestamp);
    lru.accessPage(25, ++timestamp);
    lru.accessPage(0, ++timestamp);
    lru.accessPage(1, ++timestamp);
    lru.accessPage(2, ++timestamp);

    uint64_t evicted_page = lru.evictPage();
    EXPECT_EQ(evicted_page, 99);
    evicted_page = lru.evictPage();
    EXPECT_EQ(evicted_page, 98);
    evicted_page = lru.evictPage();
    EXPECT_EQ(evicted_page, 97);
}

TEST(LRUKTest, EmptyReplacerReturnsSentinel)
{
    LRU_K lru(2);

    EXPECT_EQ(lru.evictPage(), UINT64_MAX);
}
