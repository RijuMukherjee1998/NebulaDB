#include <gtest/gtest.h>

#include <algorithm>
#include <memory>
#include <utility>
#include <vector>

#include "../headers/BPlusTree.h"

TEST(BPlusTreeTest, InsertAndPointSearch)
{
    BPlusTree<int, int, 4> bPlusTree;

    for (int i = 1; i <= 100; ++i) {
        bPlusTree.insert(i, i * 10);
    }

    bool found = false;
    std::unique_ptr<int> value = bPlusTree.searchKey(42, found);
    ASSERT_TRUE(found);
    ASSERT_NE(value, nullptr);
    EXPECT_EQ(*value, 420);

    value = bPlusTree.searchKey(1000, found);
    EXPECT_FALSE(found);
    EXPECT_EQ(value, nullptr);
    EXPECT_TRUE(bPlusTree.bplustreeSortedCheck());
}

TEST(BPlusTreeTest, RangeSearchReturnsSortedValuesInsideBounds)
{
    BPlusTree<int, int, 4> bPlusTree;
    for (int key : {7, 2, 9, 1, 5, 3, 8, 4, 6}) {
        bPlusTree.insert(key, key * 100);
    }

    bool found = false;
    std::unique_ptr<std::vector<int>> values = bPlusTree.searchRange(3, 6, found);

    ASSERT_TRUE(found);
    ASSERT_NE(values, nullptr);
    EXPECT_EQ(*values, (std::vector<int>{300, 400, 500, 600}));
}

TEST(BPlusTreeTest, DeleteEntryRemovesOnlyMatchingDuplicateValue)
{
    BPlusTree<int, int, 4> bPlusTree;
    for (int value = 0; value < 20; ++value) {
        bPlusTree.insert(5, value);
    }
    bPlusTree.insert(4, 400);
    bPlusTree.insert(6, 600);

    bool deleted = false;
    bPlusTree.deleteEntry({5, 13}, deleted);
    ASSERT_TRUE(deleted);

    bool found = false;
    std::unique_ptr<std::vector<int>> values = bPlusTree.searchRange(5, 5, found);
    ASSERT_TRUE(found);
    ASSERT_NE(values, nullptr);
    EXPECT_EQ(values->size(), 19U);
    EXPECT_EQ(std::count(values->begin(), values->end(), 13), 0);
    EXPECT_EQ(std::count(values->begin(), values->end(), 12), 1);
    EXPECT_EQ(std::count(values->begin(), values->end(), 14), 1);
}

TEST(BPlusTreeTest, DeleteRangeEntriesCountsOnlyDeletedEntries)
{
    BPlusTree<int, int, 4> bPlusTree;
    bPlusTree.insert(10, 1);
    bPlusTree.insert(10, 2);
    bPlusTree.insert(10, 3);

    std::vector<std::pair<int, int>> entries{{10, 2}, {10, 99}};
    size_t changedRows = 0;
    bPlusTree.deleteRangeEntries(&entries, changedRows);

    EXPECT_EQ(changedRows, 1U);

    bool found = false;
    std::unique_ptr<std::vector<int>> values = bPlusTree.searchRange(10, 10, found);
    ASSERT_TRUE(found);
    ASSERT_NE(values, nullptr);
    EXPECT_EQ(values->size(), 2U);
    EXPECT_EQ(std::count(values->begin(), values->end(), 1), 1);
    EXPECT_EQ(std::count(values->begin(), values->end(), 3), 1);
}
