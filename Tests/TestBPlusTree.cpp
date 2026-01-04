//
// Created by Riju Mukherjee on 11-05-2025.
//

#include <random>
#include <gtest/gtest.h>
#include  "../headers/BPlusTree.h"



TEST(BPLUSTREE_TEST, BPLUSTREE_INSERT_INCREMENTAL)
{
    BPlusTree<int,int,4> bPlusTree;
    int i = 1;
    while (i < 500000)
    {
        ASSERT_NO_THROW(bPlusTree.insert(i,i*100));
        i++;
    }
    ASSERT_EQ(bPlusTree.bplustreeSortedCheck(), true);
    //bPlusTree.print(); // Too big ---- print wont make sense
}

TEST(BPLUSTREE_TEST, BPLUSTREE_INSERT_RANDOM_CONTROLLED)
{
    BPlusTree<int,int,4> bPlusTree;
    ASSERT_NO_THROW(bPlusTree.insert(5,0));
    ASSERT_NO_THROW(bPlusTree.insert(3,0));
    ASSERT_NO_THROW(bPlusTree.insert(7,0));
    ASSERT_NO_THROW(bPlusTree.insert(2,0));
    ASSERT_NO_THROW(bPlusTree.insert(1,0));
    ASSERT_NO_THROW(bPlusTree.insert(8,0));
    ASSERT_NO_THROW(bPlusTree.insert(9,0));
    ASSERT_NO_THROW(bPlusTree.insert(4,0));
    ASSERT_NO_THROW(bPlusTree.insert(0,0));
    ASSERT_NO_THROW(bPlusTree.insert(11, 0));
    ASSERT_NO_THROW(bPlusTree.insert(10, 0));
    ASSERT_NO_THROW(bPlusTree.insert(12, 0));
    ASSERT_EQ(bPlusTree.bplustreeSortedCheck(), true);
    bPlusTree.print();
}
TEST(BPLUSTREE_TEST, BPLUSTREE_INSERT_RANDOM_GEN)
{
    BPlusTree<int,int,4> bPlusTree;
    // random insert
    std::random_device device;
    std::mt19937 generator(device());
    std::uniform_int_distribution<int> distribution(1,3500);
    int i = 1;
    while (i <= 3000)
    {
        ASSERT_NO_THROW(bPlusTree.insert(distribution(generator),i*10));
        i++;
    }
    bPlusTree.print();
    ASSERT_EQ(bPlusTree.bplustreeSortedCheck(), true);
}

TEST(BPLUSTREE_TEST, BPLUSTREE_SINGLE_SEARCH)
{
    BPlusTree<int,int,4> bPlusTree;
    ASSERT_NO_THROW(bPlusTree.insert(5,0));
    ASSERT_NO_THROW(bPlusTree.insert(3,0));
    ASSERT_NO_THROW(bPlusTree.insert(7,0));
    ASSERT_NO_THROW(bPlusTree.insert(2,0));
    ASSERT_NO_THROW(bPlusTree.insert(1,0));
    ASSERT_NO_THROW(bPlusTree.insert(8,0));
    ASSERT_NO_THROW(bPlusTree.insert(9,0));
    ASSERT_NO_THROW(bPlusTree.insert(4,0));
    ASSERT_NO_THROW(bPlusTree.insert(0,0));
    bPlusTree.print();
    bool found = false;
    GTEST_ASSERT_NE(bPlusTree.searchKey(5,found),nullptr);
    GTEST_ASSERT_NE(bPlusTree.searchKey(3,found),nullptr);
    GTEST_ASSERT_NE(bPlusTree.searchKey(7,found),nullptr);
    GTEST_ASSERT_NE(bPlusTree.searchKey(2,found),nullptr);
    ASSERT_EQ(bPlusTree.searchKey(10,found),nullptr);
    ASSERT_EQ(bPlusTree.searchKey(6,found),nullptr);
    ASSERT_EQ(bPlusTree.bplustreeSortedCheck(), true);
}

TEST(BPLUSTREE_TEST, BPLUSTREE_SEARCH_RANGE)
{
    BPlusTree<int,int,4> bPlusTree;
    ASSERT_NO_THROW(bPlusTree.insert(5,50));
    ASSERT_NO_THROW(bPlusTree.insert(3,30));
    ASSERT_NO_THROW(bPlusTree.insert(7,70));
    ASSERT_NO_THROW(bPlusTree.insert(2,20));
    ASSERT_NO_THROW(bPlusTree.insert(1,10));
    ASSERT_NO_THROW(bPlusTree.insert(8,80));
    ASSERT_NO_THROW(bPlusTree.insert(9,90));
    ASSERT_NO_THROW(bPlusTree.insert(4,40));
    ASSERT_NO_THROW(bPlusTree.insert(0,0));
    bPlusTree.print();
    bool found = false;
    std::unique_ptr<std::vector<int>> allValues = bPlusTree.searchRange(4,8,found);
    ASSERT_EQ(allValues.get()->size(),4);
    allValues = bPlusTree.searchRange(3,6,found);
    ASSERT_EQ(allValues.get()->size(),3);
    allValues = bPlusTree.searchRange(10,12,found);
    ASSERT_EQ(allValues.get()->size(),0);
    ASSERT_EQ(bPlusTree.bplustreeSortedCheck(), true);
}

TEST(BPLUSTREE_TEST, BPLUSTREE_DELETE)
{
    BPlusTree<int,int,4> bPlusTree;
    ASSERT_NO_THROW(bPlusTree.insert(5,50));
    ASSERT_NO_THROW(bPlusTree.insert(3,30));
    ASSERT_NO_THROW(bPlusTree.insert(7,70));
    ASSERT_NO_THROW(bPlusTree.insert(2,20));
    ASSERT_NO_THROW(bPlusTree.insert(1,10));
    ASSERT_NO_THROW(bPlusTree.insert(8,80));
    ASSERT_NO_THROW(bPlusTree.insert(9,90));
    ASSERT_NO_THROW(bPlusTree.insert(4,40));
    ASSERT_NO_THROW(bPlusTree.insert(0,0));
    ASSERT_NO_THROW(bPlusTree.insert(11,110));
    ASSERT_NO_THROW(bPlusTree.insert(10,100));
    ASSERT_NO_THROW(bPlusTree.insert(12,120));
    ASSERT_NO_THROW(bPlusTree.insert(13,130));
    ASSERT_NO_THROW(bPlusTree.insert(15,150));
    ASSERT_NO_THROW(bPlusTree.insert(14,140));
    ASSERT_NO_THROW(bPlusTree.insert(16,160));
    bPlusTree.print();
    bool found = false;
    ASSERT_NO_THROW(bPlusTree.deleteKey(5,found));
    bPlusTree.print();
    bPlusTree.searchKey(5,found);
    ASSERT_EQ(found,false);
    ASSERT_NO_THROW(bPlusTree.deleteKey(8,found));
    ASSERT_EQ(bPlusTree.searchKey(8,found),nullptr);
    ASSERT_NO_THROW(bPlusTree.deleteKey(12,found));
    ASSERT_EQ(bPlusTree.searchKey(12,found),nullptr);
    bPlusTree.print();
    ASSERT_EQ(bPlusTree.bplustreeSortedCheck(), true);
}



