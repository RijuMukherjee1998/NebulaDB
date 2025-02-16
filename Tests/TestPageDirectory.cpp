//
// Created by Riju Mukherjee on 16-02-2025.
//
#include <gtest/gtest.h>
#include "../headers/PageDirectory.h"

std::filesystem::path db_path = "C:\\ndb\\testdb";
std::filesystem::path table_path = R"(C:\ndb\testdb\test)";
StorageEngine::PageDirectory page_directory(db_path,table_path);
uint16_t data_size = 4096;

TEST(PD_TEST, PD_LOAD_PAGE_DIR)
{
    EXPECT_NO_THROW(page_directory.loadPageDirectory());
}
TEST(PD_TEST, PD_INSERT_ENTRY)
{
    EXPECT_NO_THROW(page_directory.updateOnInsert(data_size));
    EXPECT_NO_THROW(page_directory.updateOnInsert(data_size));
    EXPECT_NO_THROW(page_directory.updateOnInsert(data_size));
    EXPECT_NO_THROW(page_directory.updateOnInsert(data_size));
    EXPECT_NO_THROW(page_directory.updateOnInsert(data_size));
    EXPECT_NO_THROW(page_directory.updateOnInsert(data_size));
}
TEST(PD_TEST, PD_DELETE_ENTRY)
{
    EXPECT_NO_THROW(page_directory.updateOnDelete(1,data_size));
}
TEST(PD_TEST, PD_SAVE_DIR)
{
    EXPECT_NO_THROW(page_directory.savePageDirectory(true));
}