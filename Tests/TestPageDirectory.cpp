#include <gtest/gtest.h>

#include <filesystem>
#include <string>

#include "../headers/PageDirectory.h"

namespace {

std::filesystem::path makeTablePath(const std::string& name)
{
    const std::filesystem::path db_path = std::filesystem::path("/var/tmp/ndb") / name;
    const std::filesystem::path table_path = db_path / "table";
    std::filesystem::remove_all(db_path);
    std::filesystem::create_directories(table_path);
    return table_path;
}

} // namespace

TEST(PageDirectoryTest, CreatesPageDirectoryForNewTable)
{
    const std::filesystem::path table_path = makeTablePath("pd_create_test");
    const std::filesystem::path db_path = table_path.parent_path();
    StorageEngine::PageDirectory page_directory(db_path, table_path);

    ASSERT_NO_THROW(page_directory.loadPageDirectory());

    const StorageEngine::PDEntry entry = page_directory.lookUpPage(0);
    EXPECT_TRUE(entry.exists);
    EXPECT_EQ(entry.logicalPage, 0U);
    EXPECT_EQ(entry.fileId, 0U);
    EXPECT_EQ(entry.pageOffset, 0U);
}

TEST(PageDirectoryTest, InsertAllocatesNewLogicalPagesWhenFull)
{
    const std::filesystem::path table_path = makeTablePath("pd_insert_test");
    const std::filesystem::path db_path = table_path.parent_path();
    StorageEngine::PageDirectory page_directory(db_path, table_path);
    page_directory.loadPageDirectory();

    constexpr uint16_t row_size = PAGE_SIZE - sizeof(StorageEngine::PageHeader) - sizeof(StorageEngine::Slot);
    page_directory.updateOnInsert(row_size);
    page_directory.updateOnInsert(row_size);

    EXPECT_GE(StorageEngine::PageDirectory::getCurrentLogicalPage(), 1U);
    EXPECT_TRUE(page_directory.lookUpPage(1).exists);
}

TEST(PageDirectoryTest, SaveAndReloadPreservesEntries)
{
    const std::filesystem::path table_path = makeTablePath("pd_reload_test");
    const std::filesystem::path db_path = table_path.parent_path();

    {
        StorageEngine::PageDirectory page_directory(db_path, table_path);
        page_directory.loadPageDirectory();
        page_directory.updateOnInsert(128);
        page_directory.savePageDirectory(true);
    }

    StorageEngine::PageDirectory reloaded(db_path, table_path);
    ASSERT_NO_THROW(reloaded.loadPageDirectory());

    const StorageEngine::PDEntry entry = reloaded.lookUpPage(0);
    EXPECT_TRUE(entry.exists);
    EXPECT_LT(entry.freeSpace, PAGE_SIZE - sizeof(StorageEngine::PageHeader));
}

TEST(PageDirectoryTest, DeleteReturnsSpaceToExistingEntry)
{
    const std::filesystem::path table_path = makeTablePath("pd_delete_test");
    const std::filesystem::path db_path = table_path.parent_path();
    StorageEngine::PageDirectory page_directory(db_path, table_path);
    page_directory.loadPageDirectory();

    page_directory.updateOnInsert(128);
    const uint16_t after_insert = page_directory.lookUpPage(0).freeSpace;
    page_directory.updateOnDelete(0, 128);

    EXPECT_GT(page_directory.lookUpPage(0).freeSpace, after_insert);
}
