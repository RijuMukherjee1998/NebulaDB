//
// Created by Riju Mukherjee on 16-02-2025.
//

#include <gtest/gtest.h>
#include "../headers/DBManager.h"

Manager::DBManager testDBManager;
std::string db_name = "testdb";
std::string db_name_1 = "testdb1";
std::string tbl_name = "test";

TEST(DBMANAGER_FUNC, CREATE_DB)
{
   EXPECT_NO_THROW(testDBManager.createDB(&db_name));
   EXPECT_NO_THROW(testDBManager.createDB(&db_name_1));
}
TEST(DBMANAGER_FUNC, SHOW_ALL_DB)
{
   EXPECT_NO_THROW(testDBManager.showAllDB());
}
TEST(DBMANAGER_FUNC, SELECT_DB)
{
   EXPECT_NO_THROW(testDBManager.selectDB(&db_name));
   EXPECT_EQ(testDBManager.getCurrSelectedDBPath().string(), "C:\\ndb\\testdb");
}
TEST(DBMANAGER_FUNC, CREATE_TABLE)
{
   Schema testSchema(tbl_name, {
            {"id", DataType::INT, true, false},
            {"name", DataType::STRING, false, false},
            {"age", DataType::INT, false, false},
        });
   EXPECT_NO_THROW(testDBManager.createTable(&tbl_name, &testSchema));
}
TEST(DBMANAGER_FUNC, SHOW_ALL_TABLES)
{
  EXPECT_NO_THROW(testDBManager.showAllTables());
}
TEST(DBMANAGER_FUNC, SELECT_TABLE)
{
   EXPECT_NO_THROW(testDBManager.selectTable(&tbl_name));
   EXPECT_EQ(testDBManager.getCurrSelectedTablePath().string(), "C:\\ndb\\testdb\\test");
}
TEST(DBMANAGER_FUNC, DELETE_DB)
{
   testDBManager.deleteDB(&db_name_1);
}