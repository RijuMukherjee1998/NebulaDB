//
// Created by Riju Mukherjee on 16-02-2025.
//

#include <gtest/gtest.h>
#include "../headers/DBManager.h"

Manager::DBManager testDBManager;
std::string db_name = "testdb";
std::string db_name_1 = "testdb1";
std::string tbl_name = "test";

class DBManagerTest : public ::testing::Test {
protected:
   Manager::DBManager testDBManager;
   std::string db_name = "testdb";
   std::string tbl_name = "test";

   Schema testSchema = Schema(tbl_name, {
       {"id", DataType::INT, true, false},
       {"name", DataType::STRING, false, false},
       {"age", DataType::INT, false, false}
   });

   void SetUp() override {
      testDBManager.createDB(&db_name);
      testDBManager.selectDB(&db_name);
      testDBManager.createTable(&tbl_name, &testSchema);
      testDBManager.selectTable(&tbl_name);
   }

   void TearDown() override {
      testDBManager.shutdownDB();
   }
};
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

int id = 1;
std::string name = "test1";
int age = 18;
Schema testSchema(tbl_name, {
        {"id", DataType::INT, true, false},
        {"name", DataType::STRING, false, false},
        {"age", DataType::INT, false, false},
    });
TEST(TABLE_FUNC, INSERT_INTO_DB)
{
   EXPECT_NO_THROW(testDBManager.createDB(&db_name));
   EXPECT_NO_THROW(testDBManager.showAllDB());
   EXPECT_NO_THROW(testDBManager.selectDB(&db_name));
   EXPECT_EQ(testDBManager.getCurrSelectedDBPath().string(), "C:\\ndb\\testdb");

   EXPECT_NO_THROW(testDBManager.createTable(&tbl_name, &testSchema));
   EXPECT_NO_THROW(testDBManager.showAllTables());
   EXPECT_NO_THROW(testDBManager.selectTable(&tbl_name));
   EXPECT_EQ(testDBManager.getCurrSelectedTablePath().string(), "C:\\ndb\\testdb\\test");
   std::vector<Column> testColumns;
   Column ID;
   ID.col_name = "id";
   ID.col_type = DataType::INT;
   ID.col_value = id;
   ID.is_primary_key = true;
   ID.is_null = false;
   Column NAME;
   NAME.col_name = "name";
   NAME.col_type = DataType::STRING;
   NAME.col_value = name;
   Column AGE;
   AGE.col_name = "age";
   AGE.col_type = DataType::INT;
   AGE.col_value = age;
   testColumns.push_back(ID);
   testColumns.push_back(NAME);
   testColumns.push_back(AGE);
   EXPECT_NO_THROW(testDBManager.insertIntoSelectedTable(testColumns));
   EXPECT_NO_THROW(testDBManager.shutdownDB());
}

TEST(TABLE_FUNC, SELECTALLFROMTABLE)
{
   Schema sch = testSchema;
   EXPECT_NO_THROW(testDBManager.selectAllFromSelectedTable());
}