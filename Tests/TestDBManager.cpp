#include <gtest/gtest.h>

#include <filesystem>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "../headers/DBManager.h"

namespace {

constexpr const char* kBasePath = "/var/tmp/ndb";

Schema makeTestSchema(const std::string& table_name)
{
   return Schema(table_name, {
      Column{1, "id", DataType::INT, true, false, false},
      Column{2, "name", DataType::STRING, false, false, false},
      Column{3, "age", DataType::INT, false, false, false},
      Column{4, "aadhar_id", DataType::STRING, true, false, false}
   });
}

InternalQuery::TableQuery makeInsertQuery(
   const std::string& table_name,
   const int id,
   const std::string& name,
   const int age,
   const std::string& aadhar_id)
{
   auto query = std::make_unique<InternalQuery::InsertQuery>();
   query->InternalQuery::Query::qtype = InternalQuery::QueryType::INSERT_QUERY;
   query->values = {id, name, age, aadhar_id};

   InternalQuery::TableQuery table_query;
   table_query.table_name = table_name;
   table_query.type = InternalQuery::TableQuery::TableQueryType::INSERT;
   table_query.query = std::move(query);
   return table_query;
}

InternalQuery::TableQuery makeIndexQuery(const std::string& table_name, const uint16_t col_id)
{
   auto query = std::make_unique<InternalQuery::IndexQuery>();
   query->InternalQuery::Query::qtype = InternalQuery::QueryType::INDEX_QUERY;
   query->col_id = col_id;

   InternalQuery::TableQuery table_query;
   table_query.table_name = table_name;
   table_query.type = InternalQuery::TableQuery::TableQueryType::INDEX_COL;
   table_query.query = std::move(query);
   return table_query;
}

InternalQuery::Condition equalsCondition(const uint16_t col_id, const variant_data_t& value)
{
   InternalQuery::Condition condition;
   condition.col_idx = col_id;
   condition.op = InternalQuery::Conditions::EQUAL;
   condition.fil_type = InternalQuery::Condition::Filtype::SINGLE_VALUE;
   condition.value = value;
   return condition;
}

InternalQuery::TableQuery makeSelectQuery(
   const std::string& table_name,
   const InternalQuery::Condition& condition,
   std::vector<uint16_t> projection = {})
{
   auto query = std::make_unique<InternalQuery::SelectQuery>();
   query->InternalQuery::Query::qtype = InternalQuery::QueryType::SELECT_QUERY;
   query->predicate.and_groups.push_back(InternalQuery::AndQuery{{condition}});
   query->projection = std::move(projection);

   InternalQuery::TableQuery table_query;
   table_query.table_name = table_name;
   table_query.type = InternalQuery::TableQuery::TableQueryType::SELECT;
   table_query.query = std::move(query);
   return table_query;
}

InternalQuery::TableQuery makeUpdateQuery(
   const std::string& table_name,
   const InternalQuery::Condition& condition,
   std::unordered_map<uint16_t, variant_data_t> updates)
{
   auto query = std::make_unique<InternalQuery::UpdateQuery>();
   query->InternalQuery::Query::qtype = InternalQuery::QueryType::UPDATE_QUERY;
   query->predicate.and_groups.push_back(InternalQuery::AndQuery{{condition}});
   query->updates = std::move(updates);

   InternalQuery::TableQuery table_query;
   table_query.table_name = table_name;
   table_query.type = InternalQuery::TableQuery::TableQueryType::UPDATE;
   table_query.query = std::move(query);
   return table_query;
}

InternalQuery::TableQuery makeDeleteQuery(const std::string& table_name, const InternalQuery::Condition& condition)
{
   auto query = std::make_unique<InternalQuery::DeleteQuery>();
   query->InternalQuery::Query::qtype = InternalQuery::QueryType::DELETE_QUERY;
   query->predicate.and_groups.push_back(InternalQuery::AndQuery{{condition}});

   InternalQuery::TableQuery table_query;
   table_query.table_name = table_name;
   table_query.type = InternalQuery::TableQuery::TableQueryType::DELETE;
   table_query.query = std::move(query);
   return table_query;
}

class DBManagerTest : public ::testing::Test {
protected:
   std::string db_name;
   std::string table_name;
   std::filesystem::path db_path;

   void SetUp() override
   {
      std::filesystem::create_directories(kBasePath);
      db_name = std::string("testdb_") + ::testing::UnitTest::GetInstance()->current_test_info()->name();
      table_name = "people";
      db_path = std::filesystem::path(kBasePath) / db_name;
      std::filesystem::remove_all(db_path);
   }

   void TearDown() override
   {
      std::filesystem::remove_all(db_path);
   }
};

} // namespace

TEST_F(DBManagerTest, CreatesSelectsAndDeletesDatabase)
{
   Manager::DBManager dbmanager;

   ASSERT_NO_THROW(dbmanager.createDB(&db_name));
   ASSERT_TRUE(std::filesystem::exists(db_path));

   ASSERT_NO_THROW(dbmanager.selectDB(&db_name));
   EXPECT_EQ(dbmanager.getCurrSelectedDBPath(), db_path);

   ASSERT_NO_THROW(dbmanager.deleteDB(&db_name));
   EXPECT_FALSE(std::filesystem::exists(db_path));
}

TEST_F(DBManagerTest, ExecutesTableLifecycleAndKeepsSchemaAlive)
{
   Manager::DBManager dbmanager;
   dbmanager.createDB(&db_name);
   dbmanager.selectDB(&db_name);

   {
      Schema schema = makeTestSchema(table_name);
      ASSERT_NO_THROW(dbmanager.createTable(&table_name, &schema));
   }

   std::vector<InternalQuery::TableQuery> inserts;
   inserts.push_back(makeInsertQuery(table_name, 1, "A", 25, "111111111111"));
   inserts.push_back(makeInsertQuery(table_name, 2, "B", 25, "222222222222"));
   inserts.push_back(makeInsertQuery(table_name, 3, "C", 28, "333333333333"));

   for (auto& insert : inserts) {
      ASSERT_NO_THROW(dbmanager.executeQueryOnTable(insert));
   }

   InternalQuery::TableQuery index_age = makeIndexQuery(table_name, 3);
   ASSERT_NO_THROW(dbmanager.executeQueryOnTable(index_age));

   InternalQuery::TableQuery select_age_25 = makeSelectQuery(table_name, equalsCondition(3, 25), {3, 4});
   ASSERT_NO_THROW(dbmanager.executeQueryOnTable(select_age_25));

   InternalQuery::TableQuery update_age = makeUpdateQuery(table_name, equalsCondition(3, 25), {{3, 28}});
   ASSERT_NO_THROW(dbmanager.executeQueryOnTable(update_age));

   InternalQuery::TableQuery delete_age_28 = makeDeleteQuery(table_name, equalsCondition(3, 28));
   ASSERT_NO_THROW(dbmanager.executeQueryOnTable(delete_age_28));

   InternalQuery::TableQuery select_after_delete = makeSelectQuery(table_name, equalsCondition(3, 28), {3, 4});
   ASSERT_NO_THROW(dbmanager.executeQueryOnTable(select_after_delete));

   EXPECT_NO_THROW(dbmanager.shutdownDB());
   EXPECT_TRUE(std::filesystem::exists(db_path / table_name / (table_name + ".json")));
}
