//
// Created by Riju Mukherjee on 31-01-2025.
//

#include <iostream>
#include <memory>
#include <random>
#include <string>
#include <utility>
#include <vector>
#include "headers/DBManager.h"
#include "headers/InternalQuery.h"
#include "headers/ThreadPool.h"

unsigned int random_number(uint32_t min, uint32_t max ) {
    std::random_device rd;              // Seed source
    std::mt19937 gen(rd());             // Mersenne Twister engine

    std::uniform_int_distribution<unsigned int> dist(min, max);

    return dist(gen);
}
std::string random_string(const size_t min_len, const size_t max_len, bool nums = false) {
    static const std::string chars =
        "abcdefghijklmnopqrstuvwxyz"
        "ABCDEFGHIJKLMNOPQRSTUVWXYZ";

    static const std::string nums_chars=
        "0123456789";
    static thread_local std::mt19937 rng(std::random_device{}());
    std::uniform_int_distribution<> length_dist(min_len, max_len);
    std::uniform_int_distribution<> char_dist;
    if (!nums)
    {
        const std::uniform_int_distribution<> cdist(0, chars.size() - 1);
        char_dist = cdist;
    }
    else
    {
        const std::uniform_int_distribution<> cdist(0, nums_chars.size() - 1);
        char_dist = cdist;
    }

    const size_t length = length_dist(rng);
    std::string result;
    result.reserve(length);
    if (!nums)
    {
        for (size_t i = 0; i < length; ++i)
            result += chars[char_dist(rng)];
    }
    else
    {
        for (size_t i = 0; i < length; ++i)
            result += nums_chars[char_dist(rng)];
    }
    return result;
}
void single_thread_test() {
    std::cout << "Hello, NebulaDB" << std::endl;
    ThreadPool::getInstance();
    Manager::DBManager dbmanager;
    dbmanager.showAllDB();
    const std::string db_name = "AadharDB";
    const std::string tbl_name = "Aadhar";
    dbmanager.deleteDB(&db_name);
    dbmanager.createDB(&db_name);
    dbmanager.showAllDB();
    dbmanager.selectDB(&db_name);
    Schema mySchema(tbl_name, {
        {1,"sno", DataType::INT, true, false, false},
        {2,"name", DataType::STRING, false, false,false},
        {3,"age", DataType::INT, false, false, false},
        {4,"aadhar_id", DataType::STRING, true, false, false}
    });
    dbmanager.createTable(&tbl_name, &mySchema);
    dbmanager.showAllTables();

    int num_entries = 50000 ;
    while (num_entries > 0) {
        int sno = random_number(1,40000000);
        int age = random_number(1,105);
        /* This inserts a single row into the Aadhar table */
        auto insert_query = std::make_unique<InternalQuery::InsertQuery>();
        insert_query->InternalQuery::Query::qtype = InternalQuery::QueryType::INSERT_QUERY;
        insert_query->values = {
            sno,
            random_string(5, 10, false),
            age,
            random_string(12,12,true)
        };

        InternalQuery::TableQuery tbl_query;
        tbl_query.table_name = tbl_name;
        tbl_query.type = InternalQuery::TableQuery::TableQueryType::INSERT;
        tbl_query.query = std::move(insert_query);

        dbmanager.executeQueryOnTable(tbl_query);
        num_entries --;
    }

    /* This is the index column query for age*/
    auto index_col_query = std::make_unique<InternalQuery::IndexQuery>();
    index_col_query->InternalQuery::Query::qtype = InternalQuery::QueryType::INDEX_QUERY;
    index_col_query->col_id = 3;

    InternalQuery::TableQuery index_tbl_query;
    index_tbl_query.table_name = tbl_name;
    index_tbl_query.type = InternalQuery::TableQuery::TableQueryType::INDEX_COL;
    index_tbl_query.query = std::move(index_col_query);

    dbmanager.executeQueryOnTable(index_tbl_query);

    auto index_col_query_1 = std::make_unique<InternalQuery::IndexQuery>();
    index_col_query_1->InternalQuery::Query::qtype = InternalQuery::QueryType::INDEX_QUERY;
    index_col_query_1->col_id = 1;

    InternalQuery::TableQuery index_tbl_query_1;
    index_tbl_query_1.table_name = tbl_name;
    index_tbl_query_1.type = InternalQuery::TableQuery::TableQueryType::INDEX_COL;
    index_tbl_query_1.query = std::move(index_col_query_1);
    dbmanager.executeQueryOnTable(index_tbl_query_1);


    /* This selects all rows from the Aadhar table and age */
    InternalQuery::OrQuery or_query;
    std::vector<InternalQuery::Condition> conditions;
    conditions.push_back(InternalQuery::Condition(3, InternalQuery::EQUAL, InternalQuery::Condition::Filtype::SINGLE_VALUE, 25, {0,0}));
    or_query.and_groups.push_back(InternalQuery::AndQuery(conditions));
    auto select_all_query = std::make_unique<InternalQuery::SelectQuery>();
    select_all_query->InternalQuery::Query::qtype = InternalQuery::QueryType::SELECT_QUERY;
    select_all_query->predicate = std::move(or_query);
    select_all_query->projection = {3, 4};

    InternalQuery::TableQuery select_tbl_query;
    select_tbl_query.table_name = tbl_name;
    select_tbl_query.type = InternalQuery::TableQuery::TableQueryType::SELECT;
    select_tbl_query.query = std::move(select_all_query);

    dbmanager.executeQueryOnTable(select_tbl_query);

    auto update_query = std::make_unique<InternalQuery::UpdateQuery>();
    update_query->InternalQuery::Query::qtype = InternalQuery::QueryType::UPDATE_QUERY;
    InternalQuery::OrQuery or_query_1;
    std::vector<InternalQuery::Condition> conditions_1;
    conditions_1.push_back(InternalQuery::Condition(3, InternalQuery::EQUAL, InternalQuery::Condition::Filtype::SINGLE_VALUE, 25, {0,0}));
    or_query_1.and_groups.push_back(InternalQuery::AndQuery(conditions_1));
    update_query->predicate = std::move(or_query_1);
    update_query->updates= {{3,28}};
    InternalQuery::TableQuery update_tbl_query;
    update_tbl_query.table_name = tbl_name;
    update_tbl_query.type = InternalQuery::TableQuery::TableQueryType::UPDATE;
    update_tbl_query.query = std::move(update_query);

    dbmanager.executeQueryOnTable(update_tbl_query);

    /* Now try to find the same age 25 from the table u should find no selected rows */
    InternalQuery::OrQuery or_query_2;
    std::vector<InternalQuery::Condition> conditions_2;
    conditions_2.push_back(InternalQuery::Condition(3, InternalQuery::EQUAL, InternalQuery::Condition::Filtype::SINGLE_VALUE, 25, {0,0}));
    or_query_2.and_groups.push_back(InternalQuery::AndQuery(conditions_2));
    auto select_all_query_1 = std::make_unique<InternalQuery::SelectQuery>();
    select_all_query_1->InternalQuery::Query::qtype = InternalQuery::QueryType::SELECT_QUERY;
    select_all_query_1->predicate = std::move(or_query_2);
    select_all_query_1->projection = {3, 4};

    InternalQuery::TableQuery select_tbl_query_1;
    select_tbl_query_1.table_name = tbl_name;
    select_tbl_query_1.type = InternalQuery::TableQuery::TableQueryType::SELECT;
    select_tbl_query_1.query = std::move(select_all_query_1);

    dbmanager.executeQueryOnTable(select_tbl_query_1);

    /*Now find the 28 age selected query that was updated*/
    InternalQuery::OrQuery or_query_3;
    std::vector<InternalQuery::Condition> conditions_3;
    conditions_3.push_back(InternalQuery::Condition(3, InternalQuery::EQUAL, InternalQuery::Condition::Filtype::SINGLE_VALUE, 28, {0,0}));
    or_query_3.and_groups.push_back(InternalQuery::AndQuery(conditions_3));
    auto select_all_query_2 = std::make_unique<InternalQuery::SelectQuery>();
    select_all_query_2->InternalQuery::Query::qtype = InternalQuery::QueryType::SELECT_QUERY;
    select_all_query_2->predicate = std::move(or_query_3);
    select_all_query_2->projection = {3, 4};

    InternalQuery::TableQuery select_tbl_query_2;
    select_tbl_query_2.table_name = tbl_name;
    select_tbl_query_2.type = InternalQuery::TableQuery::TableQueryType::SELECT;
    select_tbl_query_2.query = std::move(select_all_query_2);

    dbmanager.executeQueryOnTable(select_tbl_query_2);

    /*Now try to delete all rows with age 28*/
    InternalQuery::OrQuery or_query_4;
    std::vector<InternalQuery::Condition> conditions_4;
    conditions_4.push_back(InternalQuery::Condition(3, InternalQuery::EQUAL, InternalQuery::Condition::Filtype::SINGLE_VALUE, 28, {0,0}));
    or_query_4.and_groups.push_back(InternalQuery::AndQuery(conditions_4));
    auto delete_query = std::make_unique<InternalQuery::DeleteQuery>();
    delete_query->InternalQuery::Query::qtype = InternalQuery::QueryType::DELETE_QUERY;
    delete_query->predicate = std::move(or_query_4);

    InternalQuery::TableQuery delete_tbl_query;
    delete_tbl_query.table_name = tbl_name;
    delete_tbl_query.type = InternalQuery::TableQuery::TableQueryType::DELETE;
    delete_tbl_query.query = std::move(delete_query);

    dbmanager.executeQueryOnTable(delete_tbl_query);


    /*
     * Now find the 28 age selected query that was deleted
     * The test should find none as all of them were deleted.
     */

    InternalQuery::OrQuery or_query_5;
    std::vector<InternalQuery::Condition> conditions_5;
    conditions_5.push_back(InternalQuery::Condition(3, InternalQuery::EQUAL, InternalQuery::Condition::Filtype::SINGLE_VALUE, 28, {0,0}));
    or_query_5.and_groups.push_back(InternalQuery::AndQuery(conditions_5));
    auto select_all_query_3 = std::make_unique<InternalQuery::SelectQuery>();
    select_all_query_3->InternalQuery::Query::qtype = InternalQuery::QueryType::SELECT_QUERY;
    select_all_query_3->predicate = std::move(or_query_5);
    select_all_query_3->projection = {3, 4};

    InternalQuery::TableQuery select_tbl_query_3;
    select_tbl_query_3.table_name = tbl_name;
    select_tbl_query_3.type = InternalQuery::TableQuery::TableQueryType::SELECT;
    select_tbl_query_3.query = std::move(select_all_query_3);

    dbmanager.executeQueryOnTable(select_tbl_query_3);
}
void create_table_work(Manager::DBManager& dbmanager, const std::string& tbl_name) {
    static std::atomic<int> sno_counter {1};
    int sno = sno_counter.fetch_add(1, std::memory_order_relaxed);
    int age = random_number(1,105);
    /* This inserts a single row into the Aadhar table */
    auto insert_query = std::make_unique<InternalQuery::InsertQuery>();
    insert_query->InternalQuery::Query::qtype = InternalQuery::QueryType::INSERT_QUERY;
    insert_query->values = {
        sno,
        random_string(5, 10, false),
        age,
        random_string(12,12,true)
    };

    InternalQuery::TableQuery tbl_query;
    tbl_query.table_name = tbl_name;
    tbl_query.type = InternalQuery::TableQuery::TableQueryType::INSERT;
    tbl_query.query = std::move(insert_query);

    dbmanager.executeQueryOnTable(tbl_query);
}

void delete_from_table_work(Manager::DBManager& dbmanager, const std::string& tbl_name, int age) {

    /*Now try to delete all rows with certain age */
    InternalQuery::OrQuery or_query;
    std::vector<InternalQuery::Condition> conditions;
    conditions.push_back(InternalQuery::Condition(3, InternalQuery::EQUAL, InternalQuery::Condition::Filtype::SINGLE_VALUE, age, {0,0}));
    or_query.and_groups.push_back(InternalQuery::AndQuery(conditions));
    auto delete_query = std::make_unique<InternalQuery::DeleteQuery>();
    delete_query->InternalQuery::Query::qtype = InternalQuery::QueryType::DELETE_QUERY;
    delete_query->predicate = std::move(or_query);

    InternalQuery::TableQuery delete_tbl_query;
    delete_tbl_query.table_name = tbl_name;
    delete_tbl_query.type = InternalQuery::TableQuery::TableQueryType::DELETE;
    delete_tbl_query.query = std::move(delete_query);

    dbmanager.executeQueryOnTable(delete_tbl_query);
}

void multi_thread_test() {
    ThreadPool* tpool = ThreadPool::getInstance();
    Manager::DBManager dbmanager;
    dbmanager.showAllDB();
    const std::string db_name = "AadharDB";
    const std::string tbl_name = "Aadhar";
    dbmanager.deleteDB(&db_name);
    dbmanager.createDB(&db_name);
    dbmanager.showAllDB();
    dbmanager.selectDB(&db_name);
    Schema mySchema(tbl_name, {
        {1,"sno", DataType::INT, true, false, false},
        {2,"name", DataType::STRING, false, false,false},
        {3,"age", DataType::INT, false, false, false},
        {4,"aadhar_id", DataType::STRING, true, false, false}
    });
    dbmanager.createTable(&tbl_name, &mySchema);
    dbmanager.showAllTables();
    std::vector<std::future<void>> futures;
    int num_entries =  50;
    while (num_entries > 0) {
        futures.emplace_back(tpool->enqueue(create_table_work,std::ref(dbmanager),std::cref(tbl_name)));
        num_entries --;
    }

    // wait for all the threads to comeback.
    for (auto& f : futures) {
        try {
            f.get();
        } catch (const std::exception& e) {
            std::cerr << "Worker failed: " << e.what() << '\n';
        }
        catch (...) {
            std::cerr << "Worker failed with unknown exception\n";
        }
    }

    /* This is the index column query for age*/
    auto index_col_query = std::make_unique<InternalQuery::IndexQuery>();
    index_col_query->InternalQuery::Query::qtype = InternalQuery::QueryType::INDEX_QUERY;
    index_col_query->col_id = 3;

    InternalQuery::TableQuery index_tbl_query;
    index_tbl_query.table_name = tbl_name;
    index_tbl_query.type = InternalQuery::TableQuery::TableQueryType::INDEX_COL;
    index_tbl_query.query = std::move(index_col_query);

    dbmanager.executeQueryOnTable(index_tbl_query);

    /* This selects all rows from the Aadhar table */
    InternalQuery::OrQuery or_query;
    auto select_all_query = std::make_unique<InternalQuery::SelectQuery>();
    select_all_query->InternalQuery::Query::qtype = InternalQuery::QueryType::SELECT_QUERY;
    select_all_query->predicate = std::move(or_query);
    select_all_query->projection = {1, 2, 3, 4};
    InternalQuery::TableQuery select_tbl_query;
    select_tbl_query.table_name = tbl_name;
    select_tbl_query.type = InternalQuery::TableQuery::TableQueryType::SELECT;
    select_tbl_query.query = std::move(select_all_query);

    dbmanager.executeQueryOnTable(select_tbl_query);

    int del_requests = 100;
    std::vector<std::future<void>> del_futures;
    while (del_requests > 0) {
        int age = random_number(1,100);
        std::cout << "Age to be deleted is = "<< age << std::endl;
        del_futures.emplace_back(tpool->enqueue(delete_from_table_work,std::ref(dbmanager),std::cref(tbl_name), age));
        del_requests--;
    }

    // wait for all the del threads to comeback.
    for (auto& f : del_futures) {
        try {
            f.get();
        } catch (const std::exception& e) {
            std::cerr << "Worker failed: " << e.what() << '\n';
        }
        catch (...) {
            std::cerr << "Worker failed with unknown exception\n";
        }
    }

    /* This selects all rows from the Aadhar table */
    InternalQuery::OrQuery or_query_1;
    auto select_all_query_1 = std::make_unique<InternalQuery::SelectQuery>();
    select_all_query_1->InternalQuery::Query::qtype = InternalQuery::QueryType::SELECT_QUERY;
    select_all_query_1->predicate = std::move(or_query_1);
    select_all_query_1->projection = {1, 2, 3, 4};
    InternalQuery::TableQuery select_tbl_query_1;
    select_tbl_query_1.table_name = tbl_name;
    select_tbl_query_1.type = InternalQuery::TableQuery::TableQueryType::SELECT;
    select_tbl_query_1.query = std::move(select_all_query_1);
    dbmanager.executeQueryOnTable(select_tbl_query_1);


    /* This selects all rows from 1 to 100. This forces search through b tree */
    InternalQuery::OrQuery or_query_2;
    std::vector<InternalQuery::Condition> conditions_select;
    conditions_select.push_back(InternalQuery::Condition(3, InternalQuery::EQUAL, InternalQuery::Condition::Filtype::RANGE_VALUE, 0, {0,106}));
    or_query_2.and_groups.push_back(InternalQuery::AndQuery(conditions_select));
    auto select_all_query_2 = std::make_unique<InternalQuery::SelectQuery>();
    select_all_query_2->InternalQuery::Query::qtype = InternalQuery::QueryType::SELECT_QUERY;
    select_all_query_2->predicate = std::move(or_query_2);
    select_all_query_2->projection = {1, 2, 3, 4};
    InternalQuery::TableQuery select_tbl_query_2;
    select_tbl_query_2.table_name = tbl_name;
    select_tbl_query_2.type = InternalQuery::TableQuery::TableQueryType::SELECT;
    select_tbl_query_2.query = std::move(select_all_query_2);
    dbmanager.executeQueryOnTable(select_tbl_query_2);
    std::cout<< "Complete Exiting" <<std::endl;
}
int main() {
    try {
        multi_thread_test();
    }
    catch (const std::exception& e) {
        std::cerr << "MAIN EXCEPTION: "
                  << e.what() << '\n';
        return 1;
    }
    catch (...) {
        std::cerr << "UNKNOWN MAIN EXCEPTION\n";
        return 1;
    }
    return 0;
}
