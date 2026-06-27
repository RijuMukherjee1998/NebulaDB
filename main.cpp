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
/*
int  WriteAndReadAll_EX() {
     try
    {
        std::cout << "Hello, NebulaDB" << std::endl;
        ThreadPool::getInstance();
        Manager::DBManager dbmanager;
        dbmanager.showAllDB();
        const std::string db_name = "AadharDB";
        const std::string tbl_name = "Aadhar";
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
        dbmanager.selectTable(&tbl_name);
        int id = 1;
        int age = 25;
        std::string name = "Riju";
        std::string aaid = "354268570149";
        std::vector<Column> columns;

        Column SNO;
        SNO.col_name = "id";
        SNO.col_type = DataType::INT;
        SNO.col_value = id;
        SNO.is_primary_key = true;
        SNO.is_null = false;

        Column NAME;
        NAME.col_name = "name";
        NAME.col_type = DataType::STRING;
        NAME.col_value = name;

        Column AGE;
        AGE.col_name = "age";
        AGE.col_type = DataType::INT;
        AGE.col_value = age;
        Column AADHAR_ID;
        AADHAR_ID.col_name = "aadhar_id";
        AADHAR_ID.col_type = DataType::STRING;
        AADHAR_ID.col_value = aaid;
        SNO.is_primary_key = true;
        SNO.is_null = false;

        // columns.push_back(SNO);
        // columns.push_back(NAME);
        // columns.push_back(AGE);
        // columns.push_back(AADHAR_ID);
        // dbmanager.insertIntoSelectedTable(columns);
        while (id <= 2048)
        {
            columns.clear();
            columns.push_back(SNO);
            columns.push_back(NAME);
            columns.push_back(AGE);
            columns.push_back(AADHAR_ID);
            dbmanager.insertIntoSelectedTable(columns);
            id++;
            age++;
            age = age % 100;
            SNO.col_value = id;
            AGE.col_value = age;
            name = random_string(6, 14);
            NAME.col_value = name;
            aaid = random_string(12, 12, true);
            AADHAR_ID.col_value = aaid;
        }
        std::cout << "All Data Inserted" << std::endl;
        dbmanager.selectAllFromSelectedTable();
    }
    catch (std::exception &e)
    {
        std::cout << e.what() << std::endl;
        std::cout<< "Exiting DB .... Critical Error" << std::endl;
        return -1;
    }
    return 0;
}

int WriteAndCreateIndex_EX() {
    try {
        std::cout << "Hello, NebulaDB" << std::endl;
        ThreadPool::getInstance();
        Manager::DBManager dbmanager;
        dbmanager.showAllDB();
        const std::string db_name = "MY_DB";
        const std::string tbl_name = "PeopleInfo";
        dbmanager.createDB(&db_name);
        dbmanager.showAllDB();
        dbmanager.selectDB(&db_name);
        Schema* mySchema = new Schema(tbl_name, {
            {1,"sno", DataType::INT, true, false, false},
            {2,"name", DataType::STRING, false, false, false},
            {3,"age", DataType::INT, false, false, false},
            {4,"aadhar_id", DataType::STRING, true, false, false}
        });
        dbmanager.createTable(&tbl_name, mySchema);
        dbmanager.showAllTables();
        dbmanager.selectTable(&tbl_name);

        int id = 1;
        int age = 25;
        std::string name = "Riju";
        std::string aaid = "354268570149";
        std::vector<Column> columns;

        Column SNO;
        SNO.col_name = "sno";
        SNO.col_type = DataType::INT;
        SNO.col_value = id;
        SNO.is_primary_key = true;
        SNO.is_null = false;
        SNO.is_indexed = false;

        Column NAME;
        NAME.col_name = "name";
        NAME.col_type = DataType::STRING;
        NAME.col_value = name;

        Column AGE;
        AGE.col_name = "age";
        AGE.col_type = DataType::INT;
        AGE.col_value = age;

        Column AADHAR_ID;
        AADHAR_ID.col_name = "aadhar_id";
        AADHAR_ID.col_type = DataType::STRING;
        AADHAR_ID.col_value = aaid;


        while (id < 100000)
        {
            columns.clear();
            columns.push_back(SNO);
            columns.push_back(NAME);
            columns.push_back(AGE);
            columns.push_back(AADHAR_ID);
            dbmanager.insertIntoSelectedTable(columns);
            id++;
            age++;
            age = age % 100;
            SNO.col_value = id;
            AGE.col_value = age;
            name = random_string(6, 14);
            NAME.col_value = name;
            aaid = random_string(12, 12, true);
            AADHAR_ID.col_value = aaid;
        }
        std::cout << "All Data Inserted" << std::endl;
        //dbmanager.selectAllFromSelectedTable();

        dbmanager.createIndexOnTable(&tbl_name,"sno");
    }
    catch (std::exception &e)
    {
        std::cout << e.what() << std::endl;
        std::cout<< "Exiting DB .... Critical Error" << std::endl;
        return -1;
    }
    return 0;
}
void FindDataByIndex() {
    std::cout << "Hello, NebulaDB" << std::endl;
    ThreadPool::getInstance();
    Manager::DBManager dbmanager;
    dbmanager.showAllDB();
    const std::string db_name = "MY_DB";
    const std::string tbl_name = "PeopleInfo";
    dbmanager.selectDB(&db_name);
    dbmanager.showAllTables();
    dbmanager.selectTable(&tbl_name);
    variant_data_t key = 50;
    std::string idx_sno = "sno";
    dbmanager.selectRowFromTableByIndex(idx_sno,key);
    dbmanager.createIndexOnTable(&tbl_name,"age");
    std::string idx_age = "age";
    variant_data_t start_age = 60;
    variant_data_t end_age = 70;
    dbmanager.selectRowsFromTableByIndexRange(idx_age, start_age, end_age);
    // all data at age = 60-60
    end_age = 60;
    dbmanager.selectRowsFromTableByIndexRange(idx_age, start_age, end_age);
    dbmanager.deleteTable(&tbl_name);
    dbmanager.deleteDB(&db_name);
}
*/
int main()
{
    std::cout << "Hello, NebulaDB" << std::endl;
    ThreadPool::getInstance();
    Manager::DBManager dbmanager;
    dbmanager.showAllDB();
    const std::string db_name = "AadharDB";
    const std::string tbl_name = "Aadhar";
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

    /* This inserts a single row into the Aadhar table */
    auto insert_query = std::make_unique<InternalQuery::InsertQuery>();
    insert_query->InternalQuery::Query::qtype = InternalQuery::QueryType::INSERT_QUERY;
    insert_query->values = {
        1,
        std::string("Riju"),
        25,
        std::string("354268570149")
    };

    InternalQuery::TableQuery tbl_query;
    tbl_query.table_name = tbl_name;
    tbl_query.type = InternalQuery::TableQuery::TableQueryType::INSERT;
    tbl_query.query = std::move(insert_query);

    dbmanager.executeQueryOnTable(tbl_query);

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


    /* This selects all rows from the Aadhar table */
    auto select_all_query = std::make_unique<InternalQuery::SelectQuery>();
    select_all_query->InternalQuery::Query::qtype = InternalQuery::QueryType::SELECT_QUERY;
    select_all_query->projection = {2, 4};

    InternalQuery::TableQuery select_tbl_query;
    select_tbl_query.table_name = tbl_name;
    select_tbl_query.type = InternalQuery::TableQuery::TableQueryType::SELECT;
    select_tbl_query.query = std::move(select_all_query);

    dbmanager.executeQueryOnTable(select_tbl_query);

    


    return 0;
}
