//
// Created by Riju Mukherjee on 31-01-2025.
//

#include <iostream>
#include <random>
#include <string>
#include <vector>
#include "headers/DBManager.h"
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
int main()
{
    // WriteAndReadAll_EX();
    WriteAndCreateIndex_EX();
    FindDataByIndex();
    return 0;
}