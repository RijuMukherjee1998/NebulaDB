//
// Created by Riju Mukherjee on 31-01-2025.
//

#include <iostream>
#include "headers/DBManager.h"

int main()
{
    try
    {
        std::cout << "Hello, NebulaDB" << std::endl;
        Manager::DBManager dbmanager;
        dbmanager.showAllDB();
        const std::string db_name = "PersonsDB";
        const std::string tbl_name = "persons";
        dbmanager.createDB(&db_name);
        dbmanager.showAllDB();
        dbmanager.selectDB(&db_name);

        const Schema mySchema(tbl_name, {
            {"id", DataType::INT, true, false},
            {"name", DataType::STRING, false, false},
            {"age", DataType::INT, false, false},
        });
        dbmanager.createTable(&tbl_name, &mySchema);
        dbmanager.showAllTables();
        dbmanager.selectTable(&tbl_name);
        int id = 1;
        int age = 25;
        std::string name = "Riju";
        std::vector<Column> columns;
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
        while (id <= 100000)
        {
            columns.push_back(ID);
            columns.push_back(NAME);
            columns.push_back(AGE);
            dbmanager.insertIntoSelectedTable(columns);
            id++;
            age++;
            age = age % 100;
            ID.col_value = id;
            AGE.col_value = age;
            columns.clear();
        }
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