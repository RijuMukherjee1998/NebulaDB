//
// Created by Riju Mukherjee on 31-01-2025.
//

#include <iostream>
#include "headers/DBManager.h"
#include <chrono>
#include <thread>

int main()
{
    try
    {
        std::cout << "Hello, NebulaDB" << std::endl;
        Manager::DBManager dbmanager;
        dbmanager.showAllDB();
        const std::string db_name = "mydb";
        const std::string tbl_name = "person";
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
        // int id = 1;
        // int age = 25;
        // dbmanager.insertIntoSelectedTable(id, "Riju", age);
        Schema sch = mySchema;
        dbmanager.selectAllFromSelectedTable(sch);
        dbmanager.shutdownDB(&db_name);
    }
    catch (std::exception &e)
    {
        std::cout << e.what() << std::endl;
        std::cout<< "Exiting DB .... Critical Error" << std::endl;
        return -1;
    }
    return 0;
}