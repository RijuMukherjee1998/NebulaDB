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
        std::string db_name = "mydb";
        std::string tbl_name = "mytable";
        dbmanager.createDB(&db_name);
        dbmanager.showAllDB();
        dbmanager.selectDB(&db_name);

        Schema mySchema(tbl_name, {
            {"id", DataType::INT, true, false},
            {"name", DataType::STRING, false, false},
            {"age", DataType::INT, false, false},
        });
        dbmanager.createTable(&tbl_name, &mySchema);
        dbmanager.showAllTables();
        dbmanager.selectTable(&tbl_name);
        dbmanager.insertIntoSelectedTable(&mySchema);
    }
    catch (std::exception &e)
    {
        std::cout << e.what() << std::endl;
        std::cout<< "Exiting DB .... Critical Error" << std::endl;
        return -1;
    }
    return 0;
}