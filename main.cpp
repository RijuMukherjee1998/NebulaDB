#include <iostream>
#include "headers/DBManager.h"
int main()
{
    try
    {
        std::cout << "Hello, NebulaDB" << std::endl;
        StorageEngine::DBManager dbmanager;
        dbmanager.showAllDB();
        std::string db_name = "test";
        std::string db_name1 = "test1";
        std::string tbl_name = "mytable";
        dbmanager.createDB(&db_name);
        dbmanager.showAllDB();
        dbmanager.createDB(&db_name);
        dbmanager.showAllDB();
        dbmanager.createDB(&db_name1);
        dbmanager.showAllDB();
        dbmanager.selectDB(&db_name1);
        dbmanager.deleteDB(&db_name1);
        dbmanager.showAllDB();
        dbmanager.deleteDB(&db_name1);
        dbmanager.selectDB(&db_name);
        Schema mySchema(tbl_name, {
            {"id", DataType::INT, true, false},
            {"name", DataType::STRING, false, false},
            {"age", DataType::INT, false, false},
        });
        dbmanager.createTable(&tbl_name, &mySchema);
        dbmanager.showAllTables();
        dbmanager.deleteTable(&tbl_name);
        dbmanager.showAllTables();
        dbmanager.createTable(&tbl_name, &mySchema);
        dbmanager.selectTable(&tbl_name);
    }
    catch (std::exception &e)
    {
        std::cout << e.what() << std::endl;
        std::cout<< "Exiting DB .... Critical Error" << std::endl;
        return -1;
    }
    return 0;
}