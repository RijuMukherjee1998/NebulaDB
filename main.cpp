#include <iostream>
#include "dbmanager/DBManager.h"
int main()
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
    dbmanager.createTable(&tbl_name);
    dbmanager.showAllTables();
    dbmanager.deleteTable(&tbl_name);
    dbmanager.showAllTables();
    dbmanager.createTable(&tbl_name);
    dbmanager.selectTable(&tbl_name);
    return 0;
}