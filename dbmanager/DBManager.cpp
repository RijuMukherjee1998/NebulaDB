//
// Created by Riju Mukherjee on 31-01-2025.
//

#include <algorithm>

#include "dbmanager.h"

#include <filesystem>
#include <iostream>
#include <queue>
#include <vector>

#include "../include/constants.h"

const std::filesystem::path base_path = BASE_NDB_PATH;
const std::vector<std::filesystem::path> StorageEngine::DBManager::listAllDB() const
{
    std::vector<std::filesystem::path> databases;
    for (auto const& entry : std::filesystem::directory_iterator(base_path))
    {
        if (entry.is_directory())
        {
            std::cout << entry.path() << std::endl;
            databases.emplace_back(entry.path());
        }
    }
    return databases;
}
const std::vector<std::filesystem::path> StorageEngine::DBManager::listAllTables() const
{
    std::vector<std::filesystem::path> tables;
    if (currSelectedDBPath.empty())
    {
        std::cout<< "No Database is selected" <<std::endl;
        return std::vector<std::filesystem::path>();
    }

    for (const auto& entry : std::filesystem::directory_iterator(currSelectedDBPath))
    {
        if (entry.is_directory())
        {
            tables.emplace_back(entry.path());
        }
    }
    return tables;
}
void StorageEngine::DBManager::showAllDB() const
{
    for (auto const& database : listAllDB())
    {
        std::cout << database.filename() << std::endl;
    }
}

void StorageEngine::DBManager::createDB(const std::string* db_name)
{
    std::string new_db_path = BASE_NDB_PATH +  *db_name;
    const std::filesystem::path db_name_fs = new_db_path;
    for (const std::vector<std::filesystem::path> databases = listAllDB(); auto const& entry : databases)
    {
        if (entry.compare(db_name_fs) == 0)
        {
            std::cout << "Database Already exists" << std::endl;
            return;
        }
    }
    std::filesystem::create_directory(db_name_fs);
    std::cout << "Database "<<db_name_fs<<" Created"<< std::endl;
}

void StorageEngine::DBManager::deleteDB(const std::string* db_name)
{
    selectDB(db_name);
    if (currSelectedDBPath.empty())
        std::cout<< "No Database of name "<<db_name->c_str()<<std::endl;
    else
    {
        std::filesystem::remove_all(currSelectedDBPath);
        currSelectedDBPath.clear();
    }
}

void StorageEngine::DBManager::selectDB(const std::string* db_name)
{
    std::string db_path = BASE_NDB_PATH +  *db_name;
    std::filesystem::path db_name_fs = db_path;
    for (const std::vector<std::filesystem::path> databases = listAllDB(); auto const& entry : databases)
    {
        if (entry.compare(db_name_fs) == 0)
        {
            std::cout << "Database Found... Selecting Database" << std::endl;
            currSelectedDBPath = std::move(db_name_fs);
            return;
        }
    }
    std::cout << "Database "<<*db_name<<" Not Found..." << std::endl;
}

void StorageEngine::DBManager::showAllTables() const
{
    if (currSelectedDBPath.empty())
    {
        std::cout<< "No Database is selected" <<std::endl;
        return;
    }
    std::cout<<"All Tables in DB: "<< currSelectedDBPath.filename()<<std::endl;
    for (auto const& table : listAllTables())
    {
        std::cout << table.filename()<<std::endl;
    }
}

void StorageEngine::DBManager::createTable(const std::string* table_name)
{
    std::vector<std::filesystem::path> tables = listAllTables();
    if (currSelectedDBPath.empty())
    {
        std::cout<< "Cannot Create Table...No Database is selected" <<std::endl;
        return;
    }
    std::filesystem::path  tbl_name_fs = *table_name;
    std::filesystem::path table_path = currSelectedDBPath/tbl_name_fs;
    for (auto const& entry : tables)
    {
        if (entry.compare(table_path) == 0)
        {
            std::cout << "Table Already exists" << std::endl;
            return;
        }
    }
    std::filesystem::create_directory(table_path);
    std::cout << "Table "<<table_path.filename()<<"Created"<< std::endl;
}

void StorageEngine::DBManager::deleteTable(const std::string* table_name)
{
    if (currSelectedDBPath.empty())
    {
        std::cout<< "Cannot Delete Table...No Database is selected" <<std::endl;
        return;
    }
    std::vector<std::filesystem::path> tables = listAllTables();
    if (tables.empty())
    {
        std::cout<<"Cannot Delete Table , No tables in DB"<<std::endl;
        return;
    }
    std::filesystem::path  tbl_name_fs = *table_name;
    for (auto const& entry : tables)
    {
        if (entry.filename().compare(tbl_name_fs) == 0)
        {
            std::cout << "Table "<<tables.back().filename()<<"Deleted"<< std::endl;
            std::filesystem::remove_all(entry);
            return;
        }
    }
    std::cout<< "Cannot Delete Table "<<*table_name<<"Not Found..."<<std::endl;
}

void StorageEngine::DBManager::selectTable(const std::string* table_name)
{
    if (currSelectedDBPath.empty())
    {
        std::cout<< "Cannot Select Table...No Database is selected" <<std::endl;
        return;
    }
    std::vector<std::filesystem::path> tables = listAllTables();
    if (tables.empty())
    {
        std::cout<<"Cannot Select Table , No tables in DB"<<std::endl;
        return;
    }
    std::filesystem::path  tbl_name_fs = *table_name;
    for (auto const& entry : tables)
    {
        if (entry.filename().compare(tbl_name_fs) == 0)
        {
            std::cout << "Table "<<tables.back().filename()<<" Selected"<< std::endl;
            currSelectedDBPath = std::move(tbl_name_fs);
        }
    }
}
