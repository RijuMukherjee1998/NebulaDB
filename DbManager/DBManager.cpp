//
// Created by Riju Mukherjee on 31-01-2025.
//

#include <algorithm>
#include <filesystem>
#include <vector>
#include "../headers/DBManager.h"

const std::filesystem::path base_path = BASE_NDB_PATH;

std::vector<std::filesystem::path> Manager::DBManager::listAllDB()
{
    std::vector<std::filesystem::path> databases;
    for (auto const& entry : std::filesystem::directory_iterator(base_path))
    {
        if (entry.is_directory())
        {
            databases.emplace_back(entry.path());
        }
    }
    return databases;
}

std::vector<std::filesystem::path> Manager::DBManager::listAllTables() const
{
    std::vector<std::filesystem::path> tables;
    if (currSelectedDBPath.empty())
    {
        logger->logWarn({"No database selected"});
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
void Manager::DBManager::showAllDB() const
{
    for (auto const& database : listAllDB())
    {
        logger->logInfo({"Database:" + database.string() + "(DB)"});
    }
}

void Manager::DBManager::createDB(const std::string* db_name) const
{
    std::string new_db_path =BASE_NDB_PATH +  *db_name;
    const std::filesystem::path db_name_fs = new_db_path;
    for (const std::vector<std::filesystem::path> databases = listAllDB(); auto const& entry : databases)
    {
        if (entry.compare(db_name_fs) == 0)
        {
            logger->logWarn({"database with name", db_name_fs.string(),"already exists"});
            return;
        }
    }
    std::filesystem::create_directory(db_name_fs);
    logger->logInfo({"Created database", db_name_fs.string()});
}

void Manager::DBManager::deleteDB(const std::string* db_name)
{
    selectDB(db_name);
    if (currSelectedDBPath.empty())
        logger->logWarn({"No database with name", *db_name});
    else
    {
        std::filesystem::remove_all(currSelectedDBPath);
        currSelectedDBPath.clear();
    }
}

void Manager::DBManager::selectDB(const std::string* db_name)
{
    const std::string db_path =BASE_NDB_PATH +  *db_name;
    std::filesystem::path db_name_fs = db_path;
    for (const std::vector<std::filesystem::path> databases = listAllDB(); auto const& entry : databases)
    {
        if (entry.compare(db_name_fs) == 0)
        {
            logger->logInfo({"Database", *db_name});
            currSelectedDBPath = std::move(db_name_fs);
            return;
        }
    }
    logger->logWarn({"Database",*db_name,"not found"});
}

void Manager::DBManager::showAllTables() const
{
    if (currSelectedDBPath.empty())
    {
        logger->logWarn({"No database selected"});
        return;
    }
    logger->logInfo({"All Tables in DB:",currSelectedDBPath.filename().string()});
    for (auto const& table : listAllTables())
    {
        logger->logInfo({"Table", table.filename().string()});
    }
}

void Manager::DBManager::selectTable(const std::string* table_name)
{
    if (currSelectedDBPath.empty())
    {
        logger->logWarn({"Cannot Select Table...No Database is selected"});
        return;
    }
    std::vector<std::filesystem::path> tables = listAllTables();
    if (tables.empty())
    {
        logger->logWarn({"Cannot Select Table , No tables in DB"});
        return;
    }
    std::filesystem::path  tbl_name_fs = *table_name;
    for (auto const& entry : tables)
    {
        if (entry.filename().compare(tbl_name_fs) == 0)
        {
            logger->logInfo({"Table", tables.back().filename().string(),"Selected"});
            currSelectedDBPath = std::move(tbl_name_fs);
        }
    }
}

void Manager::DBManager::createTable(const std::string* table_name, const Schema* schema) const
{
    const std::vector<std::filesystem::path> tables = listAllTables();
    if (currSelectedDBPath.empty())
    {
        logger->logWarn({"Cannot Create Table...No Database is selected"});
        return;
    }
    const std::filesystem::path  tbl_name_fs = *table_name;
    const std::filesystem::path table_path = currSelectedDBPath/tbl_name_fs;
    for (auto const& entry : tables)
    {
        if (entry.compare(table_path) == 0)
        {
            logger->logWarn({"Table", table_path.string()});
            return;
        }
    }
    std::filesystem::create_directory(table_path);
    schema->saveToFile(table_path);
    logger->logInfo({"Table", table_path.filename().string(),"Created"});
}

void Manager::DBManager::deleteTable(const std::string* table_name) const
{
    if (currSelectedDBPath.empty())
    {
        logger->logWarn({"Cannot Delete Table...No Database is selected"});
        return;
    }
    std::vector<std::filesystem::path> tables = listAllTables();
    if (tables.empty())
    {
        logger->logWarn({"Cannot Delete Table...No Database is selected"});
        return;
    }
    const std::filesystem::path  tbl_name_fs = *table_name;
    for (auto const& entry : tables)
    {
        if (entry.filename().compare(tbl_name_fs) == 0)
        {
            logger->logInfo({"Table", tables.back().filename().string(),"Deleted"});
            std::filesystem::remove_all(entry);
            return;
        }
    }
    logger->logWarn({"Cannot Delete Table,",*table_name,"not found"});
}