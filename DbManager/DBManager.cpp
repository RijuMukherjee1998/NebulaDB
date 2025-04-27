//
// Created by Riju Mukherjee on 31-01-2025.
//

#include <algorithm>
#include <filesystem>
#include <vector>
#include "../headers/DBManager.h"

#include <fstream>

#include "../headers/PageDirectory.h"
#include "../headers/TableManager.h"

const std::filesystem::path base_path = BASE_NDB_PATH;

Manager::DBManager::DBManager():curr_table_manager(nullptr)
{
    logger = Utils::Logger::getInstance();
    logger->logInfo({"DB Initialized"});
}

Manager::DBManager::~DBManager()
{
    logger->logInfo({"DB shutdown Initiated"});
    shutdownDB();
    logger->logInfo({"DB shutdown Completed"});
}

std::vector<std::filesystem::path> Manager::DBManager::listAllDB() const
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
    const std::string new_db_path =BASE_NDB_PATH +  *db_name;
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

void Manager::DBManager::shutdownDB() const
{
    if (curr_table_manager != nullptr)
        curr_table_manager->flushAll();
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
    const std::filesystem::path  tbl_name_fs = *table_name;
    if (currSelectedDBPath.empty())
    {
        logger->logWarn({"Cannot Select Table...No Database is selected"});
        return;
    }
    if (currSelectedTablePath == currSelectedDBPath/tbl_name_fs)
    {
        logger->logWarn({"Table already selected",*table_name});
    }
    const std::vector<std::filesystem::path> tables = listAllTables();
    if (tables.empty())
    {
        logger->logWarn({"Cannot Select Table , No tables in DB"});
        return;
    }
    for (auto const& entry : tables)
    {
        if (entry.filename().compare(tbl_name_fs) == 0)
        {
            logger->logInfo({"Table", tables.back().filename().string(),"Selected"});
            currSelectedTablePath = std::move(currSelectedDBPath/tbl_name_fs);
            logger->logInfo({"Curr Table Path = ",currSelectedTablePath.string()});
        }
    }
    if (table_manager_table.find(*table_name) != table_manager_table.end())
    {
        curr_table_manager = table_manager_table[*table_name];
    }
    else
    {
        const Schema* sch = Schema::loadFromFileSchema(currSelectedTablePath/(tbl_name_fs.string()+".json"));
        curr_table_manager = new TableManager(currSelectedDBPath,currSelectedTablePath, sch);
        table_manager_table[*table_name] = curr_table_manager;
    }
}

void Manager::DBManager::createTable(const std::string* table_name, const Schema* schema)
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
    if (table_manager_table.find(*table_name) == table_manager_table.end())
    {
        curr_table_manager = new TableManager(currSelectedDBPath,table_path, schema);
        table_manager_table[*table_name] = curr_table_manager;
    }
    else
    {
        logger->logCritical({"Weird Error...Table already exists in table_manager_table"});
    }
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

void Manager::DBManager::insertIntoSelectedTable(std::vector<Column>& columns) const
{
    if (curr_table_manager == nullptr)
    {
        logger->logCritical({"No table selected"});
        return;
    }
    curr_table_manager->insertIntoTable(columns);
    logger->logInfo({"Value Inserted in table"});
}

void Manager::DBManager::selectAllFromSelectedTable() const {
    if (curr_table_manager == nullptr)
    {
        logger->logCritical({"No table selected"});
        return;
    }
    curr_table_manager->selectAllFromTable();
}