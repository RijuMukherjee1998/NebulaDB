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

Manager::DBManager::DBManager()
{
    logger = Utils::Logger::getInstance();
    logger->logInfo({"DB Initialized"});
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

void Manager::DBManager::shutdownDB(const std::string* db_name)
{
    table_manager->flushAll();
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
    table_manager = new TableManager(currSelectedDBPath,currSelectedTablePath);
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
    // create the page dir file for that table (don't create it here it causes issue
    // std::ofstream pgdirFile(table_path/"pgdir.dat");
   /* if (!pgdirFile)
    {
        logger->logCritical({"Could not create file page-directory.dat"});
        throw std::runtime_error("Could not create file page-directory.dat");
    }*/
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

void Manager::DBManager::insertIntoSelectedTable(int i, std::string n, int a) const
{

    std::vector<Column> columns;
    Column id;
    id.col_name = "id";
    id.col_type = DataType::INT;
    id.col_value = i;
    id.is_primary_key = true;
    id.is_null = false;
    Column name;
    name.col_name = "name";
    name.col_type = DataType::STRING;
    name.col_value = n;
    Column age;
    age.col_name = "age";
    age.col_type = DataType::INT;
    age.col_value = a;
    columns.push_back(id);
    columns.push_back(name);
    columns.push_back(age);
    table_manager->insertIntoTable(columns);
    logger->logInfo({"Value Inserted in table"});
}

void Manager::DBManager::selectAllFromSelectedTable(Schema& schema) const {
    auto table = new std::vector<std::unique_ptr<char[]>>();
    table_manager->selectAllFromTable(table, 14); //for now, fixing it for simplicity and testing

    const std::filesystem::path file_path = currSelectedTablePath/"person.json";
    json schema_json = schema.loadFromFile(file_path);
    logger->logInfo({"Table::",schema_json["table_name"]});
    std::string col_names;
    std::vector<Column> row;
    for (const auto& col : schema_json["columns"])
    {
        Column column;
        column.col_name = col["col_name"];
        column.col_type = col["col_type"];
        column.col_value = 0;
        column.is_primary_key = col["is_primary_key"];
        column.is_null = col["is_null"];
        std::string colName = col["col_name"];
        col_names += colName + "    ";
        row.push_back(column);
    }
    logger->logInfo({col_names});

    std::vector<std::vector<Column>> rows;
    for(auto& row_data : *table)
    {
        char* buffer_ptr = row_data.get();
        std::string print_data;
        for(auto col : row)
        {
            DataType dt = col.col_type;
            uint16_t data_size = 0;
            switch(dt)
            {
            case DataType::INT:
                data_size = sizeof(int);
                int iValue;
                std::memcpy(&iValue, buffer_ptr, data_size);
                col.col_value = iValue;
                print_data += std::to_string(iValue)+"    ";
                buffer_ptr += data_size;
                break;
            case DataType::SHORT:
                data_size = sizeof(short);
                short sValue;
                std::memcpy(&sValue, buffer_ptr, data_size);
                col.col_value = sValue;
                print_data += std::to_string(sValue)+"    ";
                buffer_ptr += data_size;
                break;
            case DataType::CHAR:
                data_size = sizeof(char);
                char cValue;
                std::memcpy(&cValue, buffer_ptr, data_size);
                col.col_value = cValue;
                print_data += std::to_string(cValue)+"    ";
                buffer_ptr += data_size;
                break;
            case DataType::FLOAT:
                data_size = sizeof(float);
                float fValue;
                std::memcpy(&fValue, buffer_ptr, data_size);
                col.col_value = fValue;
                print_data += std::to_string(fValue)+"    ";
                buffer_ptr += data_size;
                break;
            case DataType::BOOLEAN:
                data_size = sizeof(bool);
                bool bValue;
                std::memcpy(&bValue, buffer_ptr, data_size);
                col.col_value = bValue;
                print_data += std::to_string(bValue)+"    ";
                buffer_ptr += data_size;
                break;
            case DataType::STRING:
                {
                    int str_size;
                    std::memcpy(&str_size, buffer_ptr, 2);
                    buffer_ptr += 2;
                    data_size = str_size;
                    std::string strValue(buffer_ptr, data_size);
                    col.col_value = strValue;
                    print_data += strValue+"    ";
                    buffer_ptr += data_size;
                    break;
                }
            default:
               throw std::runtime_error("Invalid DataType");
            }

        }
        logger->logInfo({print_data});
    }
    logger->logInfo({"________________________________"});
}
