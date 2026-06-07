//
// Created by Riju Mukherjee on 31-01-2025.
//

#ifndef DBMANAGER_H
#define DBMANAGER_H
#include <filesystem>
#include <vector>

//#include "Column.h"
#include "InternalQuery.h"
#include "Schema.h"
#include "Logger.h"
#include "TableManager.h"


namespace Manager {

    class DBManager {
    public:
        std::filesystem::path getCurrSelectedDBPath() const
        {
            return currSelectedDBPath;
        }

    private:
        std::filesystem::path currSelectedDBPath;
        std::vector<std::filesystem::path> listAllDB() const;
        std::vector<std::filesystem::path> listAllTables() const;
        Utils::Logger* logger;
        std::unordered_map<std::string, TableManager*> table_manager_table;
    public:
        DBManager();
        ~DBManager();
        void showAllDB() const;
        void createDB(const std::string* db_name) const;
        void deleteDB(const std::string* db_name);
        void selectDB(const std::string* dbname);
        void shutdownDB();

        void showAllTables() const;
        void createTable(const std::string* table_name,  Schema* schema);
        void deleteTable(const std::string* table_name);
        void executeQueryOnTable(InternalQuery::TableQuery &tbl_query);

    private:
        Manager::TableManager* cacheTableManager(const std::string* table_name);
    };

} // Manager

#endif //DBMANAGER_H
