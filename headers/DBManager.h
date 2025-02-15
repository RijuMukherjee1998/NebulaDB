//
// Created by Riju Mukherjee on 31-01-2025.
//

#ifndef DBMANAGER_H
#define DBMANAGER_H
#include <filesystem>
#include <vector>

#include "Schema.h"
#include "Logger.h"
#include "constants.h"

namespace Manager {

    class DBManager {
    public:
        std::filesystem::path getCurrSelectedDBPath() const
        {
            return currSelectedDBPath;
        }
        std::filesystem::path getCurrSelectedTablePath() const
        {
            return currSelectedTablePath;
        }

    private:
        std::filesystem::path currSelectedTablePath;
        std::filesystem::path currSelectedDBPath;
        std::vector<std::filesystem::path> listAllDB() const;
        std::vector<std::filesystem::path> listAllTables() const;
        Utils::Logger* logger;
    public:
        DBManager();
        void showAllDB() const;
        void createDB(const std::string* db_name) const;
        void deleteDB(const std::string* db_name);
        void selectDB(const std::string* dbname);

        void showAllTables() const;
        void selectTable(const std::string* table_name);
        void createTable(const std::string* table_name, const Schema* schema) const;
        void deleteTable(const std::string* table_name) const;
        void insertIntoSelectedTable(const Schema* schema) const;
    };

} // Manager

#endif //DBMANAGER_H
