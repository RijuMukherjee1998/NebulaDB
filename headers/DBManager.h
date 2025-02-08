//
// Created by teama on 31-01-2025.
//

#ifndef DBMANAGER_H
#define DBMANAGER_H
#include <filesystem>
#include <vector>

#include "Schema.h"
#include "../headers/Logger.h"

namespace StorageEngine {

class DBManager {
private:
    std::filesystem::path currSelectedDBPath;
    std::filesystem::path currSelectedTablePath;
    std::vector<std::filesystem::path> listAllDB() const;
    std::vector<std::filesystem::path> listAllTables() const;
    Utils::Logger* logger = Utils::Logger::getInstance();
public:
    void showAllDB() const;
    void createDB(const std::string* db_name) const;
    void deleteDB(const std::string* db_name);
    void selectDB(const std::string* dbname);

    void showAllTables() const;
    void createTable(const std::string* table_name, const Schema* schema) const;
    void deleteTable(const std::string* table_name) const;
    void selectTable(const std::string* table_name);
};

} // StorageEngine

#endif //DBMANAGER_H
