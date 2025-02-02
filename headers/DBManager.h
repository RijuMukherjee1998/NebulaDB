//
// Created by teama on 31-01-2025.
//

#ifndef DBMANAGER_H
#define DBMANAGER_H
#include <filesystem>
#include <vector>
#include "../headers/Logger.h"

namespace StorageEngine {

class DBManager {
private:
    std::filesystem::path currSelectedDBPath;
    std::filesystem::path currSelectedTablePath;
    const std::vector<std::filesystem::path> listAllDB() const;
    const std::vector<std::filesystem::path> listAllTables() const;
    Utils::Logger* logger = Utils::Logger::getInstance();
public:
    void showAllDB() const;
    void createDB(const std::string* db_name);
    void deleteDB(const std::string* db_name);
    void selectDB(const std::string* dbname);

    void showAllTables() const;
    void createTable(const std::string* table_name);
    void deleteTable(const std::string* table_name);
    void selectTable(const std::string* table_name);
};

} // StorageEngine

#endif //DBMANAGER_H
