//
// Created by Riju Mukherjee on 08-02-2025.
//

#ifndef TABLEMANAGER_H
#define TABLEMANAGER_H

#include "Schema.h"
#include "Logger.h"
#include "PageDirectory.h"

namespace Manager
{
    class TableManager {
    private:
        Utils::Logger* logger;
        std::filesystem::path currSelectedTablePath;
        std::filesystem::path currSelectedDBPath;
        StorageEngine::PageDirectory* pageDirectory;
    public:
        TableManager(const std::filesystem::path& currSelectedDBPath, const std::filesystem::path& currSelectedTablePath);
        void insertIntoTable();
        void selectAllFromTable();
        
    };
} //MANAGER




#endif //TABLEMANAGER_H
