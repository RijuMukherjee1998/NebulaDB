//
// Created by Riju Mukherjee on 08-02-2025.
//

#include "../headers/TableManager.h"

#include "../headers/PageDirectory.h"

Manager::TableManager::TableManager(const std::filesystem::path& currSelectedDBPath, const std::filesystem::path& currSelectedTablePath)
{
    this->currSelectedDBPath = currSelectedDBPath;
    this->currSelectedTablePath = currSelectedTablePath;
    pageDirectory = new StorageEngine::PageDirectory(currSelectedDBPath,currSelectedTablePath);
    logger = Utils::Logger::getInstance();
    pageDirectory->loadPageDirectory();
}

void Manager::TableManager::insertIntoTable()
{
    if (currSelectedTablePath.empty())
    {
        logger->logError({"Table is not selected"});
        return;
    }
    int dummy_data_len = 4096; //4 kb dummy data = 1 page
    pageDirectory->lookIntoPDMap();
    pageDirectory->updateOnInsert(dummy_data_len);
    pageDirectory->updateOnInsert(dummy_data_len);
    pageDirectory->updateOnInsert(dummy_data_len);
    pageDirectory->updateOnInsert(dummy_data_len);
    pageDirectory->updateOnDelete(1,dummy_data_len);
    pageDirectory->updateOnInsert(dummy_data_len);
    pageDirectory->lookIntoPDMap();
    pageDirectory->savePageDirectory(true);
}


void Manager::TableManager::selectAllFromTable()
{

}
