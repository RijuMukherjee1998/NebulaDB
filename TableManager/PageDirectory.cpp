//
// Created by Riju Mukherjee on 08-02-2025.
//

#include "../headers/PageDirectory.h"

#include <fstream>
#include <iostream>
#include <unordered_set>
StorageEngine::PageDirectory::PageDirectory(const std::filesystem::path& selectedDBPath, const std::filesystem::path& selectedTablePath)
{
    currSelectedDBPath = selectedDBPath;
    currSelectedTablePath = selectedTablePath;
    logger = Utils::Logger::getInstance();
    pgDirPath = currSelectedTablePath/"pgdir.dat";
    pd_map = std::make_unique<std::unordered_map<uint64_t, StorageEngine::PDEntry>>();
    pd_map->clear();
}

StorageEngine::PDEntry StorageEngine::PageDirectory::lookUpPage(const uint64_t logicalPage) const
{
    PDEntry entry;
    if (!pd_map->contains(logicalPage))
    {
        logger->logError({"No entry in pde"});
        return entry;
    }
    entry = (*pd_map)[logicalPage];
    return entry;
}

void StorageEngine::PageDirectory::updateOnInsert(const uint16_t data_size)
{
    uint16_t pg_size = PAGE_SIZE;
    if (data_size > pg_size)
    {
        logger->logError({"Entry too big for DB"});
        return;
    }
    changeCounter++;
    if ((*pd_map)[currentLogicalPage].freeSpace >= data_size)
    {
        (*pd_map)[currentLogicalPage].freeSpace -= data_size;
        return;
    }
    if (findFreeSpace(data_size, currentLogicalPage))
    {
        (*pd_map)[currentLogicalPage].freeSpace -= data_size;
        return;
    }
    PDEntry lastLogicalEntry = (*pd_map)[currentLogicalPage];
    currentLogicalPage += 1;
    PDEntry newEntry;
    newEntry.logicalPage = currentLogicalPage;
    newEntry.fileId = static_cast<uint32_t>(currentLogicalPage / LOGICAL_OVERFLOW);
    newEntry.pageOffset = lastLogicalEntry.pageOffset + pg_size;
    newEntry.freeSpace -= data_size;
    newEntry.exists = true;
    (*pd_map)[currentLogicalPage] = newEntry;
}
void StorageEngine::PageDirectory::updateOnDelete(const uint64_t logical_page, const uint16_t data_size) const
{
    if ((*pd_map)[logical_page].freeSpace == 4096)
    {
        logger->logError({"Weird logical page already free"});
        return;
    }
    (*pd_map)[logical_page].freeSpace += data_size;
}
bool StorageEngine::PageDirectory::findFreeSpace(const uint16_t data_size, uint64_t& logicalPage) const
{
    uint16_t pg_size = PAGE_SIZE;
    for (const auto& entry : *pd_map)
    {
        if (entry.second.freeSpace == pg_size)
        {
            logicalPage = entry.second.logicalPage;
            return true;
        }
    }
    return false;
}

void StorageEngine::PageDirectory::savePageDirectory(bool forcedSave)
{
    if (changeCounter > 1024 || forcedSave)
    {
        changeCounter = 0;
        serialize();
        logger->logInfo({"Page Directory Saved Successfully"});
    }
}

void StorageEngine::PageDirectory::loadPageDirectory()
{
    std::vector<StorageEngine::PDEntry> entries = deserialize();
    setCurrentLogicalPage(entries);
    if (entries.size() == 0)
    {
        PDEntry entry;
        entry.logicalPage = 0;
        entry.fileId = 0;
        entry.pageOffset = 0;
        entry.freeSpace = 4096;
        entry.exists = true;
        (*pd_map)[currentLogicalPage] = entry;
    }
    for(auto entry : entries)
    {
        if ((*pd_map).find(entry.logicalPage) == (*pd_map).end())
        {
            (*pd_map)[entry.logicalPage] = entry;
        }
        else
        {
            logger->logError({"Weird ... duplicate entries found on load"});
        }
    }
}

void StorageEngine::PageDirectory::serialize()
{
    // want to re-write the entire file not append to it so std::ios::trunc option is used
    std::ofstream pd_file(pgDirPath.string(), std::ios::binary|std::ios::trunc);
    if (!pd_file)
    {
        logger->logCritical({"Page directory could not be opened for write: " ,currSelectedDBPath.string()});
        throw std::runtime_error("Page directory could not be opened for write");
    }
    const size_t count = pd_map->size();
    pd_file.write(reinterpret_cast<const char*>(&count), sizeof(count));
    for (const auto& entry : *pd_map)
    {
        PDEntry entryToWrite = entry.second;
        pd_file.write(reinterpret_cast<const char*>(&entryToWrite), sizeof(entryToWrite));
    }
    pd_file.close();
}

std::vector<StorageEngine::PDEntry> StorageEngine::PageDirectory::deserialize()
{
    std::ifstream pd_file(pgDirPath.string(),std::ios::binary);
    if (!pd_file)
    {
        logger->logCritical({"Page directory could not be opened: " + currSelectedDBPath.string()});
        throw std::runtime_error("Page directory could not be opened");
    }
    //read the no of entries
    size_t count;
    pd_file.read(reinterpret_cast<char*>(&count), sizeof(count));
    std::vector<StorageEngine::PDEntry> entries(count);
    for (auto& entry : entries)
    {
        pd_file.read(reinterpret_cast<char*>(&entry), sizeof(StorageEngine::PDEntry));
    }
    pd_file.close();
    return entries;
}

void StorageEngine::PageDirectory::lookIntoPDMap()const
{
    for (const auto& entry : *pd_map)
    {
        std::cout << entry.first << " : " << entry.second.freeSpace << std::endl;
    }
    std::cout<< "____________________________________" <<std::endl;
}