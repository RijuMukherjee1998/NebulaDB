//
// Created by Riju Mukherjee on 08-02-2025.
//

#include "../headers/TableManager.h"

#include <iostream>

#include "../headers/PageDirectory.h"
#include "../headers/Schema.h"
#include "../headers/Page.h"
#include "../headers/PageCache.h"
Manager::TableManager::TableManager(const std::filesystem::path& currSelectedDBPath, const std::filesystem::path& currSelectedTablePath, const Schema* schema):
    tSchema(*schema)
{
    this->currSelectedDBPath = currSelectedDBPath;
    this->currSelectedTablePath = currSelectedTablePath;
    pageDirectory = new StorageEngine::PageDirectory(currSelectedDBPath, currSelectedTablePath);
    pageCache = new StorageEngine::PageCache(currSelectedTablePath, pageDirectory);
    logger = Utils::Logger::getInstance();
    pageDirectory->loadPageDirectory();
}

void Manager::TableManager::insertIntoTable(std::vector<Column>& columns) const
{
    uint16_t totalBytes = 0;
    if (currSelectedTablePath.empty())
    {
        logger->logError({"Table is not selected"});
        return;
    }
    for (const auto& column : columns)
    {
        if (!isColumnTypeMatching(column, column.col_type))
        {
            logger->logError({"Bad column type ... not matching schema"});
            return;
        }
        addTotalBytes(column, totalBytes);
    }
    std::vector<char> buffer;
    std::vector<char>* bufferPtr = &buffer;
    const std::vector<char>* currBufferPtr = bufferPtr;
    uint16_t currBytesLeft = totalBytes;
    for (Column& column : columns)
    {
        valueToBuffer(column, bufferPtr, currBytesLeft);
    }

    pageDirectory->updateOnInsert(totalBytes);
    const uint64_t currLogicalPageId = pageDirectory->currentLogicalPage;
    const std::shared_ptr<StorageEngine::Page> currPage = pageCache->getPageFromCache(currLogicalPageId);
    currPage->insertIntoPage(currBufferPtr, totalBytes);
    pageCache->markPageDirty(currLogicalPageId);
}


void Manager::TableManager::selectAllFromTable() const
{
    const auto rows = new std::vector<std::unique_ptr<char[]>>();
    const uint64_t currLogicalPageId = pageDirectory->currentLogicalPage;
    for (int i=0; i<= currLogicalPageId; i++)
    {
        const std::shared_ptr<StorageEngine::Page> currPage = pageCache->getPageFromCache(i);
        currPage->getAllDataFromPage(rows);
    }
    // These functions will be put in the client side later.
    printTableData(rows);
}

void Manager::TableManager::flushAll() const
{
    pageCache->flushDirtyPages();
    pageDirectory->savePageDirectory(true);
}
void Manager::TableManager::valueToBuffer(const Column& column, std::vector<char>* buffer, uint16_t& remainingSize) const
{
    try {
        auto appendToBuffer = [&](const void* data, const size_t size) {
            if (remainingSize >= size) {
                buffer->insert(buffer->end(), static_cast<const char*>(data), static_cast<const char*>(data) + size);
                remainingSize -= size;
            } else {
                throw std::runtime_error("Insufficient buffer space!");
            }
        };

        switch (column.col_type) {
        case DataType::BOOLEAN: {
                const auto value = std::any_cast<bool>(column.col_value);
                appendToBuffer(&value, sizeof(bool));
                break;
        }
        case DataType::CHAR: {
                const auto value = std::any_cast<char>(column.col_value);
                appendToBuffer(&value, sizeof(char));
                break;
        }
        case DataType::SHORT: {
                const auto value = std::any_cast<short>(column.col_value);
                appendToBuffer(&value, sizeof(short));
                break;
        }
        case DataType::INT: {
                const auto value = std::any_cast<int>(column.col_value);
                appendToBuffer(&value, sizeof(int));
                break;
        }
        case DataType::FLOAT: {
                const auto value = std::any_cast<float>(column.col_value);
                appendToBuffer(&value, sizeof(float));
                break;
        }
        case DataType::STRING: {
                const auto value = std::any_cast<std::string>(column.col_value);
                const size_t str_size = value.length();
                appendToBuffer(&str_size, 2);
                appendToBuffer(value.c_str(), value.size());
                break;
        }
        default:
            throw std::invalid_argument("Unsupported DataType!");
        }
    } catch (const std::bad_any_cast& e) {
        logger->logError({"Bad any_cast: ", e.what()});
    } catch (const std::exception& e) {
        logger->logError({"Unhandled exception: ", e.what()});
    }
}
