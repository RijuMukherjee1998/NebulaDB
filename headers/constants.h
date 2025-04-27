//
// Created by Riju Mukherjee on 31-01-2025.
//
#ifndef CONSTANTS_H
#define CONSTANTS_H

#include <string>
#ifdef WIN32
    const std::string BASE_NDB_PATH = "C:\\ndb\\";
    const std::string LOG_PATH = "C:\\ndb\\logs.log";
#elif LINUX
    const std::string DB_FILE_PATH = "/usr/local/ndb/";
    const std::string LOG_PATH = "/usr/local/ndb/logs/logs.log";
#endif

#define TABLE_FILE_SIZE 1024*1024*1024; //1 GB per table file
constexpr int PAGE_SIZE = 4096;  // 4KB page size
#define LOGICAL_OVERFLOW 262144
constexpr size_t MAX_PAGES_IN_CACHE = 8192; // 8192 page entries (space required = 32MB)
constexpr short DIRTY_PAGE_TOLERANCE = 512; // if the number of dirty pages in cache is greater than 50% we go for a disk write
#endif //CONSTANTS_H
