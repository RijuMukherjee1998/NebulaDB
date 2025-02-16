//
// Created by teama on 31-01-2025.
//

#ifndef CONSTANTS_H
#define CONSTANTS_H

#ifdef WIN32
    const std::string BASE_NDB_PATH = "C:\\ndb\\";
    const std::string LOG_PATH = "C:\\ndb\\logs.log";
#elif LINUX
    const std::string DB_FILE_PATH = "/usr/local/ndb/";
    const std::string LOG_PATH = "/usr/local/ndb/logs/logs.log";
#endif

#define TABLE_FILE_SIZE 1024*1024*1024; //1 GB per table file
#define PAGE_SIZE 4096; // 4KB page size
#define LOGICAL_OVERFLOW 262144
#endif //CONSTANTS_H
