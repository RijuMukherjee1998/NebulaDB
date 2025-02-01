//
// Created by teama on 31-01-2025.
//

#ifndef CONSTANTS_H
#define CONSTANTS_H

#ifdef WIN32
const std::string BASE_NDB_PATH = "C:\\ndb\\";
#elif LINUX
const std::string DB_FILE_PATH = "/usr/local/ndb/";
#endif

#define TABLE_FILE_SIZE 1024*1024*1024; //1 GB per table file
#define PAGE_SIZE (1024*4); // 4KB page size

#endif //CONSTANTS_H
