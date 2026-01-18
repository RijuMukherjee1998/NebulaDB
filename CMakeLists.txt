cmake_minimum_required(VERSION 3.30)
project(NebulaDB)

set(CMAKE_CXX_STANDARD 23)

# Enable AddressSanitizer
#option(ENABLE_ASAN "Enable AddressSanitizer" ON)

find_package(spdlog CONFIG REQUIRED)
add_executable(NebulaDB main.cpp
        DbManager/DBManager.cpp
        DbManager/Schema.cpp
        TableManager/TableManager.cpp
        TableManager/PageDirectory.cpp
        StorageEngine/PageCache.cpp
        StorageEngine/DiskManager.cpp
        StorageEngine/Page.cpp
        Utils/ThreadPool.cpp
        Utils/Logger.cpp
        headers/DBManager.h
        headers/constants.h
        headers/Logger.h
        headers/Schema.h
        headers/TableManager.h
        headers/PageDirectory.h
        headers/ISerializable.h
        headers/Page.h
        headers/PageCache.h
        headers/DiskManager.h
        headers/Column.h
        headers/ThreadPool.h
        headers/LRUK.h
        headers/BPlusTree.h
)

target_link_libraries(NebulaDB PRIVATE spdlog::spdlog)


# GTest for NebulaDB
find_package(GTest REQUIRED)
find_package(spdlog REQUIRED)
add_executable(run_tests
        DbManager/DBManager.cpp
        DbManager/Schema.cpp
        Utils/Logger.cpp
        Utils/ThreadPool.cpp
        TableManager/TableManager.cpp
        TableManager/PageDirectory.cpp
        StorageEngine/Page.cpp                  # ✅ add this
        StorageEngine/PageCache.cpp
        StorageEngine/DiskManager.cpp           # ✅ and this headers (optional to include here, just for IDE indexing)
        headers/DBManager.h
        headers/constants.h
        headers/Logger.h
        headers/Schema.h
        headers/TableManager.h
        headers/PageDirectory.h
        headers/ISerializable.h
        headers/DiskManager.h
        headers/Page.h
        headers/PageCache.h
        headers/Column.h
        headers/LRUK.h
        headers/ThreadPool.h
        headers/BPlusTree.h
        Tests/TestLRUK.cpp
        Tests/TestDBManager.cpp
        Tests/TestPageDirectory.cpp
        Tests/TestBPlusTree.cpp
)
target_link_libraries(run_tests
        PRIVATE
        GTest::GTest
        GTest::Main
        spdlog::spdlog
        fmt::fmt
)

# Enable testing
enable_testing()
add_test(NAME run_tests COMMAND run_tests)