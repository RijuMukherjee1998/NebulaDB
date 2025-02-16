cmake_minimum_required(VERSION 3.30)
project(NebulaDB)

set(CMAKE_CXX_STANDARD 23)

find_package(spdlog CONFIG REQUIRED)
add_executable(NebulaDB main.cpp
        dbmanager/dbmanager.cpp
        headers/DBManager.h
        headers/constants.h
        utils/Logger.cpp
        headers/Logger.h
        dbmanager/Schema.cpp
        headers/Schema.h
        TableManager/TableManager.cpp
        headers/TableManager.h
        TableManager/PageDirectory.cpp
        headers/PageDirectory.h
        headers/ISerializable.h)

target_link_libraries(NebulaDB PRIVATE spdlog::spdlog)


# GTest for NebulaDB
find_package(GTest REQUIRED)

add_executable(run_tests Tests/TestDBManager.cpp
                        Tests/TestPageDirectory.cpp
        dbmanager/dbmanager.cpp
        headers/DBManager.h
        headers/constants.h
        utils/Logger.cpp
        headers/Logger.h
        dbmanager/Schema.cpp
        headers/Schema.h
        TableManager/TableManager.cpp
        headers/TableManager.h
        TableManager/PageDirectory.cpp
        headers/PageDirectory.h
        headers/ISerializable.h)
target_link_libraries(run_tests
        GTest::GTest
        GTest::Main
        spdlog::spdlog
        fmt::fmt
)

# Enable testing
enable_testing()
add_test(NAME run_tests COMMAND run_tests)