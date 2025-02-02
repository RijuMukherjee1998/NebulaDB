cmake_minimum_required(VERSION 3.30)
project(NebulaDB)

set(CMAKE_CXX_STANDARD 23)

find_package(spdlog CONFIG REQUIRED)

add_executable(NebulaDB main.cpp
        dbmanager/dbmanager.cpp
        headers/DBManager.h
        headers/constants.h
        utils/Logger.cpp
        headers/Logger.h)

target_link_libraries(NebulaDB PRIVATE spdlog::spdlog)