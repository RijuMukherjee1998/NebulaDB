cmake_minimum_required(VERSION 3.30)
project(NebulaDB)

set(CMAKE_CXX_STANDARD 23)

add_executable(NebulaDB main.cpp
        dbmanager/dbmanager.cpp
        dbmanager/dbmanager.h
        include/constants.h)
