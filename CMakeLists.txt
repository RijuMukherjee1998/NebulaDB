cmake_minimum_required(VERSION 3.30)
project(NebulaDB)

set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

# Ensure vcpkg toolchain is picked automatically
if(DEFINED ENV{VCPKG_ROOT} AND NOT DEFINED CMAKE_TOOLCHAIN_FILE)
    set(CMAKE_TOOLCHAIN_FILE "$ENV{VCPKG_ROOT}/scripts/buildsystems/vcpkg.cmake"
        CACHE STRING "")
endif()

# Warnings (VERY important for systems code)
add_compile_options(
    -Wall
    -Wextra
    -Wpedantic
    -Werror
)

# Optional: Address Sanitizer
option(ENABLE_ASAN "Enable AddressSanitizer" ON)
if(ENABLE_ASAN)
    add_compile_options(-fsanitize=address -fno-omit-frame-pointer)
    add_link_options(-fsanitize=address)
endif()

# Dependencies
find_package(spdlog CONFIG REQUIRED)
find_package(GTest REQUIRED)

# =========================
# Main Executable
# =========================
add_executable(NebulaDB
        main.cpp
        DbManager/DBManager.cpp
        DbManager/Schema.cpp
        TableManager/TableManager.cpp
        TableManager/PageDirectory.cpp
        StorageEngine/PageCache.cpp
        StorageEngine/DiskManager.cpp
        StorageEngine/Page.cpp
        StorageEngine/Indexer.tpp
        StorageEngine/PageData.cpp
        Utils/ThreadPool.cpp
        Utils/Logger.cpp
)

target_link_libraries(NebulaDB PRIVATE spdlog::spdlog)

# =========================
# Tests
# =========================
add_executable(run_tests
        DbManager/DBManager.cpp
        DbManager/Schema.cpp
        Utils/Logger.cpp
        Utils/ThreadPool.cpp
        TableManager/TableManager.cpp
        TableManager/PageDirectory.cpp
        StorageEngine/Page.cpp
        StorageEngine/PageCache.cpp
        StorageEngine/PageData.cpp
        StorageEngine/DiskManager.cpp
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

enable_testing()
add_test(NAME run_tests COMMAND run_tests)