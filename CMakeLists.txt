cmake_minimum_required(VERSION 3.30)
project(NebulaDB)

set(CMAKE_CXX_STANDARD 23)

# Enable AddressSanitizer
#option(ENABLE_ASAN "Enable AddressSanitizer" ON)

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
        headers/ISerializable.h
        headers/Page.h
        StorageEngine/PageCache.cpp
        headers/PageCache.h
        StorageEngine/DiskManager.cpp
        headers/DiskManager.h
        StorageEngine/Page.cpp
)

target_link_libraries(NebulaDB PRIVATE spdlog::spdlog)


# GTest for NebulaDB
find_package(GTest REQUIRED)
find_package(spdlog REQUIRED)
add_executable(run_tests
        Tests/TestDBManager.cpp
        Tests/TestPageDirectory.cpp
        Dbmanager/dbmanager.cpp
        Dbmanager/Schema.cpp
        Utils/Logger.cpp
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
)
target_link_libraries(run_tests
        PRIVATE
        GTest::GTest
        GTest::Main
        spdlog::spdlog
        fmt::fmt
)

#if (ENABLE_ASAN AND MSVC)
#    message(STATUS "ASan enabled")
#    target_compile_options(run_tests PRIVATE /fsanitize=address)
#    link_directories("C:/Program Files (x86)/Microsoft Visual Studio/2022/BuildTools/VC/Tools/Llvm/lib/clang/19/lib/windows")
#    target_link_libraries(run_tests PRIVATE clang_rt.asan-x86_64.lib)
#endif()
# Enable testing
enable_testing()
add_test(NAME run_tests COMMAND run_tests)