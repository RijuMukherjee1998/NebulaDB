cmake_minimum_required(VERSION 3.21...4.1)

set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

# Prevent accidental in-source builds (a common contributor mistake).
if(CMAKE_SOURCE_DIR STREQUAL CMAKE_BINARY_DIR)
    message(FATAL_ERROR
            "In-source builds are not allowed.\n"
            "Use:  cmake -B build -S .   (then build with: cmake --build build)")
endif()

# These must be configured before project(), because project() loads the vcpkg
# toolchain and detects the compiler.
set(ENV{VCPKG_DISABLE_METRICS} "1")
set(ENV{VCPKG_FORCE_SYSTEM_BINARIES} "1")

# Make CMake more robust when the generator is Ninja but PATH does not expose
# the build tool during configure/regenerate.
if(CMAKE_GENERATOR MATCHES "Ninja" AND NOT CMAKE_MAKE_PROGRAM)
    find_program(CMAKE_MAKE_PROGRAM
            NAMES ninja ninja-build
            PATHS /usr/bin /usr/local/bin /bin
    )
    if(CMAKE_MAKE_PROGRAM)
        set(CMAKE_MAKE_PROGRAM "${CMAKE_MAKE_PROGRAM}" CACHE FILEPATH "Ninja build tool" FORCE)
    endif()
endif()

# Pick up vcpkg automatically if the caller did not pass CMAKE_TOOLCHAIN_FILE.
if(NOT DEFINED CMAKE_TOOLCHAIN_FILE)
    set(_NEBULADB_VCPKG_CANDIDATES
            "$ENV{VCPKG_ROOT}"
            "${CMAKE_CURRENT_LIST_DIR}/.vcpkg"
            "/opt/vcpkg"
            "/home/vscode/vcpkg"
            "$ENV{HOME}/vcpkg"
    )
    foreach(_NEBULADB_VCPKG_ROOT IN LISTS _NEBULADB_VCPKG_CANDIDATES)
        if(_NEBULADB_VCPKG_ROOT AND EXISTS "${_NEBULADB_VCPKG_ROOT}/scripts/buildsystems/vcpkg.cmake")
            set(CMAKE_TOOLCHAIN_FILE
                    "${_NEBULADB_VCPKG_ROOT}/scripts/buildsystems/vcpkg.cmake"
                    CACHE FILEPATH "vcpkg toolchain file" FORCE
            )
            message(STATUS "NebulaDB: auto-detected vcpkg at ${_NEBULADB_VCPKG_ROOT}")
            break()
        endif()
    endforeach()

    if(NOT DEFINED CMAKE_TOOLCHAIN_FILE)
        message(WARNING
                "vcpkg toolchain not found. Dependencies (spdlog, nlohmann_json, gtest, fmt) "
                "may fail to resolve.\n"
                "Fix: install vcpkg and either set the VCPKG_ROOT environment variable, or pass\n"
                "  -DCMAKE_TOOLCHAIN_FILE=<vcpkg-root>/scripts/buildsystems/vcpkg.cmake")
    endif()
endif()

# Drop vcpkg's persistent disable-metrics marker when the root is writable.
# This avoids a known abort in some vcpkg builds during compiler detection.
if(DEFINED CMAKE_TOOLCHAIN_FILE AND CMAKE_TOOLCHAIN_FILE MATCHES "vcpkg\\.cmake$")
    get_filename_component(_NEBULADB_VCPKG_BUILDSYSTEMS_DIR "${CMAKE_TOOLCHAIN_FILE}" DIRECTORY)
    get_filename_component(_NEBULADB_VCPKG_SCRIPTS_DIR "${_NEBULADB_VCPKG_BUILDSYSTEMS_DIR}" DIRECTORY)
    get_filename_component(_NEBULADB_VCPKG_ROOT "${_NEBULADB_VCPKG_SCRIPTS_DIR}" DIRECTORY)
    execute_process(
            COMMAND "${CMAKE_COMMAND}" -E touch "${_NEBULADB_VCPKG_ROOT}/vcpkg.disable-metrics"
            OUTPUT_QUIET
            ERROR_QUIET
    )
endif()

project(NebulaDB
        VERSION 0.1.0
        DESCRIPTION "A database system built from scratch in C++23"
        LANGUAGES CXX
)

set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

# Default to Debug if the contributor didn't specify a build type.
if(NOT CMAKE_BUILD_TYPE AND NOT CMAKE_CONFIGURATION_TYPES)
    set(CMAKE_BUILD_TYPE Debug CACHE STRING "Build type" FORCE)
    set_property(CACHE CMAKE_BUILD_TYPE PROPERTY STRINGS Debug Release RelWithDebInfo MinSizeRel)
endif()

# Warnings (VERY important for systems code) — scoped to our targets only,
# so third-party code built by vcpkg is never affected.
set(NEBULADB_WARNINGS -Wall -Wextra -Wpedantic -Werror)

# Options
option(ENABLE_ASAN "Enable AddressSanitizer" ON)
option(NEBULADB_BUILD_TESTS "Build the NebulaDB test suite" ON)

# Dependencies
find_package(spdlog CONFIG REQUIRED)
find_package(nlohmann_json CONFIG REQUIRED)
find_package(fmt CONFIG REQUIRED)

# =========================
# Core library (shared by app and tests)
# =========================
add_library(nebuladb_core STATIC
        DbManager/DBManager.cpp
        DbManager/Schema.cpp
        QueryEngine/Executor.cpp
        QueryEngine/Planner.cpp
        TableManager/TableManager.cpp
        TableManager/PageDirectory.cpp
        StorageEngine/PageCache.cpp
        StorageEngine/DiskManager.cpp
        StorageEngine/Page.cpp
        StorageEngine/PageOps.cpp
        Utils/ThreadPool.cpp
        Utils/Logger.cpp
)

target_include_directories(nebuladb_core PUBLIC
        ${CMAKE_CURRENT_SOURCE_DIR}
        ${CMAKE_CURRENT_SOURCE_DIR}/headers
)

target_compile_options(nebuladb_core PRIVATE ${NEBULADB_WARNINGS})

target_link_libraries(nebuladb_core PUBLIC
        spdlog::spdlog
        nlohmann_json::nlohmann_json
        fmt::fmt
)

if(ENABLE_ASAN)
    target_compile_options(nebuladb_core PUBLIC -fsanitize=address -fno-omit-frame-pointer)
    target_link_options(nebuladb_core PUBLIC -fsanitize=address)
endif()

# =========================
# Main Executable
# =========================
add_executable(NebulaDB main.cpp)
target_compile_options(NebulaDB PRIVATE ${NEBULADB_WARNINGS})
target_link_libraries(NebulaDB PRIVATE nebuladb_core)

# =========================
# Tests (optional: -DNEBULADB_BUILD_TESTS=OFF to skip entirely)
# =========================
if(NEBULADB_BUILD_TESTS)
    enable_testing()

    find_package(GTest CONFIG REQUIRED)

    add_executable(run_tests
            Tests/TestLRUK.cpp
            Tests/TestDBManager.cpp
            Tests/TestPageDirectory.cpp
            Tests/TestBPlusTree.cpp
    )

    target_compile_options(run_tests PRIVATE ${NEBULADB_WARNINGS})

    target_link_libraries(run_tests PRIVATE
            nebuladb_core
            GTest::gtest
            GTest::gtest_main
    )

    add_test(NAME run_tests COMMAND run_tests)
else()
    message(STATUS "NebulaDB: tests disabled (NEBULADB_BUILD_TESTS=OFF)")
endif()