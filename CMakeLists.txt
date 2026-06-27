cmake_minimum_required(VERSION 3.0)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

# These must be configured before project(), because project() loads the vcpkg
# toolchain and detects the compiler.
set(ENV{VCPKG_DISABLE_METRICS} "1")
set(ENV{VCPKG_FORCE_SYSTEM_BINARIES} "1")

# Make CMake/Zed more robust when the generator is Ninja but PATH does not expose
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
      break()
    endif()
  endforeach()
endif()

# Also drop vcpkg's persistent disable-metrics marker when the root is writable.
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

project(NebulaDB)

set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

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
find_package(nlohmann_json CONFIG REQUIRED)
find_package(GTest REQUIRED)

# =========================
# Main Executable
# =========================
add_executable(NebulaDB
        main.cpp
        DbManager/DBManager.cpp
        DbManager/Schema.cpp
        QueryEngine/Executor.cpp
        QueryEngine/Planner.cpp
        TableManager/TableManager.cpp
        TableManager/PageDirectory.cpp
        StorageEngine/PageCache.cpp
        StorageEngine/DiskManager.cpp
        StorageEngine/Page.cpp
        StorageEngine/Indexer.tpp
        StorageEngine/PageOps.cpp
        Utils/ThreadPool.cpp
        Utils/Logger.cpp
)

target_link_libraries(NebulaDB PRIVATE
        spdlog::spdlog
        nlohmann_json::nlohmann_json
)

# =========================
# Tests
# =========================
add_executable(run_tests
        DbManager/DBManager.cpp
        DbManager/Schema.cpp
        QueryEngine/Executor.cpp
        QueryEngine/Planner.cpp
        Utils/Logger.cpp
        Utils/ThreadPool.cpp
        TableManager/TableManager.cpp
        TableManager/PageDirectory.cpp
        StorageEngine/Page.cpp
        StorageEngine/PageCache.cpp
        StorageEngine/PageOps.cpp
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
        nlohmann_json::nlohmann_json
        fmt::fmt
)

enable_testing()
add_test(NAME run_tests COMMAND run_tests)
