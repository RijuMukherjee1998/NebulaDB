# 🚀 NebulaDB
![nebula_icon.jpg](nebula_icon.jpg)

NebulaDB is a database system built from scratch in C++23, focused on understanding real database internals such as storage engines, indexing, and memory management.

🧠 Overview

  NebulaDB is a systems-level project implementing core DBMS components from scratch:

  Page-based storage engine
  Disk + memory management
  B+ Tree indexing
  Table and schema handling

  The goal is to build a correct, deterministic, low-level database system, not just a feature-complete one.

📦 Current Features

    Storage Engine
    Fixed-size page layout (4KB pages)
    DiskManager for persistent storage
    PageCache for buffering pages in memory
    Table & Schema
    Database → Table → Page hierarchy
    Schema definition with column metadata
    Indexing
    B+ Tree implementation
    Multi-type key support (std::variant)
    Point and range queries
    Utilities
    ThreadPool
    Logging (spdlog)

⚙️ Setup & Run Guide  ✅ Recommended: Dev Container Requirements:

    Docker Desktop
  
    Visual Studio Code
  
    Dev Containers extension

🔧 Setup

 - git clone -b new_code https://github.com/RijuMukherjee1998/NebulaDB.git
 - cd NebulaDB
 - Open in VS Code:
 - Ctrl + Shift + P → Dev Containers: Reopen in Container

🚀 Running NebulaDB

 - NebulaDB provides a self-healing script (run.sh) that handles setup, build, testing, and execution.

 ▶️ Default Run

    - ./run.sh
    - ✔ Builds project
    - ✔ Runs tests
    - ✔ Executes NebulaDB

  🧹 Clean Build

    - ./run.sh clean
    - ✔ Deletes previous build
    - ✔ Reconfigures everything from scratch

  🧪 Run Without Tests

    - RUN_TESTS=OFF ./run.sh

  ⚡ Run Without Executing App

    - RUN_APP=OFF ./run.sh

  🧠 Enable AddressSanitizer

    - ENABLE_ASAN=ON ./run.sh
    (Currently leak detection is disabled automatically)

  🔧 Combine Options

    - ENABLE_ASAN=ON RUN_TESTS=OFF ./run.sh clean

  🛠️ Manual Build (Optional)

    - cmake -B build -S . -G Ninja \
        -DCMAKE_TOOLCHAIN_FILE=/opt/vcpkg/scripts/buildsystems/vcpkg.cmake

    - cmake --build build

    - ./build/NebulaDB

📌 Notes

  - Use the container environment for consistent builds
  - Avoid using sudo during build/run
  - run.sh is the recommended workflow

# 📜 License

  NebulaDB is licensed under the **GNU General Public License v2.0 (GPL-2.0)**.

  This means:
  - You can use, modify, and distribute the code
  - Any derivative work must also be open-sourced under GPL

  See the [LICENSE](LICENSE) file for details.