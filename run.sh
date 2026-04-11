#!/usr/bin/env bash

set -e

echo "🚀 NebulaDB build + run pipeline"

PROJECT_DIR="/workspaces/NebulaDB"
BUILD_DIR="$PROJECT_DIR/build"
VCPKG_DIR="/opt/vcpkg"
TOOLCHAIN="$VCPKG_DIR/scripts/buildsystems/vcpkg.cmake"

ENABLE_ASAN=${ENABLE_ASAN:-OFF}
RUN_TESTS=${RUN_TESTS:-ON}
RUN_APP=${RUN_APP:-ON}

cd "$PROJECT_DIR"

# -------------------------------
# ENV CHECK
# -------------------------------
echo "🔍 Checking environment..."

for cmd in cmake ninja g++; do
    command -v $cmd >/dev/null || {
        echo "❌ $cmd not found. Please fix your container."
        exit 1
    }
done

echo "✅ Environment OK"

# -------------------------------
# PROJECT PERMISSION FIX
# -------------------------------
if [ ! -w "$PROJECT_DIR" ]; then
    echo "⚠️ Fixing project permissions..."
    sudo chown -R vscode:vscode "$PROJECT_DIR"
fi

# -------------------------------
# VCPKG SETUP (SELF-HEALING)
# -------------------------------
echo "📦 Checking vcpkg..."

if [ ! -d "$VCPKG_DIR" ]; then
    echo "⚠️ vcpkg not found. Installing..."

    sudo git clone https://github.com/microsoft/vcpkg.git "$VCPKG_DIR"
    cd "$VCPKG_DIR"

    sudo git fetch --all
    sudo git checkout 0ca64b4e1c70fa6d9f53b369b8f3f0843797c20c

    sudo ./bootstrap-vcpkg.sh
    sudo chown -R vscode:vscode "$VCPKG_DIR"

    cd "$PROJECT_DIR"

    echo "✅ vcpkg installed"
else
    echo "🔧 Ensuring vcpkg permissions..."

    if [ ! -w "$VCPKG_DIR" ]; then
        sudo chown -R vscode:vscode "$VCPKG_DIR"
    fi
fi

# -------------------------------
# CLEAN (optional)
# -------------------------------
if [ "$1" == "clean" ]; then
    echo "🧹 Cleaning build directory..."
    rm -rf "$BUILD_DIR"
fi

# -------------------------------
# CONFIGURE
# -------------------------------
echo "⚙️ Configuring..."

cmake -B "$BUILD_DIR" -S . -G Ninja \
    -DCMAKE_TOOLCHAIN_FILE="$TOOLCHAIN" \
    -DCMAKE_BUILD_TYPE=Debug \
    -DENABLE_ASAN="$ENABLE_ASAN"

# -------------------------------
# BUILD
# -------------------------------
echo "🔨 Building..."
cmake --build "$BUILD_DIR" -j$(nproc)

# -------------------------------
# TESTS
# -------------------------------
if [ "$RUN_TESTS" == "ON" ]; then
    echo "🧪 Running tests..."
    (cd "$BUILD_DIR" && ctest --output-on-failure || true)
fi

# -------------------------------
# RUN APP
# -------------------------------
if [ "$RUN_APP" == "ON" ]; then
    echo "🏃 Running NebulaDB..."

    if [ "$ENABLE_ASAN" == "ON" ]; then
        ASAN_OPTIONS=detect_leaks=0 "$BUILD_DIR/NebulaDB"
    else
        "$BUILD_DIR/NebulaDB"
    fi
fi

echo "✅ Done"