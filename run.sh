#!/usr/bin/env bash

set -Eeuo pipefail

log() {
    echo "$*"
}

fail() {
    echo "❌ $*" >&2
    exit 1
}

have_cmd() {
    command -v "$1" >/dev/null 2>&1
}

version_ge() {
    local actual="$1"
    local required="$2"

    [ "$(printf '%s\n%s\n' "$required" "$actual" | sort -V | head -n 1)" = "$required" ]
}

find_project_dir() {
    local source_path="${BASH_SOURCE[0]}"

    while [ -L "$source_path" ]; do
        local source_dir
        source_dir="$(cd -P -- "$(dirname -- "$source_path")" >/dev/null 2>&1 && pwd)"
        source_path="$(readlink "$source_path")"
        [[ "$source_path" != /* ]] && source_path="$source_dir/$source_path"
    done

    cd -P -- "$(dirname -- "$source_path")" >/dev/null 2>&1 && pwd
}

resolve_vcpkg_root() {
    if [ -n "${VCPKG_ROOT:-}" ]; then
        echo "$VCPKG_ROOT"
        return
    fi

    local candidates=(
        "/opt/vcpkg"
        "/home/vscode/vcpkg"
        "$HOME/vcpkg"
        "$PROJECT_DIR/.vcpkg"
    )

    local candidate
    for candidate in "${candidates[@]}"; do
        if [ -f "$candidate/scripts/buildsystems/vcpkg.cmake" ]; then
            echo "$candidate"
            return
        fi
    done

    echo "$PROJECT_DIR/.vcpkg"
}

ensure_command() {
    local cmd="$1"
    local hint="${2:-}"

    if ! have_cmd "$cmd"; then
        if [ -n "$hint" ]; then
            fail "$cmd not found. $hint"
        fi

        fail "$cmd not found. Please install it and rerun this script."
    fi
}

install_local_cmake() {
    local version="$1"
    local os
    local machine
    local arch
    local cmake_root
    local installer
    local url

    os="$(uname -s)"
    machine="$(uname -m)"

    case "$os" in
        Linux) ;;
        *) fail "CMake $REQUIRED_CMAKE_VERSION+ is required. Automatic CMake install is only supported on Linux; install CMake $REQUIRED_CMAKE_VERSION+ manually." ;;
    esac

    case "$machine" in
        x86_64|amd64) arch="x86_64" ;;
        aarch64|arm64) arch="aarch64" ;;
        *) fail "Unsupported CPU architecture for automatic CMake install: $machine" ;;
    esac

    cmake_root="$PROJECT_DIR/.tools/cmake-$version-linux-$arch"
    CMAKE_BIN="$cmake_root/bin/cmake"
    CTEST_BIN="$cmake_root/bin/ctest"

    if [ -x "$CMAKE_BIN" ]; then
        local local_version
        local_version="$($CMAKE_BIN --version | awk 'NR == 1 {print $3}')"

        if version_ge "$local_version" "$REQUIRED_CMAKE_VERSION"; then
            export PATH="$(dirname -- "$CMAKE_BIN"):$PATH"
            log "✅ Using local CMake $local_version"
            return
        fi
    fi

    log "⚠️ Installing CMake $version locally into $cmake_root..."

    if ! have_cmd curl && ! have_cmd wget; then
        fail "curl or wget is required to install CMake automatically."
    fi

    mkdir -p "$PROJECT_DIR/.tools"
    rm -rf "$cmake_root"
    mkdir -p "$cmake_root"

    installer="$PROJECT_DIR/.tools/cmake-$version-linux-$arch.sh"
    url="https://github.com/Kitware/CMake/releases/download/v$version/cmake-$version-linux-$arch.sh"

    if have_cmd curl; then
        curl -fsSL "$url" -o "$installer"
    else
        wget -q "$url" -O "$installer"
    fi

    chmod +x "$installer"
    "$installer" --skip-license --prefix="$cmake_root"
    rm -f "$installer"

    [ -x "$CMAKE_BIN" ] || fail "CMake install did not create $CMAKE_BIN"
    export PATH="$(dirname -- "$CMAKE_BIN"):$PATH"
    log "✅ CMake installed"
}

ensure_cmake() {
    local required="$REQUIRED_CMAKE_VERSION"
    local system_cmake
    local system_version

    if have_cmd cmake; then
        system_cmake="$(command -v cmake)"
        system_version="$($system_cmake --version | awk 'NR == 1 {print $3}')"

        if version_ge "$system_version" "$required"; then
            CMAKE_BIN="$system_cmake"
            if have_cmd ctest; then
                CTEST_BIN="$(command -v ctest)"
            else
                CTEST_BIN="$(dirname -- "$CMAKE_BIN")/ctest"
            fi
            log "✅ Using CMake $system_version"
            return
        fi

        log "⚠️ Found CMake $system_version, but $required+ is required."
    else
        log "⚠️ cmake not found."
    fi

    install_local_cmake "$CMAKE_BOOTSTRAP_VERSION"
}

ensure_vcpkg() {
    VCPKG_DIR="$(resolve_vcpkg_root)"
    TOOLCHAIN="$VCPKG_DIR/scripts/buildsystems/vcpkg.cmake"

    log "📦 Using vcpkg root: $VCPKG_DIR"

    if [ -d "$VCPKG_DIR" ] && [ -w "$VCPKG_DIR" ]; then
        touch "$VCPKG_DIR/vcpkg.disable-metrics"
    fi

    if [ -f "$TOOLCHAIN" ]; then
        return
    fi

    log "⚠️ vcpkg not found. Installing into $VCPKG_DIR..."
    ensure_command git "git is required to install vcpkg."

    mkdir -p "$(dirname -- "$VCPKG_DIR")"
    git clone https://github.com/microsoft/vcpkg.git "$VCPKG_DIR"

    (
        cd "$VCPKG_DIR"
        git checkout 0ca64b4e1c70fa6d9f53b369b8f3f0843797c20c
        ./bootstrap-vcpkg.sh -disableMetrics
    )

    if [ -w "$VCPKG_DIR" ]; then
        touch "$VCPKG_DIR/vcpkg.disable-metrics"
    fi

    [ -f "$TOOLCHAIN" ] || fail "vcpkg bootstrap did not create $TOOLCHAIN"
    log "✅ vcpkg installed"
}

usage() {
    cat <<EOF
Usage: ./run.sh [clean]

Environment overrides:
  PROJECT_DIR=/path/to/NebulaDB   Project directory. Defaults to this script's directory.
  BUILD_DIR=/path/to/build        Build directory. Defaults to PROJECT_DIR/build.
  VCPKG_ROOT=/path/to/vcpkg       vcpkg root. Auto-detected or installed to PROJECT_DIR/.vcpkg.
  VCPKG_DISABLE_METRICS=1         Disable vcpkg telemetry/metrics. Defaults to 1.
  VCPKG_FORCE_SYSTEM_BINARIES=1   Force vcpkg to use system CMake/Ninja. Defaults to 1.
  REQUIRED_CMAKE_VERSION=3.30.0   Minimum required CMake version.
  CMAKE_BOOTSTRAP_VERSION=3.30.3  Local CMake version to install when needed.
  ENABLE_ASAN=ON|OFF              Enable AddressSanitizer. Defaults to OFF.
  RUN_TESTS=ON|OFF                Run ctest after building. Defaults to ON.
  RUN_APP=ON|OFF                  Run NebulaDB after building. Defaults to ON.
EOF
}

log "🚀 NebulaDB build + run pipeline"

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
    usage
    exit 0
fi

PROJECT_DIR="${PROJECT_DIR:-$(find_project_dir)}"
BUILD_DIR="${BUILD_DIR:-$PROJECT_DIR/build}"
REQUIRED_CMAKE_VERSION="${REQUIRED_CMAKE_VERSION:-3.30.0}"
CMAKE_BOOTSTRAP_VERSION="${CMAKE_BOOTSTRAP_VERSION:-3.30.3}"
ENABLE_ASAN="${ENABLE_ASAN:-OFF}"
RUN_TESTS="${RUN_TESTS:-OFF}"
RUN_APP="${RUN_APP:-ON}"

export VCPKG_DISABLE_METRICS="${VCPKG_DISABLE_METRICS:-1}"
export VCPKG_FORCE_SYSTEM_BINARIES="${VCPKG_FORCE_SYSTEM_BINARIES:-1}"

[ -d "$PROJECT_DIR" ] || fail "Project directory does not exist: $PROJECT_DIR"
cd "$PROJECT_DIR"

# -------------------------------
# ENV CHECK
# -------------------------------
log "🔍 Checking environment..."

ensure_cmake
ensure_command ninja "Install Ninja, for example: sudo apt install ninja-build"
ensure_command g++ "Install g++, for example: sudo apt install g++"

NINJA_BIN="$(command -v ninja)"
CXX_COMPILER_BIN="$(command -v g++)"
if have_cmd gcc; then
    C_COMPILER_BIN="$(command -v gcc)"
elif have_cmd cc; then
    C_COMPILER_BIN="$(command -v cc)"
else
    fail "gcc or cc not found. Install gcc, for example: sudo apt install gcc"
fi

if [ ! -w "$PROJECT_DIR" ]; then
    fail "Project directory is not writable: $PROJECT_DIR"
fi

log "✅ Environment OK"

# -------------------------------
# VCPKG SETUP (SELF-HEALING)
# -------------------------------
ensure_vcpkg

# -------------------------------
# CLEAN (optional)
# -------------------------------
if [[ "${1:-}" == "clean" || "${1:-}" == "--clean" ]]; then
    log "🧹 Cleaning build directory..."
    rm -rf "$BUILD_DIR"
elif [ -n "${1:-}" ]; then
    usage
    fail "Unknown argument: $1"
fi

# -------------------------------
# CONFIGURE
# -------------------------------
log "⚙️ Configuring..."

"$CMAKE_BIN" -B "$BUILD_DIR" -S "$PROJECT_DIR" -G Ninja \
    -DCMAKE_MAKE_PROGRAM="$NINJA_BIN" \
    -DCMAKE_C_COMPILER="$C_COMPILER_BIN" \
    -DCMAKE_CXX_COMPILER="$CXX_COMPILER_BIN" \
    -DCMAKE_TOOLCHAIN_FILE="$TOOLCHAIN" \
    -DCMAKE_BUILD_TYPE=Debug \
    -DENABLE_ASAN="$ENABLE_ASAN"

# -------------------------------
# BUILD
# -------------------------------
log "🔨 Building..."
"$CMAKE_BIN" --build "$BUILD_DIR" --parallel

# -------------------------------
# TESTS
# -------------------------------
if [ "$RUN_TESTS" == "ON" ]; then
    log "🧪 Running tests..."
    "$CTEST_BIN" --test-dir "$BUILD_DIR" --output-on-failure
fi

# -------------------------------
# RUN APP
# -------------------------------
if [ "$RUN_APP" == "ON" ]; then
    APP="$BUILD_DIR/NebulaDB"
    [ -x "$APP" ] || fail "Built app not found or not executable: $APP"

    log "🏃 Running NebulaDB..."

    if [ "$ENABLE_ASAN" == "ON" ]; then
        ASAN_OPTIONS=detect_leaks=0 "$APP"
    else
        "$APP"
    fi
fi

log "✅ Done"
