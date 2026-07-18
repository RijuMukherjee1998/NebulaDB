#!/usr/bin/env bash

set -Eeuo pipefail

# -------------------------------
# Logging & error handling
# -------------------------------
log() {
    echo "$*"
}

fail() {
    echo "❌ $*" >&2
    exit 1
}

on_error() {
    local exit_code=$?
    echo "❌ run.sh failed at line $1 (exit code $exit_code)." >&2
    echo "   Re-run with:  bash -x ./run.sh  for verbose tracing," >&2
    echo "   or try:       ./run.sh clean    to rebuild from scratch." >&2
    exit "$exit_code"
}
trap 'on_error $LINENO' ERR

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

package_hint() {
    # Best-effort install hint for the contributor's distro.
    local pkgs="$*"
    if have_cmd apt-get; then
        echo "sudo apt-get install -y $pkgs"
    elif have_cmd dnf; then
        echo "sudo dnf install -y $pkgs"
    elif have_cmd pacman; then
        echo "sudo pacman -S --needed $pkgs"
    elif have_cmd apk; then
        echo "sudo apk add $pkgs"
    elif have_cmd brew; then
        echo "brew install $pkgs"
    else
        echo "install: $pkgs (using your system package manager)"
    fi
}

ensure_command() {
    local cmd="$1"
    local pkg="${2:-$1}"
    if ! have_cmd "$cmd"; then
        fail "'$cmd' not found. Try:  $(package_hint "$pkg")"
    fi
}

# -------------------------------
# Toolchain checks
# -------------------------------
check_compiler_version() {
    # C++23 needs GCC >= 13 or Clang >= 17 for reasonable support.
    local cxx="$1"
    local version_line major name

    version_line="$("$cxx" --version 2>/dev/null | head -n 1)" || return 0

    if [[ "$version_line" == *clang* ]]; then
        name="Clang"
        major="$("$cxx" -dumpversion 2>/dev/null | cut -d. -f1)"
        if [ -n "$major" ] && [ "$major" -lt 17 ]; then
            log "⚠️ $name $major detected. C++23 support may be incomplete (Clang 17+ recommended)."
        fi
    else
        name="GCC"
        major="$("$cxx" -dumpversion 2>/dev/null | cut -d. -f1)"
        if [ -n "$major" ] && [ "$major" -lt 13 ]; then
            fail "$name $major is too old for C++23 (GCC 13+ required).
   Try:  $(package_hint g++-13)  then rerun with:  CXX=g++-13 ./run.sh"
        fi
    fi
}

resolve_compilers() {
    # Honor CXX/CC env vars; otherwise prefer g++/gcc, fall back to clang++/clang.
    if [ -n "${CXX:-}" ]; then
        have_cmd "$CXX" || fail "CXX is set to '$CXX' but it is not on PATH."
        CXX_COMPILER_BIN="$(command -v "$CXX")"
    elif have_cmd g++; then
        CXX_COMPILER_BIN="$(command -v g++)"
    elif have_cmd clang++; then
        CXX_COMPILER_BIN="$(command -v clang++)"
    else
        fail "No C++ compiler found. Try:  $(package_hint g++)"
    fi

    if [ -n "${CC:-}" ]; then
        have_cmd "$CC" || fail "CC is set to '$CC' but it is not on PATH."
        C_COMPILER_BIN="$(command -v "$CC")"
    elif have_cmd gcc; then
        C_COMPILER_BIN="$(command -v gcc)"
    elif have_cmd clang; then
        C_COMPILER_BIN="$(command -v clang)"
    elif have_cmd cc; then
        C_COMPILER_BIN="$(command -v cc)"
    else
        fail "No C compiler found. Try:  $(package_hint gcc)"
    fi

    check_compiler_version "$CXX_COMPILER_BIN"
    log "✅ C++ compiler: $CXX_COMPILER_BIN"
}

resolve_ninja() {
    # Some distros name the binary 'ninja-build'.
    if have_cmd ninja; then
        NINJA_BIN="$(command -v ninja)"
    elif have_cmd ninja-build; then
        NINJA_BIN="$(command -v ninja-build)"
    else
        fail "Ninja not found. Try:  $(package_hint ninja-build)"
    fi
}

install_local_cmake() {
    local version="$1"
    local os machine arch cmake_root installer url

    os="$(uname -s)"
    machine="$(uname -m)"

    case "$os" in
        Linux) ;;
        *) fail "CMake $REQUIRED_CMAKE_VERSION+ is required. Automatic install is Linux-only; please install CMake $REQUIRED_CMAKE_VERSION+ manually." ;;
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
        local_version="$("$CMAKE_BIN" --version | awk 'NR == 1 {print $3}')"
        if version_ge "$local_version" "$REQUIRED_CMAKE_VERSION"; then
            export PATH="$cmake_root/bin:$PATH"
            log "✅ Using local CMake $local_version"
            return
        fi
    fi

    log "⚠️ Installing CMake $version locally into $cmake_root ..."

    if ! have_cmd curl && ! have_cmd wget; then
        fail "curl or wget is required to download CMake. Try:  $(package_hint curl)"
    fi

    mkdir -p "$PROJECT_DIR/.tools"
    rm -rf "$cmake_root"
    mkdir -p "$cmake_root"

    installer="$PROJECT_DIR/.tools/cmake-$version-linux-$arch.sh"
    url="https://github.com/Kitware/CMake/releases/download/v$version/cmake-$version-linux-$arch.sh"

    if have_cmd curl; then
        curl -fsSL --retry 3 "$url" -o "$installer"
    else
        wget -q "$url" -O "$installer"
    fi

    chmod +x "$installer"
    "$installer" --skip-license --prefix="$cmake_root" >/dev/null
    rm -f "$installer"

    [ -x "$CMAKE_BIN" ] || fail "CMake install did not create $CMAKE_BIN"
    export PATH="$cmake_root/bin:$PATH"
    log "✅ CMake $version installed"
}

ensure_cmake() {
    if have_cmd cmake; then
        local system_cmake system_version
        system_cmake="$(command -v cmake)"
        system_version="$("$system_cmake" --version | awk 'NR == 1 {print $3}')"

        if version_ge "$system_version" "$REQUIRED_CMAKE_VERSION"; then
            CMAKE_BIN="$system_cmake"
            if have_cmd ctest; then
                CTEST_BIN="$(command -v ctest)"
            else
                CTEST_BIN="$(dirname -- "$CMAKE_BIN")/ctest"
            fi
            log "✅ Using CMake $system_version"
            return
        fi
        log "⚠️ Found CMake $system_version, but $REQUIRED_CMAKE_VERSION+ is required."
    else
        log "⚠️ cmake not found."
    fi

    install_local_cmake "$CMAKE_BOOTSTRAP_VERSION"
}

# -------------------------------
# vcpkg (self-healing)
# -------------------------------
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

check_vcpkg_prereqs() {
    # vcpkg needs these to bootstrap and to build ports; missing ones cause
    # confusing failures deep inside dependency builds, so check up front.
    local missing=()
    local dep
    for dep in git curl zip unzip tar pkg-config; do
        have_cmd "$dep" || missing+=("$dep")
    done

    if [ "${#missing[@]}" -gt 0 ]; then
        fail "Missing tools required by vcpkg: ${missing[*]}
   Try:  $(package_hint "${missing[@]}")"
    fi
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

    log "⚠️ vcpkg not found. Installing into $VCPKG_DIR ..."
    check_vcpkg_prereqs

    mkdir -p "$(dirname -- "$VCPKG_DIR")"
    git clone https://github.com/microsoft/vcpkg.git "$VCPKG_DIR"

    (
        cd "$VCPKG_DIR"
        if [ -n "$VCPKG_COMMIT" ]; then
            git checkout "$VCPKG_COMMIT"
        fi
        ./bootstrap-vcpkg.sh -disableMetrics
    )

    if [ -w "$VCPKG_DIR" ]; then
        touch "$VCPKG_DIR/vcpkg.disable-metrics"
    fi

    [ -f "$TOOLCHAIN" ] || fail "vcpkg bootstrap did not create $TOOLCHAIN"
    log "✅ vcpkg installed"
}

# -------------------------------
# Stale cache self-healing
# -------------------------------
heal_stale_cache() {
    # If the build dir was configured with a different generator or toolchain
    # (e.g., by an IDE), CMake refuses to reconfigure. Detect and wipe it.
    local cache="$BUILD_DIR/CMakeCache.txt"
    [ -f "$cache" ] || return 0

    local cached_generator cached_toolchain cached_home
    cached_generator="$(grep -E '^CMAKE_GENERATOR:' "$cache" | cut -d= -f2- || true)"
    cached_toolchain="$(grep -E '^CMAKE_TOOLCHAIN_FILE:' "$cache" | cut -d= -f2- || true)"
    cached_home="$(grep -E '^CMAKE_HOME_DIRECTORY:' "$cache" | cut -d= -f2- || true)"

    local reason=""
    if [ -n "$cached_generator" ] && [ "$cached_generator" != "Ninja" ]; then
        reason="generator changed ($cached_generator -> Ninja)"
    elif [ -n "$cached_toolchain" ] && [ "$cached_toolchain" != "$TOOLCHAIN" ]; then
        reason="vcpkg toolchain changed"
    elif [ -n "$cached_home" ] && [ "$cached_home" != "$PROJECT_DIR" ]; then
        reason="source directory moved"
    fi

    if [ -n "$reason" ]; then
        log "🧹 Build cache is stale ($reason). Wiping $BUILD_DIR ..."
        rm -rf "$BUILD_DIR"
    fi
}

usage() {
    cat <<EOF
Usage: ./run.sh [clean] [-h|--help]

Arguments:
  clean                           Delete the build directory before configuring.

Environment overrides:
  PROJECT_DIR=/path/to/NebulaDB   Project directory. Defaults to this script's directory.
  BUILD_DIR=/path/to/build        Build directory. Defaults to PROJECT_DIR/build.
  BUILD_TYPE=Debug|Release|...    CMake build type. Defaults to Debug.
  CXX=g++-13 / CC=gcc-13          Compiler overrides.
  VCPKG_ROOT=/path/to/vcpkg       vcpkg root. Auto-detected or installed to PROJECT_DIR/.vcpkg.
  VCPKG_COMMIT=<hash>             Pin vcpkg to a commit when auto-installing. Empty = latest.
  REQUIRED_CMAKE_VERSION=3.21.0   Minimum required CMake version.
  CMAKE_BOOTSTRAP_VERSION=3.30.5  Local CMake version to install when needed.
  ENABLE_ASAN=ON|OFF              Enable AddressSanitizer. Defaults to OFF.
  RUN_TESTS=ON|OFF                Run tests after building. Defaults to ON.
  RUN_APP=ON|OFF                  Run NebulaDB after building. Defaults to ON.
  JOBS=<n>                        Parallel build jobs. Defaults to all cores.

Examples:
  ./run.sh                        Build, test, and run.
  ./run.sh clean                  Full rebuild from scratch.
  RUN_TESTS=OFF ./run.sh          Skip tests.
  ENABLE_ASAN=ON ./run.sh clean   Clean ASan build.
EOF
}

# -------------------------------
# Main
# -------------------------------
log "🚀 NebulaDB build + run pipeline"

CLEAN=0
for arg in "$@"; do
    case "$arg" in
        -h|--help) usage; exit 0 ;;
        clean|--clean) CLEAN=1 ;;
        *) usage; fail "Unknown argument: $arg" ;;
    esac
done

PROJECT_DIR="${PROJECT_DIR:-$(find_project_dir)}"
BUILD_DIR="${BUILD_DIR:-$PROJECT_DIR/build}"
BUILD_TYPE="${BUILD_TYPE:-Debug}"
REQUIRED_CMAKE_VERSION="${REQUIRED_CMAKE_VERSION:-3.21.0}"
CMAKE_BOOTSTRAP_VERSION="${CMAKE_BOOTSTRAP_VERSION:-3.30.5}"
VCPKG_COMMIT="${VCPKG_COMMIT:-0ca64b4e1c70fa6d9f53b369b8f3f0843797c20c}"
ENABLE_ASAN="${ENABLE_ASAN:-OFF}"
RUN_TESTS="${RUN_TESTS:-ON}"
RUN_APP="${RUN_APP:-ON}"
JOBS="${JOBS:-$(nproc 2>/dev/null || sysctl -n hw.ncpu 2>/dev/null || echo 2)}"

export VCPKG_DISABLE_METRICS="${VCPKG_DISABLE_METRICS:-1}"
export VCPKG_FORCE_SYSTEM_BINARIES="${VCPKG_FORCE_SYSTEM_BINARIES:-1}"

[ -d "$PROJECT_DIR" ] || fail "Project directory does not exist: $PROJECT_DIR"
[ -f "$PROJECT_DIR/CMakeLists.txt" ] || fail "No CMakeLists.txt in $PROJECT_DIR — is PROJECT_DIR correct?"
[ -w "$PROJECT_DIR" ] || fail "Project directory is not writable: $PROJECT_DIR (avoid running with sudo)"
cd "$PROJECT_DIR"

# -------------------------------
# ENV CHECK
# -------------------------------
log "🔍 Checking environment..."
ensure_cmake
resolve_ninja
resolve_compilers
log "✅ Environment OK"

# -------------------------------
# VCPKG SETUP (SELF-HEALING)
# -------------------------------
ensure_vcpkg

# -------------------------------
# CLEAN / STALE CACHE
# -------------------------------
if [ "$CLEAN" -eq 1 ]; then
    log "🧹 Cleaning build directory..."
    rm -rf "$BUILD_DIR"
else
    heal_stale_cache
fi

# -------------------------------
# CONFIGURE
# -------------------------------
log "⚙️ Configuring ($BUILD_TYPE)..."
"$CMAKE_BIN" -B "$BUILD_DIR" -S "$PROJECT_DIR" -G Ninja \
    -DCMAKE_MAKE_PROGRAM="$NINJA_BIN" \
    -DCMAKE_C_COMPILER="$C_COMPILER_BIN" \
    -DCMAKE_CXX_COMPILER="$CXX_COMPILER_BIN" \
    -DCMAKE_TOOLCHAIN_FILE="$TOOLCHAIN" \
    -DCMAKE_BUILD_TYPE="$BUILD_TYPE" \
    -DENABLE_ASAN="$ENABLE_ASAN" \
    -DNEBULADB_BUILD_TESTS="$RUN_TESTS"


# -------------------------------
# BUILD
# -------------------------------
log "🔨 Building (jobs: $JOBS)..."
"$CMAKE_BIN" --build "$BUILD_DIR" --parallel "$JOBS"

# -------------------------------
# TESTS
# -------------------------------
if [ "$RUN_TESTS" == "ON" ]; then
    log "🧪 Running tests..."
    export ASAN_OPTIONS="${ASAN_OPTIONS:-detect_leaks=0}"
    if [ -x "$CTEST_BIN" ]; then
        "$CTEST_BIN" --test-dir "$BUILD_DIR" --output-on-failure
    elif [ -x "$BUILD_DIR/run_tests" ]; then
        log "⚠️ ctest not found; running test binary directly."
        "$BUILD_DIR/run_tests"
    else
        fail "Neither ctest nor $BUILD_DIR/run_tests is available."
    fi
else
    log "⏭️ Skipping tests (RUN_TESTS=OFF): test targets were not built."
fi


# -------------------------------
# RUN APP
# -------------------------------
if [ "$RUN_APP" == "ON" ]; then
    APP="$BUILD_DIR/NebulaDB"
    [ -x "$APP" ] || fail "Built app not found or not executable: $APP"

    log "🏃 Running NebulaDB..."
    if [ "$ENABLE_ASAN" == "ON" ]; then
        ASAN_OPTIONS="${ASAN_OPTIONS:-detect_leaks=0}" "$APP"
    else
        "$APP"
    fi
fi

log "✅ Done"
