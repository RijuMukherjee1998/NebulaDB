FROM mcr.microsoft.com/devcontainers/cpp:2-ubuntu24.04

# Install required tools (explicitly include gcc)
RUN apt-get update && apt-get install -y \
    gcc g++ \
    ninja-build \
    pkg-config \
    curl zip unzip tar

# Install CMake >= 3.30
RUN wget https://github.com/Kitware/CMake/releases/download/v3.30.3/cmake-3.30.3-linux-x86_64.sh \
    -O /tmp/cmake.sh && \
    chmod +x /tmp/cmake.sh && \
    /tmp/cmake.sh --skip-license --prefix=/usr/local && \
    rm /tmp/cmake.sh

# Setup vcpkg (PINNED to your baseline)
RUN git clone https://github.com/microsoft/vcpkg.git /opt/vcpkg && \
    cd /opt/vcpkg && \
    git checkout 0ca64b4e1c70fa6d9f53b369b8f3f0843797c20c && \
    ./bootstrap-vcpkg.sh

ENV VCPKG_ROOT=/opt/vcpkg
ENV PATH="$VCPKG_ROOT:$PATH"