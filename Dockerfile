# Modified from https://github.com/mrts/docker-cmake-gtest-valgrind-ubuntu.git

FROM ubuntu:latest

ARG DEBIAN_FRONTEND=noninteractives

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        build-essential \
        clang-8 clang-format-8 \
        cmake \
        gdb \
        pkg-config \
        valgrind && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists

RUN update-alternatives --install /usr/bin/cc cc /usr/bin/clang-8 100 && \
    update-alternatives --install /usr/bin/c++ c++ /usr/bin/clang++-8 100 && \
    update-alternatives --install /usr/bin/clang clang /usr/bin/clang-8 100 && \
    update-alternatives --install /usr/bin/clang++ clang++ /usr/bin/clang++-8 100 && \
    update-alternatives --install /usr/bin/clang-format clang-format /usr/bin/clang-format-8 100

ENV ASAN_OPTIONS log_path=/tmp/asan_log

RUN mkdir CubDB

COPY . CubDB

RUN mkdir test && \
    cd test && \
    cmake -E env \
      CXXFLAGS="-fsanitize=fuzzer,address" \
      cmake -DCMAKE_BUILD_TYPE=Debug -DCUB_BUILD_FUZZERS=ON \
            -DCUB_FUZZER_LDFLAGS="-fsanitize=fuzzer,address" ../CubDB && \
    cmake --build . --config Debug --target all_fuzzers

