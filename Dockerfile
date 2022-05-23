# Modified from https://github.com/mrts/docker-cmake-gtest-valgrind-ubuntu.git

FROM ubuntu:latest

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        vim \
        git \
        build-essential \
        clang-8 clang-format-8 \
        pkg-config \
        cmake \
        valgrind && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists

RUN update-alternatives --install /usr/bin/cc cc /usr/bin/clang-8 100 && \
    update-alternatives --install /usr/bin/c++ c++ /usr/bin/clang++-8 100 && \
    update-alternatives --install /usr/bin/clang clang /usr/bin/clang-8 100 && \
    update-alternatives --install /usr/bin/clang++ clang++ /usr/bin/clang++-8 100 && \
    update-alternatives --install /usr/bin/clang-format clang-format /usr/bin/clang-format-8 100

RUN mkdir CubDB

COPY . ./CubDB

#RUN cmake . && \
#    cmake --build .