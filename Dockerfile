FROM ubuntu:latest

ENV DEBIAN_FRONTEND=noninteractive
ENV HOME=/fuzz

WORKDIR $HOME/CalicoDB

RUN apt update && \
    apt install -y \
        build-essential ca-certificates clang cmake gcc gdb git vim && \
    update-ca-certificates && \
    rm -rf /var/lib/apt/lists/*

ADD ./CMakeLists.txt ./CMakeLists.txt
ADD ./cmake ./cmake
ADD ./include ./include
ADD ./src ./src
ADD ./test ./test

RUN mkdir build && \
    cd build && \
    export BUILD_TESTING=0 \
    export CC=/usr/bin/clang \
    export CXX=/usr/bin/clang++ && \
    cmake -DCMAKE_CXX_FLAGS="-fsanitize=fuzzer,address,undefined" \
          -DCMAKE_BUILD_TYPE=RelWithAssertions \
          -DCALICODB_BuildTests=On \
          -DCALICODB_BuildFuzzers=On \
          -DCALICODB_BuildUnitTests=Off \
          -DCALICODB_FuzzerStandalone=Off .. && \
    cmake --build . \
