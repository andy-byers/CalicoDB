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
ADD test/fuzz ./fuzz
ADD ./include ./include
ADD ./src ./src
ADD ./test ./test
ADD test/tools ./tools

RUN mkdir build && \
    cd build && \
    export CC=/usr/bin/clang \
    export CXX=/usr/bin/clang++ && \
    cmake -DCMAKE_CXX_FLAGS="-fsanitize=fuzzer,address,undefined" \
          -DCMAKE_BUILD_TYPE=Debug \
          -DCALICO_BuildBenchmarks=Off \
          -DCALICO_BUILD_EXAMPLES=Off \
          -DCALICO_BuildFuzzers=On \
          -DCALICO_BuildTests=Off \
          -DCALICO_FuzzerStandalone=Off .. && \
    cmake --build . \
