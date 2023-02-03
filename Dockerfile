FROM ubuntu:latest

ENV DEBIAN_FRONTEND=noninteractive
ENV HOME=/fuzz

WORKDIR $HOME/CalicoDB

RUN apt update && \
    apt install -y \
        build-essential ca-certificates clang cmake gcc gdb git vim && \
    update-ca-certificates && \
    rm -rf /var/lib/apt/lists/*

RUN mkdir build && \
    cd build && \
    cmake -DCMAKE_CXX_FLAGS="-fsanitize=fuzzer,address,undefined" \
          -DCMAKE_BUILD_TYPE=RelWithAssertions \
          -DCALICO_BUILD_BENCHMARKS=Off \
          -DCALICO_BUILD_EXAMPLES=Off \
          -DCALICO_BUILD_FUZZERS=On \
          -DCALICO_BUILD_TESTS=Off .. && \
    cmake --build . \
