# Modified from https://github.com/mrts/docker-cmake-gtest-valgrind-ubuntu.git

FROM ubuntu:latest

# https://askubuntu.com/questions/909277 (Terentev Maksim's answer)
ENV TZ=America/Chicago
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        build-essential \
        clang-8 clang-format-8 \
        cmake \
        gdb \
        git \
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

WORKDIR /CalicoDB
COPY . .

WORKDIR ./build

# Disabling SSL verification via mkebri's answer to https://stackoverflow.com/questions/35821245 for FetchContent to
# work properly. Definitely a bad idea in general, but we're just using this container for fuzz testing.
RUN git config --global http.sslverify false && \
    cmake -E env CXXFLAGS="-fsanitize=fuzzer,address" \
    cmake -DCMAKE_BUILD_TYPE=RelWithAssertions -DCALICO_USE_VALIDATORS=On \
          -DCALICO_BUILD_FUZZERS=ON -DCALICO_FUZZER_LDFLAGS="-fsanitize=fuzzer,address" .. && \
    cmake --build . --config RelWithAssertions --target calico all_fuzzers

