![CI status badge](https://github.com/andy-byers/CalicoDB/actions/workflows/actions.yml/badge.svg)

> **Warning**: This library is not yet stable, nor is it code reviewed. 
> Please don't use it for anything serious!

Calico DB is an embedded key-value database written in C++20.
It exposes a small API that allows storage and retrieval of variable-length byte sequences.

<p align="center">
    <img src="doc/mascot.jpeg" width="60%" />
<p align="center">

+ [Features](#features)
+ [Caveats](#caveats)
+ [Dependencies](#dependencies)
+ [Build](#build)
+ [API](#api)
+ [Performance](#performance)
+ [Design](#design)
+ [TODO](#todo)
+ [Source Tree](#source-tree)
+ [Contributions](#contributions)

## Features
+ Durability provided through write-ahead logging
+ Uses a dynamic-order B<sup>+</sup>-tree to organize the data on disk
+ Supports forward and reverse traversal using cursors
+ Supports variable-length keys and values (with a hard limit on both)
+ API only exposes objects (no pointers to deal with)
+ Allows tuning of various parameters (page size, cache size, etc.)
+ Transactions provided as first-class objects

## Caveats
+ Only tested on 64-bit Ubuntu and OSX
+ Hard limit on key length is anywhere from 29 B to ~16 KB, depending on the chosen page size
+ Hard limit on value length is roughly 4 GB
+ Only one transaction may be running at any given time
+ Doesn't provide synchronization past support for concurrent cursors

## Dependencies
The library itself depends on `@gabime/spdlog` and `@TartanLlama/expected`.
The tests depend on `@google/googletest`, and the benchmarks depend on `@google/benchmark`.
All dependencies are downloaded using CMake's FetchContent API.

## Build
Calico DB is built using CMake.
In the project root directory, run
```bash
mkdir -p build && cd ./build
```

followed by
```bash
cmake -DCMAKE_BUILD_TYPE=RelWithAssertions .. && cmake --build .
```

to build the library and tests.
Note that the tests must be built with assertions, hence the `RelWithAssertions`.
To build the library in release mode without tests, the last command would look like:
```bash
cmake -DCMAKE_BUILD_TYPE=Release -DCALICO_BUILD_TESTS=Off .. && cmake --build .
```

## API
See the [API documentation](doc/api.md).

## Architecture

<p align="center">
   <img src="doc/architecture.png" width="75%" />
</p>

## Performance
Calico DB has a way to go performance-wise.
Currently, we have decent performance (>= 200,000 ops/second) in the following categories:
+ Sequential writes
+ Random reads
+ Sequential reads
+ Overwrites

Unfortunately, random writes are quite slow (~6 or 7 times slower than sequential writes).
Writing in reverse-sequential order represents the worst case for the B<sup>+</sup>-tree splitting algorithm, and leads to awful performance.

## TODO
1. Get everything code reviewed!
2. Get unit test coverage up
3. Update the documentation in [doc](doc), or get rid of it in favor of something like Doxygen
4. Work on performance
    + B<sup>+</sup>-tree splitting algorithm could be modified to favor sequential writes less
5. Need some way to reduce the file size once many pages become unused
    + We need some way to collect freelist pages at the end of the file so that we can truncate
    + Look into SQLite's pointer maps

## Documentation
Check out Calico DB's [usage and design documents](doc).

## Source Tree
```
CalicoDB
┣╸benchmarks ┄┄┄┄┄┄┄ Performance benchmarks
┣╸examples ┄┄┄┄┄┄┄┄┄ Examples and use cases
┣╸include/calico
┃ ┣╸calico.h ┄┄┄┄┄┄┄ Pulls in the rest of the API
┃ ┣╸common.h ┄┄┄┄┄┄┄ Common types and constants
┃ ┣╸cursor.h ┄┄┄┄┄┄┄ Cursor for database traversal
┃ ┣╸database.h ┄┄┄┄┄ Toplevel database object
┃ ┣╸info.h ┄┄┄┄┄┄┄┄┄ Query information about the database
┃ ┣╸options.h ┄┄┄┄┄┄ Options for the toplevel database object
┃ ┣╸platform.h ┄┄┄┄┄ Platform-specific definitions
┃ ┣╸slice.h ┄┄┄┄┄┄┄┄ Constructs for holding contiguous sequences of bytes
┃ ┣╸status.h ┄┄┄┄┄┄┄ Status object for function returns
┃ ┣╸storage.h ┄┄┄┄┄┄ Storage interface
┃ ┗╸transaction.h ┄┄ Transaction object
┣╸src
┃ ┣╸core ┄┄┄┄┄┄┄┄┄┄┄ API implementation
┃ ┣╸pager ┄┄┄┄┄┄┄┄┄┄ Cache layer
┃ ┣╸storage ┄┄┄┄┄┄┄┄ Data storage and retrieval
┃ ┣╸tree ┄┄┄┄┄┄┄┄┄┄┄ Data organization
┃ ┣╸utils ┄┄┄┄┄┄┄┄┄┄ Common utilities
┃ ┗╸wal ┄┄┄┄┄┄┄┄┄┄┄┄ Write-ahead logging
┗╸test
  ┣╸recovery ┄┄┄┄┄┄┄ Crash recovery tests
  ┣╸tools ┄┄┄┄┄┄┄┄┄┄ Common test utilities
  ┗╸unit_tests ┄┄┄┄┄ Unit tests
```

## Contributions
Contributions are welcome!
Check out this repo's issues or the [TODO section](#todo) for a list of things that need to be addressed.


