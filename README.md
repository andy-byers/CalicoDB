![CI status badge](https://github.com/andy-byers/CalicoDB/actions/workflows/actions.yml/badge.svg)

> **Warning**: This library is not yet stable and should **not** be used for anything serious.

> **Note**: The public API is now stable!

Calico DB is an embedded key-value database written in C++17.
It exposes a small API that allows storage and retrieval of variable-length byte sequences.

+ [Disclaimer](#disclaimer)
+ [Features](#features)
+ [Caveats](#caveats)
+ [Dependencies](#dependencies)
+ [Build](#build)
+ [API](#api)
+ [Performance](#performance)
+ [Design](#design)
+ [TODO](#todo)
+ [Source Tree Overview](#source-tree-overview)
+ [Contributions](#contributions)

## Disclaimer
None of this code has been reviewed, and I am not a professional software developer.
I started writing this library so that I could get better at writing modern C++, since I would like to pursue a career in C++ development.
I've really had a fun time working on Calico DB, and have ended up putting quite a bit of time and effort into it.
Still, it is a work in progress and needs to have some issues addressed before I feel comfortable declaring it usable.
Check out the [Contributions](#contributions) section if you are interested in working on Calico DB!

## Features
+ Durability provided through write-ahead logging
+ Uses a dynamic-order B<sup>+</sup>-tree to organize the data on disk
+ Supports forward and reverse traversal using cursors
+ Allows creation of in-memory databases
+ Supports variable-length keys and values
+ API only exposes objects (no pointers to deal with)
+ Allows tuning of various parameters (page size, cache size, etc.)

## Caveats
+ Currently, Calico DB only runs on 64-bit Ubuntu and OSX
+ Uses a single WAL file, which can grow quite large in a long-running transaction
+ WAL is only used to ensure ACID properties on the current transaction and is truncated afterward
+ Has a hard limit on key length, equal to roughly 1/4 of the page size (anywhere from ~64 B to ~8 KB)
+ Doesn't support concurrent transactions
+ Doesn't provide synchronization past support for multiple cursors

## Dependencies
The library itself depends on `@gabime/spdlog` and `@TartanLlama/expected`.
`spdlog` is downloaded during the build using CMake's FetchContent API, and `expected` is bundled with the source code.
The tests depend on `@google/googletest` and the benchmarks depend on `@google/benchmark`.

## Build
Calico DB is built using CMake.
In the project root directory, run
```bash
mkdir -p build && cd ./build
```

to set up an out-of-source build.
Then
```bash
cmake -DCMAKE_BUILD_TYPE=RelWithAssertions .. && cmake --build .
```

to build the library and tests.
Note that the tests must be built with assertions, hence the "RelWithAssertions".
To build the library in release mode, the last command would look like:
```bash
cmake -DCMAKE_BUILD_TYPE=Release -DCCO_BUILD_TESTS=Off .. && cmake --build .
```

## API
See the [API documentation](doc/api.md).

## Performance
Following are the latest performance benchmarks for Calico DB.
The benchmarks use 16 B keys, 100 B values, pages of size 32 KB, and 4 MB of cache.

### Persistent Database
| Name                  | Result (ops/second) |
|:----------------------|--------------------:|
| RandomWrites          |               1,351 |
| SequentialWrites      |               1,422 |
| RandomBatchWrites     |               7,368 |
| SequentialBatchWrites |              76,958 |
| RandomReads           |             343,406 |
| ForwardIteration      |           2,403,846 |
| ReverseIteration      |           2,386,634 |

### In-Memory Database
| Name                  | Result (ops/second) |
|:----------------------|--------------------:|
| RandomWrites          |              58,041 |
| SequentialWrites      |              73,030 |
| RandomBatchWrites     |              64,532 |
| SequentialBatchWrites |             287,438 |
| RandomReads           |           1,226,993 |
| ForwardIteration      |           3,952,569 |
| ReverseIteration      |           3,968,253 |















## TODO
1. Get everything code reviewed!
2. Get unit test coverage up
3. Write some documentation
4. Add more logging
5. Work on this README
6. Work on the design document
7. Implement optional compression of record values
8. Work on performance
9. Write a benchmark suite
10. Get the CMake installation to work
11. Implement WAL segmentation
  + WAL should be segmented after it reaches a fixed size, similar to `spdlog`s rotating file sink
  + This should improve the performance of long-running transactions
12. Consider allowing multiple independent trees in a single database (could be either in the same `data` file or separate `data-*` files)

## Design
Internally, Calico DB is broken down into 6 submodules.
Each submodule is represented by a directory in `src`, as shown in the [source tree overview](#source-tree-overview).
See `DESIGN.md` for more information about the design of Calico DB.

## Source Tree Overview
```
CalicoDB
┣╸benchmarks ┄┄┄┄┄┄┄ Performance benchmarks
┣╸examples ┄┄┄┄┄┄┄┄┄ Examples and use cases
┣╸include/calico
┃ ┣╸batch.h ┄┄┄┄┄┄┄┄ Represents an atomic unit of work to perform on the database
┃ ┣╸bytes.h ┄┄┄┄┄┄┄┄ Constructs for holding contiguous sequences of bytes
┃ ┣╸calico.h ┄┄┄┄┄┄┄ Pulls in the rest of the API
┃ ┣╸common.h ┄┄┄┄┄┄┄ Common types and constants
┃ ┣╸cursor.h ┄┄┄┄┄┄┄ Cursor for database traversal
┃ ┣╸database.h ┄┄┄┄┄ Toplevel database object
┃ ┣╸status.h ┄┄┄┄┄┄┄ Status object
┃ ┗╸options.h ┄┄┄┄┄┄ Options for the toplevel database object
┣╸src
┃ ┣╸db ┄┄┄┄┄┄┄┄┄┄┄┄┄ API implementation
┃ ┣╸pool ┄┄┄┄┄┄┄┄┄┄┄ Buffer pool module
┃ ┣╸storage ┄┄┄┄┄┄┄┄ Persistent storage module
┃ ┣╸tree ┄┄┄┄┄┄┄┄┄┄┄ Data organization module
┃ ┣╸utils ┄┄┄┄┄┄┄┄┄┄ Utility module
┃ ┗╸wal ┄┄┄┄┄┄┄┄┄┄┄┄ Write-ahead logging module
┗╸test
  ┣╸fuzz ┄┄┄┄┄┄┄┄┄┄┄ Fuzz tests
  ┣╸recovery ┄┄┄┄┄┄┄ Test database failure and recovery
  ┣╸tools ┄┄┄┄┄┄┄┄┄┄ Test tools
  ┗╸unit_tests ┄┄┄┄┄ Unit tests
```

## Contributions
Contributions are welcome!
Pull requests that fix bugs or address correctness issues will always be considered.
The `TODO` section contains a list of things that need to be addressed, and `DESIGN.md` contains some TODO comments that I thought were important.
Feel free to create a pull request.
