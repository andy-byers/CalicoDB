![CI status badge](https://github.com/andy-byers/CalicoDB/actions/workflows/actions.yml/badge.svg)

> **Warning**: This library is not yet stable and should **not** be used for anything serious.

> **Note (08/21)**: Check out the develop branch for the latest iteration of the design.
I'm working on the abort and recovery routines right now.
I've also gotten the WAL to write everything in the background and do segmentation, which has improved performance considerably.
Unfortunately, the pager ends up waiting on the WAL to flush more blocks under heavy write workloads.
Once this gets figured out, performance should actually be pretty good!

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
+ Supports variable-length keys and values (with a hard limit on both)
+ API only exposes objects (no pointers to deal with)
+ Allows tuning of various parameters (page size, cache size, etc.)

## Caveats
+ Currently, Calico DB only runs on 64-bit Ubuntu and OSX
+ Has a hard limit on key length, equal to roughly 1/4 of the page size (anywhere from 39 B to 8,167 B)
+ Has a hard limit on value length, equal to roughly 4 GB
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
Note that the tests must be built with assertions, hence the `RelWithAssertions`.
To build the library in release mode, the last command would look like:
```bash
cmake -DCMAKE_BUILD_TYPE=Release -DCCO_BUILD_TESTS=Off .. && cmake --build .
```

## API
See the [API documentation](doc/api.md).

## Performance
Benchmarks are run in a modified version of LevelDB, using the `db_bench` routines.

## TODO
1. Get everything code reviewed!
2. Get unit test coverage up
3. Write documentation
4. Work on performance
    + Could try to do all the writes on a different thread
    + Need to consider how this would impact data integrity after a crash
5. Write a benchmark suite
6. Get the CMake installation to work
7. Need some way to reduce the file size once many pages become unused
    + We need some way to collect freelist pages at the end of the file so that we can truncate

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
The [TODO](#todo) section contains a list of things that need to be addressed, and [design.md](doc/design.md) contains some TODO comments that I thought were important.
Feel free to create a pull request.


