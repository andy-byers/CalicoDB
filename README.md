![CI status badge](https://github.com/andy-byers/CalicoDB/actions/workflows/actions.yml/badge.svg)

> **Warning**: This library is not quite stable and is definitely not code reviewed. 
> Definitely don't use this library in anything serious!

Calico DB is an embedded key-value database written in C++17.
It exposes a small API that allows storage and retrieval of variable-length byte sequences.

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
+ Currently, Calico DB only runs on 64-bit Ubuntu and OSX
+ Has a hard limit on key length (anywhere from 29 B to ~16 KB, depending on the page size)
+ Has a hard limit on value length, equal to roughly 4 GB
+ Only one transaction may be running at any given time
+ Doesn't provide synchronization past support for concurrent cursors

## Dependencies
The library itself depends on `@gabime/spdlog` and `@TartanLlama/expected`.
`spdlog` is downloaded during the build using CMake's FetchContent API, and `expected` is bundled with the source code.
The tests depend on `@google/googletest`.

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
   <img src="./doc/architecture.png" width="75%" />
</p>

## Performance
Benchmarks are run in a modified version of LevelDB, using the `db_bench` routines.
Below are the results of running some of `db_bench` on Calico DB and SQLite3.

```
Calico DB:  version 0.0.1
SQLite:     version 3.37.2
CPU:        16 * 12th Gen Intel(R) Core(TM) i5-12600K
CPUCache:   20480 KB
```

### Calico DB
> Note: We've had a bit of a silly performance regression.
> I've had to make the commit routine flush the whole page cache so that frequently-used pages don't get too out-of-sync with disk.
> See [TODO](#todo).

| Benchmark                | Result (ops/second) |
|:-------------------------|--------------------:|
| fillseq<sup>1</sup>      |             370,026 |
| fillrandom<sup>1</sup>   |              74,731 |
| overwrite<sup>1</sup>    |             100,204 |
| readrandom               |             424,405 |
| readseq                  |           2,631,493 |
| fillrand100K<sup>1</sup> |                 698 |
| fillseq100K<sup>1</sup>  |                 787 |

<sup>1</sup> These benchmarks were run using a transaction size of 1000.
Performing many modifications outside a transaction is slow, since each operation is atomic.

### SQLite3
| Benchmark    | Result (ops/second) |
|:-------------|--------------------:|
| fillseq      |             352,361 |
| fillrandom   |             186,567 |
| overwrite    |             185,529 |
| readrandom   |             497,265 |
| readseq      |           7,936,508 |
| fillrand100K |               5,185 |
| fillseq100K  |               7,572 |

## TODO
1. Figure out a way to eliminate the call to `flush()` during `commit()`
   + This may involve flushing pages that haven't been flushed since some previous transaction when we acquire them
   + Or, we could do the database file writes in the background as well, with periodic scans for old pages.
2. Get everything code reviewed!
3. Get unit test coverage up
4. Write documentation
5. Work on performance
6. Need some way to reduce the file size once many pages become unused
    + We need some way to collect freelist pages at the end of the file so that we can truncate
    + Look into SQLite's pointer maps

## Documentation
Check out Calico DB's [usage and design documents](./doc).

## Source Tree
```
CalicoDB
┣╸examples ┄┄┄┄┄┄┄┄┄ Examples and use cases
┣╸include/calico
┃ ┣╸bytes.h ┄┄┄┄┄┄┄┄ Constructs for holding contiguous sequences of bytes
┃ ┣╸calico.h ┄┄┄┄┄┄┄ Pulls in the rest of the API
┃ ┣╸common.h ┄┄┄┄┄┄┄ Common types and constants
┃ ┣╸cursor.h ┄┄┄┄┄┄┄ Cursor for database traversal
┃ ┣╸database.h ┄┄┄┄┄ Toplevel database object
┃ ┣╸info.h ┄┄┄┄┄┄┄┄┄ Query information about the database
┃ ┣╸options.h ┄┄┄┄┄┄ Options for the toplevel database object
┃ ┣╸status.h ┄┄┄┄┄┄┄ Status object for function returns
┃ ┣╸store.h ┄┄┄┄┄┄┄┄ Storage interface
┃ ┗╸transaction.h ┄┄ First-class transaction object
┣╸src
┃ ┣╸core ┄┄┄┄┄┄┄┄┄┄┄ API implementation
┃ ┣╸pager ┄┄┄┄┄┄┄┄┄┄ Pager module
┃ ┣╸store ┄┄┄┄┄┄┄┄┄┄ Storage implementations
┃ ┣╸tree ┄┄┄┄┄┄┄┄┄┄┄ Data organization module
┃ ┣╸utils ┄┄┄┄┄┄┄┄┄┄ Utility module
┃ ┗╸wal ┄┄┄┄┄┄┄┄┄┄┄┄ Write-ahead log implementations
┗╸test
  ┣╸fuzz ┄┄┄┄┄┄┄┄┄┄┄ Fuzz tests
  ┣╸recovery ┄┄┄┄┄┄┄ Test database failure and recovery
  ┣╸tools ┄┄┄┄┄┄┄┄┄┄ Test tools
  ┗╸unit_tests ┄┄┄┄┄ Unit tests
```

## Contributions
Contributions are welcome!
The [TODO](#todo) section contains a list of things that need to be addressed.
Also, pull requests that fix bugs or address correctness issues will always be considered.
Feel free to create a pull request.


