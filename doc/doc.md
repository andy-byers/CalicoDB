# Calico DB Documentation

+ [Build](#build)
+ [Examples](#examples)
+ [Architecture](#architecture)
+ [Source Tree](#source-tree)
+ [Acknowledgements](#acknowledgements)

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

## Examples
Examples can be found in the [examples directory](../examples).

## Architecture
CalicoDB is organized as 6 interacting submodules, as illustrated in the [source tree diagram](#source-tree).
The main data store is kept in a single file, called the data file, which is organized as a B<sup>+</sup>-tree.
Info log files, if logging to a file is enabled, are kept in the same directory as the data file.
Write-ahead log (WAL) segment files can be stored with the rest of the database, or in a separate location, depending on user preference.

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
┃ ┣╸options.h ┄┄┄┄┄┄ Options for the toplevel database object
┃ ┣╸platform.h ┄┄┄┄┄ Platform-specific definitions
┃ ┣╸slice.h ┄┄┄┄┄┄┄┄ Constructs for holding contiguous sequences of bytes
┃ ┣╸statistics.h ┄┄┄ Query database information and statistics
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

## Acknowledgements
1. https://cstack.github.io/db_tutorial/
    + Awesome tutorial on database development in C
2. https://www.sqlite.org/arch.html
    + Much of this project was inspired by SQLite3, both the architecture design documents and the source code
    + Especially see the B-tree design document, as well as `btree.h`, `btree.c`, and `btreeInt.h`
3. https://github.com/google/leveldb
    + The slice object and the task queue object are inspired by similar objects in LevelDB
    + Some parts of the CMake build process is taken from their `CMakeLists.txt`
4. https://github.com/facebook/rocksdb/wiki/Write-Ahead-Log
    + Nice explanation of RocksDB's WAL
    + The idea to have multiple different record types and to use a "tail" buffer are from this document
5. https://arpitbhayani.me/blogs/2q-cache
    + Nice description of the 2Q cache replacement policy
6. https://stablecog.com/
    + Used to generate the original calico cat image, which was then further modified to produce [mascot.png](mascot.png)