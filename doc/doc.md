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
Usage examples can be found in the [examples directory](../examples).

## Architecture
...

## Source Tree
```
CalicoDB
┣╸benchmarks ┄┄┄┄┄┄┄ Performance benchmarks
┣╸cmake ┄┄┄┄┄┄┄┄┄┄┄┄ CMake utilities/config files
┣╸examples ┄┄┄┄┄┄┄┄┄ Examples and use cases
┣╸include/calico
┃ ┣╸calico.h ┄┄┄┄┄┄┄ Pulls in the rest of the API
┃ ┣╸common.h ┄┄┄┄┄┄┄ Common types and constants
┃ ┣╸cursor.h ┄┄┄┄┄┄┄ Cursor for database traversal
┃ ┣╸database.h ┄┄┄┄┄ Toplevel database object
┃ ┣╸options.h ┄┄┄┄┄┄ Options for the toplevel database object
┃ ┣╸slice.h ┄┄┄┄┄┄┄┄ Construct for holding a contiguous sequence of bytes
┃ ┣╸statistics.h ┄┄┄ Query database information and statistics
┃ ┣╸status.h ┄┄┄┄┄┄┄ Status object for function returns
┃ ┣╸storage.h ┄┄┄┄┄┄ Storage interface
┃ ┗╸transaction.h ┄┄ Transaction object
┣╸src
┃ ┣╸core ┄┄┄┄┄┄┄┄┄┄┄ API implementation
┃ ┣╸pager ┄┄┄┄┄┄┄┄┄┄ Database page cache
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