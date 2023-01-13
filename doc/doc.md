# Calico DB Documentation

+ [Build](#build)
+ [Examples](#examples)
    + [Slice objects](#slice-objects)
    + [Opening a database](#opening-a-database)
    + [Updating a database](#updating-a-database)
    + [Querying a database](#querying-a-database)
    + [Statistics objects](#statistics-objects)
    + [Destroying a database](#destroying-a-database)
    + [Transaction objects](#transaction-objects)
+ [Architecture](#architecture)
+ [Source Tree](#source-tree)

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

### Slice objects
```C++:examples/api.cpp [15-37]
```

### Opening a database
```C++:examples/api.cpp [42-68]
```

### Updating a database
```C++:examples/api.cpp [73-92]
```

### Querying a database
```C++:examples/api.cpp [98-126]
```

### Transaction objects
```C++:examples/api.cpp [132-160]
```

### Statistics objects
```C++:examples/api.cpp [166-175]
```

### Closing a database
```C++:examples/api.cpp [181-184]
```

### Destroying a database
```C++:examples/api.cpp [196-199]
```

## Architecture
...


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
    + The idea for using a hand-rolled slice object and some of its functionality are from slice.h.
    + Their CMake build process was very instructive.
4. https://github.com/facebook/rocksdb/wiki/Write-Ahead-Log
    + Nice explanation of RocksDB's WAL
    + The idea to have multiple different record types and to use a "remaining" block are from this document
5. https://arpitbhayani.me/blogs/2q-cache
    + Nice description of the 2Q cache replacement policy
6. https://stablecog.com/
    + Used to generate the calico cat image