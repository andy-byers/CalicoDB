![CI status badge](https://github.com/andy-byers/CalicoDB/actions/workflows/actions.yml/badge.svg)

> **Warning**: This library is not yet stable and should **not** be used for anything serious.

Calico DB is an embedded key-value database written in C++17.
It exposes a small API that allows storage and retrieval of variable-length byte sequences.

+ [Disclaimer](#disclaimer)
+ [Features](#features)
+ [Caveats](#caveats)
+ [Dependencies](#dependencies)
+ [Build](#build)
+ [API](#api)
  + [Opening a Database](#opening-a-database)
  + [Closing a Database](#closing-a-database)
  + [Bytes Objects](#bytes-objects)
  + [Updating a Database](#updating-a-database)
  + [Querying a Database](#querying-a-database)
  + [Transactions](#transactions)
  + [Deleting a Database](#deleting-a-database)
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
+ Uses a dynamic-order B<sup>+</sup>-tree to store the data in a single file
+ Supports forward and reverse traversal using cursors
+ Allows creation of in-memory databases
+ Supports nearly arbitrary values (up to 4 GB)
+ API only exposes objects (no pointers to deal with)
+ Allows tuning of various parameters (page size, block size, cache size, etc.)

## Caveats
+ Currently, Calico DB only runs on 64-bit Ubuntu and OSX
+ Uses a single WAL file, which can grow quite large in a long-running transaction
+ WAL is only used to ensure ACID properties on the current transaction and is truncated afterward
+ Has a hard limit on key length, equal to roughly 1/4 of the page size (anywhere from ~64 B to ~8 KB)
+ Doesn't support concurrent transactions
+ Doesn't provide synchronization past support for multiple cursors, however `std::shared_mutex` can be used to coordinate writes (see `/test/integration/test_rw.cpp` for an example)

## Dependencies
Calico DB depends on `spdlog` for logging, and `zlib` for compression (***NOT IMPLEMENTED YET***).
Both libraries are downloaded during the build using CMake's FetchContent API.

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
cmake -DCMAKE_BUILD_TYPE=Release -DCALICO_BUILD_TESTS=Off .. && cmake --build .
```

While not yet part of CI, some basic fuzzers (using libFuzzer) are also included.
See the `Dockerfile` for details on how to build them.

## API
**NOTE**: The following examples make use of the alias `namespace cco = calico;`.

### Exceptions
Calico DB uses exceptions for reporting invalid arguments, database corruption, and system-level errors.

### Opening a Database
The entry point to an application using Calico DB might look something like:

```C++
try {
    cco::Options options;
    auto store = cco::Database::open("/tmp/calico", options);
    // Run the application!
} catch (const CorruptionError &error) {
    // This is thrown if corruption is detected in a file.
} catch (const std::invalid_argument &error) {
    // This is thrown if invalid arguments were passed to a Calico DB function.
} catch (const std::system_error &error) {
    // This propagates up from failed system calls.
} catch (const std::exception &error) {
    // This will catch any exception thrown by Calico DB.
}
```

### Closing a Database
Calico DB uses RAII, so databases are closed by letting them go out of scope.
At that point, the database object will automatically commit the most recent transaction.

### Bytes Objects
Calico DB uses `Bytes` and `BytesView` objects to represent unowned byte sequences, a.k.a. slices.
`Bytes` objects can modify the underlying data while `BytesView` objects cannot.

```C++
auto function_taking_a_bytes_view = [](cco::BytesView) {};

std::string data {"Hello, world!"};

// Construct slices from a string. The string still owns the memory, the slices just refer
// to it.
cco::Bytes b {data.data(), data.size()};
cco::BytesView v {data.data(), data.size()};

// Convenience conversion from a string.
const auto from_string = cco::stob(data);

// Convenience conversion back to a string. This operation may allocate heap memory.
assert(cco::btos(from_string) == data);

// Implicit conversions from `Bytes` to `BytesView` are allowed.
function_taking_a_bytes_view(b);

// advance() moves the start of the slice forward and truncate moves the end of the slice
// backward.
b.advance(7).truncate(5);

// Comparisons.
assert(cco::compare_three_way(b, v) != cco::ThreeWayComparison::EQ);
assert(b == cco::stob("world"));

// Bytes objects can modify the underlying string, while BytesView objects cannot.
b[0] = '\xFF';
assert(data[7] == '\xFF');
```

### Updating a Database
Records and be added or removed using methods on the `Database` object.

```C++
// Insert some records. If a record is already in the database, insert() will return false.
assert(db.insert("bengal", "short;spotted,marbled,rosetted"));
assert(db.insert("turkish vankedisi", "long;white"));
assert(db.insert("moose", "???"));
assert(db.insert("abyssinian", "short;ticked tabby"));
assert(db.insert("russian blue", "short;blue"));
assert(db.insert("american shorthair", "short;all"));
assert(db.insert("badger", "???"));
assert(db.insert("manx", "short,long;all"));
assert(db.insert("chantilly-tiffany", "long;solid,tabby"));
assert(db.insert("cyprus", "..."));

// Modify a record.
assert(not db.insert("cyprus", "all;all"));

// Erase a record by key.
assert(db.erase("badger"));

// Erase a record using a cursor (see "Querying a Database" below).
assert(db.erase(db.find_exact("moose")));
```

### Querying a Database
The database is queried using cursors returned by the `find*()` methods.

```C++
static constexpr auto target = "russian blue";
const auto key = cco::stob(target);

// find_exact() looks for a record that compares equal to the given key and returns a cursor
// pointing to it.
auto cursor = db.find_exact(key);
assert(cursor.is_valid());
assert(cursor.key() == key);

// If there isn't such a record, the cursor will be invalid.
assert(not db.find_exact("not found").is_valid());

// find() returns a cursor on the first record that does not compare less than the given key.
const auto prefix = key.copy().truncate(key.size() / 2);
assert(db.find(prefix).key() == cursor.key());

// We can use this method is we just need to check for the existence of a key.
assert(db.contains("bengal"));
assert(not db.contains("moose"));

// Cursors can be used for range queries. They can traverse the database in sequential order,
// or in reverse sequential order.
for (auto c = db.find_minimum(); c.is_valid(); c++) {}
for (auto c = db.find_maximum(); c.is_valid(); c--) {}

// They also support equality comparison.
if (const auto boundary = db.find_exact(key); boundary.is_valid()) {
    for (auto c = db.find_minimum(); c.is_valid() && c != boundary; c++) {}
    for (auto c = db.find_maximum(); c.is_valid() && c != boundary; c--) {}
}
```

### Transactions
Every modification to a Calico DB database occurs within a transaction.
The first transaction begins when the database is opened, and the last one commits when the database is closed.
Otherwise, transaction boundaries are defined by calls to either `commit()` or `abort()`.

```C++
// Commit all the updates we made in the previous examples.
db.commit();

// Make some changes and abort the transaction.
db.insert("opossum", "pretty cute");
assert(db.erase(db.find_minimum()));
assert(db.erase(db.find_maximum()));
db.abort();

// All updates since the last call to commit() have been reverted.
assert(not db.find_exact("opposum").is_valid());
assert(db.find_minimum().key() == cco::stob("abyssinian"));
assert(db.find_maximum().key() == cco::stob("turkish vankedisi"));
```

### Deleting a Database
```C++
// We can delete a database by passing ownership to the following static method.
cco::Database::destroy(std::move(db));
```

## Performance
The benchmark suite (still in-progress) prints out each benchmark result in units of operations per second.
Each database is opened without using direct I/O, as this usually seems to hurt performance (still working on that one).
We use 16-byte keys and 100-byte values with a 4MB cache (similar to http://www.lmdb.tech/bench/microbench/benchmark.html).
We still have a ways to go performance-wise, however, it seems that the cursors provide pretty fast sequential and reverse-sequential reads.

### Benchmark Results
| Name        | Result (ops/sec) |
|:------------|-----------------:|
| write_rand  |           37,853 |
| write_seq   |           40,014 |
| read_rand   |          214,133 |
| read_seq    |        3,622,860 |
| read_rev    |        4,400,077 |
| erase_rand  |           21,290 |
| erase_seq   |           20,449 |


### Benchmark Results (In-Memory Database)
| Name (In-Memory DB) | Result (ops/sec) |
|:--------------------|-----------------:|
| write_rand          |          184,074 |
| write_seq           |          215,414 |
| read_rand           |        1,057,665 |
| read_seq            |        5,052,099 |
| read_rev            |        5,559,223 |
| erase_rand          |           67,573 |
| erase_seq           |           77,081 |

### Benchmark Results (w/o Transactions)
| Name        | Result (ops/sec) |
|:------------|-----------------:|
| write_rand  |          234,945 |
| write_seq   |          384,419 |
| read_rand   |          173,517 |
| read_seq    |        3,610,230 |
| read_rev    |        3,317,678 |
| erase_rand  |           63,421 |
| erase_seq   |           65,919 |

### Benchmark Results (In-Memory Database w/o Transactions)
| Name (In-Memory DB) | Result (ops/sec) |
|:--------------------|-----------------:|
| write_rand          |          609,501 |
| write_seq           |          587,153 |
| read_rand           |          882,593 |
| read_seq            |        4,504,567 |
| read_rev            |        5,249,257 |
| erase_rand          |          365,165 |
| erase_seq           |          333,536 |

## TODO
1. Get everything code reviewed!
2. Get unit test coverage up
3. Write some documentation
4. Add more logging
5. Work on this README
6. Work on the design document
7. Implement optional compression of record values
8. Work on performance
9. Work on the benchmark suite
  + Results may not be all that accurate (really need to benchmark against other databases)
  + Need to test large (100,000 B) values
  + Code is very messy/difficult to change
11. Get the CMake installation to work
12. Implement WAL segmentation
  + WAL should be segmented after it reaches a fixed size, similar to `spdlog`s rotating file sink
  + This should improve the performance of long-running transactions
13. Consider allowing multiple independent trees in a single database (could be either in the same `data` file or separate `data-*` files)

## Design
Internally, Calico DB is broken down into 6 submodules.
Each submodule is represented by a directory in `src`, as shown in the [source tree overview](#source-tree-overview).
See `DESIGN.md` for more information about the design of Calico DB.

## Source Tree Overview
```
CalicoDB
┣╸examples ┄┄┄┄┄┄┄┄┄ Examples and use cases
┣╸include/calico
┃ ┣╸bytes.h ┄┄┄┄┄┄┄┄ Constructs for holding contiguous sequences of bytes
┃ ┣╸calico.h ┄┄┄┄┄┄┄ Pulls in the rest of the API
┃ ┣╸common.h ┄┄┄┄┄┄┄ Common types and constants
┃ ┣╸cursor.h ┄┄┄┄┄┄┄ Cursor for database traversal
┃ ┣╸database.h ┄┄┄┄┄ Toplevel database object
┃ ┣╸exception.h ┄┄┄┄ Public-facing exceptions
┃ ┗╸options.h ┄┄┄┄┄┄ Options for the toplevel database object
┣╸src
┃ ┣╸db ┄┄┄┄┄┄┄┄┄┄┄┄┄ API implementation
┃ ┣╸pool ┄┄┄┄┄┄┄┄┄┄┄ Buffer pool module
┃ ┣╸storage ┄┄┄┄┄┄┄┄ Persistent storage module
┃ ┣╸tree ┄┄┄┄┄┄┄┄┄┄┄ Data organization module
┃ ┣╸utils ┄┄┄┄┄┄┄┄┄┄ Utility module
┃ ┗╸wal ┄┄┄┄┄┄┄┄┄┄┄┄ Write-ahead logging module
┗╸test
  ┣╸benchmark ┄┄┄┄┄┄ Performance benchmarks
  ┣╸fuzz ┄┄┄┄┄┄┄┄┄┄┄ Fuzz tests
  ┣╸integration ┄┄┄┄ Integration tests
  ┣╸recovery ┄┄┄┄┄┄┄ Test database failure and recovery
  ┣╸tools ┄┄┄┄┄┄┄┄┄┄ Test tools
  ┗╸unit_tests ┄┄┄┄┄ Unit tests
```

## Contributions
Contributions are welcome!
Pull requests that fix bugs or address correctness issues will always be considered.
The `TODO` section contains a list of things that need to be addressed, and `DESIGN.md` contains some TODO comments that I thought were important.
Feel free to create a pull request.
