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
  + [Errors](#errors)
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
Calico DB depends on `@gabime/spdlog` and `@TartanLlama/expected`.
`spdlog` is downloaded during the build using CMake's FetchContent API, and `expected` is bundled with the source code.

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

### Opening a Database
The entry point to an application using Calico DB might look something like:

```C++
// Set some options. We'll create a database at "tmp/cats" with pages of size 8 KB and 
// 128 cache frames (4 MB total).
cco::Options options;
options.path = "/tmp/cats";
options.page_size = 0x8000;
options.frame_count = 128;

// Create the database object.
cco::Database db {options};

// Open the database connection.
if (const auto s = db.open(); !s.is_ok()) {
    fmt::print("(1/2) cannot open database\n");
    fmt::print("(2/2) (reason) {}\n", s.what());
    std::exit(EXIT_FAILURE);
}
// This will be true until db.close() is called.
assert(db.is_open());
```

### Closing a Database

```C++
assert(db.close().is_ok());
```

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
assert(b.starts_with(cco::stob("wor")));

// Bytes objects can modify the underlying string, while BytesView objects cannot.
b[0] = '\xFF';
assert(data[7] == '\xFF');
```

### Updating a Database
Records and be added or removed using methods on the `Database` object.

```C++
std::vector<cco::Record> records {
    {"bengal", "short;spotted,marbled,rosetted"},
    {"turkish vankedisi", "long;white"},
    {"moose", "???"},
    {"abyssinian", "short;ticked tabby"},
    {"russian blue", "short;blue"},
    {"american shorthair", "short;all"},
    {"badger", "???"},
    {"manx", "short,long;all"},
    {"chantilly-tiffany", "long;solid,tabby"},
    {"cyprus", "..."},
};

// Insert some records.
for (const auto &record: records)
    assert(db.insert(record).is_ok());

// Keys are unique, so inserting a record with an existing key will modify the
// existing value.
assert(db.insert("cyprus", "all;all").is_ok());

// Erase a record by key.
assert(db.erase("badger").is_ok());

// Erase a record using a cursor (see "Querying a Database" below).
assert(db.erase(db.find_exact("moose")).is_ok());
```

### Querying a Database
The database is queried using cursors returned by the `find*()` methods.

```C++
static constexpr auto target = "russian blue";
const auto key = cco::stob(target);

// find_exact() looks for a record that compares equal to the given key and returns a cursor
// pointing to it.
auto cursor = db.find_exact(key);

// If the cursor is valid (i.e. is_valid() returns true) we are safe to use any of the getter
// methods.
assert(cursor.is_valid());
assert(cursor.key() == key);
assert(cursor.value() == "short;blue");

// If we cannot find an exact match, an invalid cursor will be returned.
assert(not db.find_exact("not_found").is_valid());

// If a cursor encounters an error at any point, it will also become invalidated. In this case,
// it will modify its status (returned by cursor.status()) to contain information about the error.
assert(db.find_exact("").status().is_invalid_argument());


// If a cursor encounters an error at any point, it will also become invalidated. In this case,
// it will modify its status (returned by cursor.status()) to contain information about the error.
assert(db.find_exact("").status().is_invalid_argument());

// find() returns a cursor on the first record that does not compare less than the given key.
const auto prefix = key.copy().truncate(key.size() / 2);
assert(db.find(prefix).key() == cursor.key());

// Cursors can be used for range queries. They can traverse the database in sequential order,
// or in reverse sequential order.
for (auto c = db.find_minimum(); c.is_valid(); ++c) {}
for (auto c = db.find_maximum(); c.is_valid(); --c) {}

// They also support equality comparison.
if (const auto boundary = db.find_exact(key); boundary.is_valid()) {
    for (auto c = db.find_minimum(); c.is_valid() && c != boundary; ++c) {}
    for (auto c = db.find_maximum(); c.is_valid() && c != boundary; --c) {}
}
```

### Errors
Methods on the database object that can fail will generally return a `cco::Status` object (similar to and inspired by LevelDB's status object).
If a method returning a cursor encounters an error, the error status will be made available in the cursor's status field.
If an error occurs that could potentially lead to corruption of the database contents, the database object will lock up and refuse to perform any more work.
Rather, the exceptional status that caused the lockup will be returned each time a method call is made.
An error such as this could be caused, for example, by becoming unable to write to disk in the middle of a tree balancing operation.
The lockup can be resolved by a successful call to abort(), which discards updates made during the current transaction.
abort() is reentrant, so it can be called repeatedly.
A good rule of thumb is that if one receives a system error from a call that can modify the database, i.e. insert(), erase(), or commit(), then one should try to abort().
If this isn't possible, it's best to just exit the program.
The next time that the database is started up, it will perform the necessary recovery.

### Transactions
Every modification to a Calico DB database occurs within a transaction.
The first transaction begins when the database is opened, and the last one commits when the database is closed.
Otherwise, transaction boundaries are defined by successful calls to either `commit()` or `abort()`.
Calico DB only provides ACID guarantees and rollback behavior on the current transaction.
After a transaction is committed and all dirty pages are written to disk, the WAL is truncated and a new transaction is started.

```C++
// Commit all the updates we made in the previous examples and begin a new transaction.
assert(db.commit().is_ok());

// Modify the database.
assert(db.insert("opossum", "pretty cute").is_ok());
assert(db.erase("manx").is_ok());

// abort() restores the database to how it looked at the beginning of the transaction.
assert(db.abort().is_ok());

// All updates since the last call to commit() have been reverted.
assert(db.find_exact("opossum").status().is_not_found());
assert(db.find_exact("manx").is_valid());
```

### Deleting a Database
```C++
// We can delete a database by passing ownership to the following static method.
cco::Database::destroy(std::move(db));
```

## Performance
The benchmark suite (still in-progress) prints out each benchmark result in units of operations per second.
We use 16-byte keys and 100-byte values with a 4MB cache (similar to http://www.lmdb.tech/bench/microbench/benchmark.html).
The results are not shown here since they aren't very meaningful without other databases to compare them to (see [TODO](#todo)).
Basically, any routine that modifies the database is going to be pretty slow while using transactions and a persistent database, roughly 30K ops/second.
This number can increase to around 1M ops/second with transactions disabled and an in-memory database.
Reads are pretty fast, and are unaffected by transactions.
We can usually get over 500K ops/second for random reads and several million for sequential reads.

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
┣╸examples ┄┄┄┄┄┄┄┄┄ Examples and use cases
┣╸include/calico
┃ ┣╸bytes.h ┄┄┄┄┄┄┄┄ Constructs for holding contiguous sequences of bytes
┃ ┣╸calico.h ┄┄┄┄┄┄┄ Pulls in the rest of the API
┃ ┣╸common.h ┄┄┄┄┄┄┄ Common types and constants
┃ ┣╸cursor.h ┄┄┄┄┄┄┄ Cursor for database traversal
┃ ┣╸database.h ┄┄┄┄┄ Toplevel database object
┃ ┣╸status.h ┄┄┄┄┄┄┄┄ Status object
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
