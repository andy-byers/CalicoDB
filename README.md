![CI status badge](https://github.com/andy-byers/CubDB/actions/workflows/actions.yml/badge.svg)

> **Warning**: This library is not yet stable and should **not** be used for anything serious.

Cub DB is an embedded key-value database written in C++17.

+ [Disclaimer](#disclaimer)
+ [Features](#features)
+ [Caveats](#caveats)
+ [Build](#build)
+ [API](#api)
  + [Opening a Database](#opening-a-database)
  + [Closing a Database](#closing-a-database)
  + [Slices](#slices)
  + [Updating a Database](#updating-a-database)
  + [Querying a Database](#querying-a-database)
  + [Transactions](#transactions)
+ [Design](#design)
  + [db](#db)
  + [file](#file)
  + [pool](#pool)
  + [tree](#tree)
  + [utils](#utils)
  + [wal](#wal)
+ [TODO](#todo)
+ [Source Tree Overview](#source-tree-overview)
+ [Contributions](#contributions)

## Disclaimer
None of this code has been reviewed, and I am not a professional software developer.
I started writing this library so that I could get better at writing modern C++, since I would like to pursue a career in C++ development.
I've really had a fun time working on Cub DB, and have ended up putting quite a bit of time and effort into it.
Still, it is a work in progress and needs to have some issues addressed before I feel comfortable declaring it usable.
Check out the [Contributions](#contributions) section if you are interested in working on Cub DB!

## Features
+ Durability provided through write-ahead logging
+ Uses a dynamic-order B-tree to store the data in a single file
+ Supports forward and reverse traversal using cursors
+ Supports arbitrarily-sized values

## Caveats
+ Currently only supports 64-bit Ubuntu and OSX
+ Uses a single WAL file, which can grow quite large in a long-running transaction
+ Has a limit on key length, equal to roughly 1/4 of the page size
+ Current reader-writer lock implementation (just using `std::shared_mutex`) does not give preference to writers
  + Each time we perform a modifying operation, an exclusive lock is taken on the database
  + Each time a cursor is opened, a shared lock is taken on the database
  + The shared lock is held for the lifetime of the cursor, so that the tree structure does not change during traversal
  + This means that an open cursor can cause an update to block indefinitely, so care must be taken when coordinating
  + For this reason, it's generally a good idea to keep cursors open for just as long as they are needed

## Build
Cub DB is built using CMake.
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
cmake -DCMAKE_BUILD_TYPE=Release -DCUB_BUILD_TESTS=Off .. && cmake --build .
```

While not yet part of CI, some basic fuzzers (using libFuzzer) are also included.
See the `Dockerfile` for details on how to build them.

## API

### Exceptions
Cub DB uses exceptions for reporting invalid arguments, database corruption, and system-level errors.


### Opening a Database
The entry point to an application using Cub DB might look something like:

```C++
try {
    cub::Options options;
    auto db = cub::Database::open("/tmp/cub", options);
    // Run the application!
} catch (const CorruptionError &error) {
    // This is thrown if corruption is detected in a file.
} catch (const IOError &error) {
    // This is thrown if we were unable to perform I/O on a file.
} catch (const std::invalid_argument &error) {
    // This is thrown if invalid arguments were passed to a Cub DB function.
} catch (const std::system_error &error) {
    // This propagates up from failed non-I/O system calls.
} catch (const std::exception &error) {
    // This catches any Cub DB exception.
} catch (...) {
    // So does this...
}
```

### Closing a Database
Cub DB uses RAII, so closing a database is as simple as letting it go out of scope.

### Slices
Cub DB uses `Bytes` and `BytesView` objects to refer to unowned byte sequences.
`Bytes` objects can modify the underlying data while `BytesView` objects cannot.

```C++
std::string data {"Hello, bears!"};

// Construct a `Bytes` object.
cub::Bytes b {data.data(), data.size()};

// Construct a `BytesView` object.
cub::BytesView v {data.data(), data.size()};

// Convenience conversion from std::string.
const auto from_string = cub::_b(data);

// Convenience conversion back to std::string;
assert(cub::_s(from_string) == data);

// Implicit conversions from `Bytes` to `BytesView` are allowed.
function_taking_a_bytes_view(b);

// Comparisons.
assert(cub::compare_three_way(b, v) == cub::ThreeWayComparison::EQ);
assert(b == v);
```

### Updating a Database
Records and be added or removed using methods on the `Database` object.
For better performance updates, see [Batch Updates](#batch-updates).

```C++
// Insert some records.
assert(db.write(cub::_b("grizzly bear"), cub::_b("big")));
assert(db.write(cub::_b("kodiak bear"), cub::_b("awesome")));
assert(db.write(cub::_b("polar bear"), cub::_b("cool")));
assert(db.write(cub::_b("sun bear"), cub::_b("respectable")));
assert(db.write(cub::_b("panda bear"), cub::_b("rare")));
assert(db.write(cub::_b("black bear"), cub::_b("lovable")));

// Update an existing record (keys are always unique). write() returns false if the record was already in the database.
assert(!db.write(cub::_b("grizzly bear"), cub::_b("huge")));

// Erase a record.
assert(db.erase(cub::_b("grizzly bear")));
```

### Querying a Database
Querying a Cub DB database is performed either through the `read*()` convenience methods, or using a `Cursor` object.
It is possible to have multiple cursors active at once, in multiple threads.
Any updates to the database will block until all cursors have been closed.

```C++
static constexpr bool require_exact {};

// We can require an exact match, or find the first record with a key greater than the given key.
const auto record = db.read(cub::_b("kodiak bear"), require_exact);
assert(record->value == "awesome");

// We can also search for the extrema.
const auto smallest = db.read_minimum();
const auto largest = db.read_maximum();

// The database will be immutable until this cursor is closed.
auto cursor = db.get_cursor();
assert(cursor.has_record());

// Seek to extrema.
cursor.find_maximum();
cursor.find_minimum();

// Forward traversal.
assert(cursor.increment());
assert(cursor.increment(2) == 2);

// Reverse traversal.
assert(cursor.decrement());
assert(cursor.decrement(2) == 2);

// Key and value access. For the key, we first convert to std::string, since key() returns a BytesView.
const auto key = cub::_s(cursor.key());
const auto value = cursor.value();
printf("Record {%s, %s}\n", key.c_str(), value.c_str()); // Record {black bear, lovable}
```

### Batch Updates
Cub DB only supports single writers, so each time we update the database, we incur the overhead of ensuring exclusive access.
To address this issue, we support batch updates through `Batch` objects.
A `Batch` instance will acquire the lock only once, during construction, and hold it until it is destroyed.

```C++
// Create a new batch.
auto batch = db.get_batch();
batch.write(cub::_b("hello"), cub::_b("1"));
batch.write(cub::_b("bears"), cub::_b("2"));
batch.write(cub::_b("world"), cub::_b("3"));

// Checkpoint our changes.
batch.commit();

batch.erase(cub::_b("bears"));

// Discard all changes since the last commit/abort.
batch.abort();

// We can also read from the database using a batch object. This can be useful when we need some read access during an
// atomic update routine.
assert(batch.read(cub::_b("hello"), true)->value == "1");
assert(batch.read(cub::_b("bears"), true)->value == "2");
assert(batch.read(cub::_b("world"), true)->value == "3");
const auto minimum = batch.read_minimum();
const auto maximum = batch.read_maximum();

// Batches automatically commit when they go out of scope. When this one is destroyed, only {"bears", "2"} will be
// persisted.
batch.erase(cub::_b(minimum->key));
batch.erase(cub::_b(maximum->key));
```

### Transactions
Every modification to a Cub DB database occurs within a transaction.
The first transaction begins when the database is opened, and the last one commits when the database is closed.
Otherwise, transaction boundaries are either defined by calls to either `commit()` or `abort()`, or governed by the lifetime of a `Batch` object.

```C++
db.write(cub::_b("a"), cub::_b("1"));
db.write(cub::_b("b"), cub::_b("2"));
db.commit();

db.write(cub::_b("c"), cub::_b("3"));
assert(db.erase(cub::_b("a")));
assert(db.erase(cub::_b("b")));
db.abort();

// Database still contains {"a", "1"} and {"b", "2"}.
assert(db.read(cub::_b("a"), true)->value == "1");
assert(db.read(cub::_b("b"), true)->value == "2");
```

## TODO
1. Get everything code reviewed!
2. Get unit test coverage up
3. Write some documentation
4. Work on the this document
5. Work on performance

## Design

Internally, Cub DB is broken down into 7 submodules.
Each submodule is represented by a directory in `src`, as shown in [source tree overview](#source-tree-overview).

#### `db`
[//]: # (TODO)

#### `file`
[//]: # (TODO)

#### `pool`
[//]: # (TODO)

#### `tree`
[//]: # (TODO)

#### `utils`
[//]: # (TODO)

#### `wal`
[//]: # (TODO)

## Source Tree Overview
```
CubDB
┣╸examples ┄┄┄┄┄┄┄┄┄ Examples and use cases
┣╸include/cub 
┃ ┣╸batch.h ┄┄┄┄┄┄┄┄ Provides batch update functionality 
┃ ┣╸bytes.h ┄┄┄┄┄┄┄┄ Slices for holding contiguous sequences of bytes
┃ ┣╸common.h ┄┄┄┄┄┄┄ Common types and constants
┃ ┣╸cub.h ┄┄┄┄┄┄┄┄┄┄ Pulls in the rest of the API
┃ ┣╸cursor.h ┄┄┄┄┄┄┄ Cursor for database traversal
┃ ┣╸database.h ┄┄┄┄┄ Database connection object
┃ ┗╸exception.h ┄┄┄┄ Public-facing exceptions
┣╸src
┃ ┣╸db ┄┄┄┄┄┄┄┄┄┄┄┄┄ API implementation
┃ ┣╸file ┄┄┄┄┄┄┄┄┄┄┄ OS file module
┃ ┣╸pool ┄┄┄┄┄┄┄┄┄┄┄ Buffer pool module
┃ ┣╸tree ┄┄┄┄┄┄┄┄┄┄┄ B-tree module
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
Contributions are welcomed!
Pull requests that fix bugs or address correctness issues will always be considered.
There are also some things we could try to improve performance, however, I think robustness and guarantee of ACID properties should come first.
The `TODO` section contains a list of things that need to be addressed.
Feel free to create a pull request.
