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
  + [Slice Objects](#slice-objects)
  + [Updating a Database](#updating-a-database)
  + [Querying a Database](#querying-a-database)
  + [Cursor Objects](#cursor-objects)
  + [Transactions](#transactions)
+ [Performance](#performance)
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
+ Currently, Cub DB only runs on 64-bit Ubuntu and OSX
+ Uses a single WAL file, which can grow quite large in a long-running transaction
+ Has a limit on key length, equal to roughly 1/4 of the page size
+ Does not provide internal synchronization past multiple readers, however external synchronization can be used to allow writes (see `/test/integration/test_rw.cpp`)

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
Cub DB uses RAII, so databases are closed by letting them go out of scope.

### Slice Objects
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
assert(cub::compare_three_way(b, v) == cub::Comparison::EQ);
assert(b == v);
```

### Updating a Database
Records and be added or removed using methods on the `Database` object.

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
The `read*()` methods are provided for querying the database.

```C++
// We can require an exact match.
const auto record = db.read(cub::_b("sun bear"), cub::Comparison::EQ);
assert(record->value == "respectable");

// Or, we can look for the first record with a key less than or greater than the given key.
const auto less_than = db.read(cub::_b("sun bear"), cub::Comparison::LT);
const auto greater_than = db.read(cub::_b("sun bear"), cub::Comparison::GT);
assert(less_than->value == "cool");

// Whoops, there isn't a key greater than "sun bear".
assert(greater_than == std::nullopt);

// We can also search for the minimum and maximum.
const auto smallest = db.read_minimum();
const auto largest = db.read_maximum();
```

### Cursor Objects
Cursors can be used to find records and traverse the database.

```C++
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

### Transactions
Every modification to a Cub DB database occurs within a transaction.
The first transaction begins when the database is opened, and the last one commits when the database is closed.
Otherwise, transaction boundaries are defined by calls to either `commit()` or `abort()`.

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

## Performance
The benchmark suite (still in-progress) prints out each benchmark result in units of operations per second.
Each database is opened without using direct I/O, as this usually seems to hurt performance (still working on that one).
We use 16-byte keys and 100-byte values with a 4MB cache (similar to http://www.lmdb.tech/bench/microbench/benchmark.html).
We still have a ways to go performance-wise, however, it seems that the cursors provide pretty fast sequential and reverse-sequential reads.

```
.--------------------------.--------------------------.
| Name                     | Result (ops/second)      |
|--------------------------|--------------------------|
| write_rand               |                   37,185 |
| write_seq                |                   57,079 |
| read_rand                |                  161,717 |
| read_seq                 |                3,611,615 |
| read_rev                 |                4,429,556 |
| erase_all_rand           |                   25,911 |
| erase_all_seq            |                   26,941 |
| erase_half_rand          |                   22,306 |
| erase_half_seq           |                   22,230 |
'--------------------------'--------------------------'

.--------------------------.--------------------------.
| Name (In-Memory DB)      | Result (ops/second)      |
|--------------------------|--------------------------|
| write_rand               |                   36,368 |
| write_seq                |                   55,883 |
| read_rand                |                  335,235 |
| read_seq                 |                4,156,060 |
| read_rev                 |                4,672,760 |
| erase_all_rand           |                   53,372 |
| erase_all_seq            |                   70,807 |
| erase_half_rand          |                   58,205 |
| erase_half_seq           |                   50,895 |
'--------------------------'--------------------------'
```

## TODO
1. Get everything code reviewed!
2. Get unit test coverage up
3. Write some documentation
4. Work on this README
5. Work on performance
6. Work on the benchmark suite
   + Results may not be all that accurate
   + Need to test large (100,000 B) values
   + Code is very messy/difficult to change

## Design

Internally, Cub DB is broken down into 6 submodules.
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
