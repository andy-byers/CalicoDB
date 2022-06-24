![CI status badge](https://github.com/andy-byers/CalicoDB/actions/workflows/actions.yml/badge.svg)

> **Warning**: This library is not yet stable and should **not** be used for anything serious.

Calico DB is an embedded key-value database written in C++17.

+ [Disclaimer](#disclaimer)
+ [Features](#features)
+ [Caveats](#caveats)
+ [Build](#build)
+ [API](#api)
  + [Opening a Database](#opening-a-database)
  + [Closing a Database](#closing-a-database)
  + [Bytes Objects](#bytes-objects)
  + [Updating a Database](#updating-a-database)
  + [Querying a Database](#querying-a-database)
  + [Cursor Objects](#cursor-objects)
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
+ Uses a dynamic-order B-tree to store the data in a single file
+ Supports forward and reverse traversal using cursors
+ Allows creation of in-memory databases
+ Supports arbitrary value sizes
+ API only exposes objects (no pointers to deal with)

## Caveats
+ Currently, Calico DB only runs on 64-bit Ubuntu and OSX
+ Uses a single WAL file, which can grow quite large in a long-running transaction
+ WAL is only used to ensure ACID properties on the current transaction and is truncated afterward
+ Has a limit on key length, equal to roughly 1/4 of the page size
+ Doesn't support concurrent transactions
+ Doesn't provide synchronization past support for multiple cursors, however `std::shared_mutex` can be used to coordinate writes (see `/test/integration/test_rw.cpp` for an example)

## Dependencies
+ spdlog
+ zlib

## Build
We use CMake to gather dependencies and build the project.
In the project root directory, run
```bash
mkdir -p build && cd ./build
```


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

### Exceptions
Calico DB uses exceptions for reporting invalid arguments, database corruption, and system-level errors.

### Opening a Database
The entry point to an application using Calico DB might look something like:

```C++
try {
    calico::Options options;
    auto db = calico::Database::open("/tmp/calico", options);
    // Run the application!
} catch (const CorruptionError &error) {
    // This is thrown if corruption is detected in a file.
} catch (const IOError &error) {
    // This is thrown if we were unable to perform I/O on a file.
} catch (const std::invalid_argument &error) {
    // This is thrown if invalid arguments were passed to a Calico DB function.
} catch (const std::system_error &error) {
    // This propagates up from failed non-I/O system calls.
} catch (const std::exception &error) {
    // This catches any Calico DB exception.
} catch (...) {
    // So does this...
}
```

### Closing a Database
Calico DB uses RAII, so databases are closed by letting them go out of scope.

### Bytes Objects
Calico DB uses `Bytes` and `BytesView` objects to represent unowned byte sequences, a.k.a. slices.
`Bytes` objects can modify the underlying data while `BytesView` objects cannot.

```C++
std::string data {"Hello, bears!"};

// Construct slices from a string. The string still owns the memory, the slices just refer to it.
calico::Bytes b {data.data(), data.size()};
calico::BytesView v {data.data(), data.size()};

// Convenience conversion from a string.
const auto from_string = calico::stob(data);

// Convenience conversion back to a string. This operation must allocate a new string.
assert(calico::btos(from_string) == data);

// Implicit conversions from `Bytes` to `BytesView` are allowed.
function_taking_a_bytes_view(b);

// Advance and truncate with chaining.
b.advance(7).truncate(5);

// Comparisons.
assert(calico::compare_three_way(b, v) == calico::ThreeWayComparison::EQ);
assert(b == calico::stob("bears"));
```

### Updating a Database
Records and be added or removed using methods on the `Database` object.

```C++
// Insert some records.
assert(db.write(calico::stob("chartreux"), calico::stob("grey-blue")));
assert(db.write(calico::stob("manx"), calico::stob("awesome")));
assert(db.write(calico::stob("abyssinian"), calico::stob("cool")));
assert(db.write({"egyptian mau", "respectable"}));
assert(db.write({"ocicat", "rare"}));
assert(db.write({"si-siwat", "lovable"}));

// Update an existing record (keys are always unique). write() returns false if the record was already in the database.
assert(!db.write(calico::stob("grizzly bear"), calico::stob("huge")));

// Erase a record.
assert(db.erase(calico::stob("grizzly bear")));
```

### Querying a Database
The `read*()` methods are provided for querying the database.

```C++
// We can require an exact match.
const auto record = db.read(calico::stob("sun bear"), calico::ThreeWayComparison::EQ);
assert(record->value == "respectable");

// Or, we can look for the first record with a key less than or greater than the given key.
const auto less_than = db.read(calico::stob("sun bear"), calico::ThreeWayComparison::LT);
const auto greater_than = db.read(calico::stob("sun bear"), calico::ThreeWayComparison::GT);
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
const auto key = calico::btos(cursor.key());
const auto value = cursor.value();
printf("Record {%s, %s}\n", key.c_str(), value.c_str()); // Record {black bear, lovable}
```

### Transactions
Every modification to a Calico DB database occurs within a transaction.
The first transaction begins when the database is opened, and the last one commits when the database is closed.
Otherwise, transaction boundaries are defined by calls to either `commit()` or `abort()`.

```C++
db.write(calico::stob("a"), calico::stob("1"));
db.write(calico::stob("b"), calico::stob("2"));
db.commit();

db.write(calico::stob("c"), calico::stob("3"));
assert(db.erase(calico::stob("a")));
assert(db.erase(calico::stob("b")));
db.abort();

// Database still contains {"a", "1"} and {"b", "2"}.
assert(db.read(calico::stob("a"), true)->value == "1");
assert(db.read(calico::stob("b"), true)->value == "2");
```

### Deleting a Database
```C++
// We can delete a database by passing ownership to the following static method.
calico::Database::destroy(std::move(db));
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
4. Work on this README
5. Work on performance
6. Work on the benchmark suite
   + Results may not be all that accurate
   + Need to test large (100,000 B) values
   + Code is very messy/difficult to change

## Design
Internally, Calico DB is broken down into 6 submodules.
Each submodule is represented by a directory in `src`, as shown in [source tree overview](#source-tree-overview).
See `DESIGN.md` for more information about the design of Calico DB.

## Source Tree Overview
```
CalicoDB
┣╸examples ┄┄┄┄┄┄┄┄┄ Examples and use cases
┣╸include/calico
┃ ┣╸bytes.h ┄┄┄┄┄┄┄┄ Slices for holding contiguous sequences of bytes
┃ ┣╸common.h ┄┄┄┄┄┄┄ Common types and constants
┃ ┣╸calico.h ┄┄┄┄┄┄┄┄┄┄ Pulls in the rest of the API
┃ ┣╸cursor.h ┄┄┄┄┄┄┄ Cursor for database traversal
┃ ┣╸database.h ┄┄┄┄┄ Toplevel database object
┃ ┣╸exception.h ┄┄┄┄ Public-facing exceptions
┃ ┗╸options.h ┄┄┄┄┄┄ Options for the toplevel database object
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
Contributions are welcome!
Pull requests that fix bugs or address correctness issues will always be considered.
There are also some things we could try to improve performance, however, I think robustness and guarantee of ACID properties should come first.
The `TODO` section contains a list of things that need to be addressed.
Feel free to create a pull request.
