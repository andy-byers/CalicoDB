> **Warning**: This library is not yet stable and should **not** be used for anything serious.

Cub DB is an embedded key-value database written in C++17.

+ [Disclaimer](#disclaimer)
+ [Features](#features)
+ [Caveats](#caveats)
+ [API](#api)
  + [Opening a Database](#opening-a-database)
  + [Closing a Database](#closing-a-database)
  + [Slices](#slices)
  + [Modifying a Database](#modifying-a-database)
  + [Querying a Database](#querying-a-database)
  + [Transactions](#transactions)
+ [Design](#design)
  + [Architecture](#architecture)
+ [TODO](#todo)
+ [Source Tree Overview](#source-tree-overview)
+ [Contributions](#contributions)

## Disclaimer
I am not, nor have I ever been, a professional software developer.
I'm just a student who is writing a library: both for fun, and as a way to better learn modern C++.
With that being said, I do intend on bringing this library to fruition eventually.
Check out the `Contributions` section if you are interested in working on Cub DB!

## Features
+ Durability provided through write-ahead logging
+ Uses a dynamic-order B-tree to store all the data in a single file
+ Supports forward and reverse traversal using cursors

## Caveats
+ Uses a single WAL file, which can grow quite large in a long-running transaction
+ Current reader-writer lock (`<std::shared_mutex`) does not give preference to writers
  + Each time we perform a modifying operation, an exclusive lock is taken on the database
  + Each time a cursor is opened, a shared lock is taken on the database
  + The lock is held for the lifetime of the cursor, so that the tree structure does not change during traversal
  + This means that an open cursor can cause an update to block indefinitely, so care must be taken when coordinating
  + For this reason, it's generally a good idea to keep cursors open for just as long as they are needed

## API

### Opening a Database
Cub DB uses exceptions for reporting invalid arguments, database corruption, and system-level errors.
The entry point to an application using Cub DB might look something like:

```C++
try {
    cub::Options options;
    auto db = cub::Database::open("/tmp/cub", options);
    // <Run the application!>
} catch (const cub::SystemError &error) {
    // ...
} catch (const cub::CorruptionError &error) {
    // ...
} catch (const cub::Exception &error) {
    // ...
} catch (...) {
    // ...
}
```

### Closing a Database
Cub DB uses RAII, so closing a database is as simple as letting it go out of scope.

### Slices
Cub DB uses slices to refer to unowned byte sequences.
Slices are realized in the `Bytes` and `BytesView` classes.
`Bytes` instances can modify the contents of the underlying array, while `BytesView` instances cannot.

```C++
std::string data {"Hello, world!"};

// Construct two equivalent `Bytes` instances.
cub::Bytes b1 {data.data(), data.size()};
auto b2 = cub::_b(data);
assert(b1 == b2);

// Construct two equivalent `BytesView` instances.
BytesView v1 {data.data(), data.size()};
auto v2 = cub::_b(data);
assert(v1 == v2);

// Convert back to a std::string;
assert(data == cub::_s(b1));

// Implicit conversions from `Bytes` to `BytesView` are allowed.
function_taking_a_bytes_view(b1);

// Comparisons.
assert(cub::compare_three_way(b1, v2) == cub::ThreeWayComparison::EQ);
assert((b1 < v2) == false);
```

### Modifying a Database

```C++
// Insert a new record.
db.insert(cub::_b("a"), cub::_b("1"));

// Modify an existing record (keys are always unique).
db.insert(cub::_b("a"), cub::_b("2"));

// Remove a record.
assert(db.remove(cub::_b("a")));
```

### Querying a Database
Querying a Cub DB database is performed either through the `lookup*()` convenience methods, or using a `Cursor` object.
It is possible to have many cursors active at once (with support for multithreading).
Any modifications to the database will block until all the open cursors have been closed.

```C++
auto record = db.lookup("key", true);
assert(record->value == "vvv");

record = db.lookup_minimum();
assert(record->key == "aaa");

record = db.lookup_maximum();
assert(record->key == "zzz");

auto cursor = db.get_cursor();
assert(cursor.has_record());

// Forward traversal.
assert(cursor.increment());
assert(cursor.increment(3) == 3);

// Reverse traversal.
assert(cursor.decrement());
assert(cursor.decrement(3) == 3);

// Search for extrema.
cursor.find_minimum();
cursor.find_maximum();

// Key and value access.
cursor.key();
cursor.value();
```

### Transactions
Every modification to a Cub DB database occurs within a transaction.
The first transaction begins when the database is opened, and the last one commits when the database is closed.
Otherwise, transaction boundaries are defined by calls to either `commit()` or `abort()`.

```C++
db.insert(cub::_b("a"), cub::_b("1"));
db.insert(cub::_b("b"), cub::_b("2"));
db.commit();

db.insert(cub::_b("c"), cub::_b("3"));
assert(db.remove(cub::_b("a")));
assert(db.remove(cub::_b("b")));
db.abort();

// Database still contains {"a", "1"} and {"b", "2"}.
```

## TODO
1. Write some real documentation.
2. Work on this README
3. 'Reverse pointer map' structure to support 'vacuuming' the database file (see SQLite 3)
4. Better freelist that uses trunk pages (see SQLite 3)
5. Get unit test coverage up way higher

## Design

Internally, Cub DB is broken down into 7 submodules.
Each submodule is represented by a directory in `src`, as shown in the [source tree overview](#source-tree-overview).

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
The `TODO` section contains a few things that need to be addressed.
Feel free to create a pull request.