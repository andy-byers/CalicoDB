**This library is under active development and is probably broken right now!**

Cub DB is an embedded key-value database written in C++17.

## Disclaimer
This library, in its current form, should **not** be used in production.
Writing a reliable, ACID-compliant database is certainly far from trivial, and I have zero professional software development experience at this time.
Furthermore, this library is under active development: I'm just putting it out there now so that others can help out.
Also, Cub DB uses exceptions, which may not be desirable for some potential users.
I initially began working on this project to better learn modern C++ and exception handling, so some design decisions reflect that goal rather than that of producing a general-purpose DBMS.
I am, however, open to refactoring the project to rely on other forms of error handling, as it would be nice to support embedded systems.
Please see the `Contributions` section if you are interested in working on Cub DB!

## TODO
1. Get the fuzz testing up and running. 
2. Update the B-tree merge routine to be more proactive.
This shouldn't be terribly difficult since most of the unit tests don't rely on the specific tree structure.
They just make sure the tree is correctly ordered and all connections are consistent.
I'm definitely open to suggestions on this one.
3. Write some real documentation.
4. Work on this README
5. 'Reverse pointer map' structure to support 'vacuuming' the database file (see SQLite 3)
6. Better freelist that uses trunk pages (see SQLite 3)

## API

### Creating/Opening a Database
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

### `Bytes` Objects
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

## Features
+ Durability provided through write-ahead logging
+ Uses a dynamic-order B-tree to store the whole database in a single file (excluding the WAL)
+ Supports multiple cursors running concurrently

## Project Source Tree Overview

```
CubDB
  ┣━╸examples
  ┣━╸include
  ┣━╸src
  ┃  ┣━╸db
  ┃  ┣━╸file
  ┃  ┣━╸pool
  ┃  ┣━╸tree
  ┃  ┣━╸utils
  ┃  ┗━╸wal
  ┗━╸test
      ┣━╸benchmark
      ┣━╸fuzz
      ┣━╸integration
      ┣━╸tools
      ┗━╸unit
```

+ `/include`: Public API
+ `/src/db`: API implementation
+ `/src/file`: OS file module
+ `/src/pool`: Buffer pool module
+ `/src/tree`: B-tree module
+ `/src/utils`: Utility module
+ `/src/wal`: Write-ahead logging module
+ `/test/benchmark`: Performance benchmarks
+ `/test/fuzz`: Fuzz tests
+ `/test/integration`: Integration tests
+ `/test/tools`: Test tools
+ `/test/unit`: Unit tests

## Contributions
Contributions are welcomed!
The `TODO` section contains a few things that need to be addressed.
Feel free to create a pull request.


# B-Tree Rules
Insertion and removal are similar to many B-trees.
The main difference is the definitions of "overflowing" and "underflowing" with respect to nodes.
We consider a node to be overflowing when it doesn't have room for the record we are inserting.
The definition for underflowing is a little different.
See `node.cpp` for the exact computation used.

[//]: # (TODO: We may need to eliminate the underflowing state and instead try to 
               rebalance nodes each time we remove. We'll maybe need a redistribution 
               algorithm that hits the nodes' neighbors, or the next 2 from the end if
               it is either the leftmost or rightmost child in its parent, rather than 
               using rotations.)

Right now, nodes may be left underflowing after a rebalance, so it's really more of a heuristic that governs when we will try to merge or rotate, rather than an explicitly-enforced rule.