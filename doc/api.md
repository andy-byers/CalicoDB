# API
Calico DB aims to provide a simple yet robust API for manipulating ordered data.

+ [Opening a Database](#opening-a-database)
+ [Closing a Database](#closing-a-database)
+ [Bytes Objects](#bytes-objects)
+ [Updating a Database](#updating-a-database)
+ [Querying a Database](#querying-a-database)
+ [Errors](#errors)
+ [Transactions](#transactions)
+ [Deleting a Database](#deleting-a-database)

### Opening a Database
Opening a database is a two-step process.
First, we create the database object using a public constructor.
Next, we call `open()` to open the database connection.

```C++
// Create the database object. Note that we could just as easily use a smart pointer or
// new/delete to manage the resource.
calico::Database core;

// Set some options. We'll create a database with pages of size 2 KB and 128 cache frames 
// (1 MB total). We'll also enable logging with spdlog and put our WAL segments in a
// different directory.
calico::Options options;
options.wal_path = "/tmp/cats_wal";
options.page_size = 0x2000;
options.frame_count = 128;
options.log_level = spdlog::level::info;

// Open the database connection.
if (auto s = core.open("/tmp/cats", options); !s.is_ok()) {
    fmt::print(stderr, "{}\n", s.what());
    std::exit(EXIT_FAILURE);
}
// This will be true until db.close() is called.
assert(db.is_open());
```

### Closing a Database

```C++
if (auto s = core.close(); !s.is_ok()) {
    fmt::print(stderr, "{}\n", s.what());
}
```

### Bytes Objects
Calico DB uses `Bytes` and `BytesView` objects to represent unowned, contiguous sequences of bytes.
`Bytes` objects can modify the underlying data while `BytesView` objects cannot.

```C++
auto function_taking_a_bytes_view = [](calico::BytesView) {};

std::string data {"Hello, world!"};

// Construct slices from a string. The string still owns the memory, the slices just refer
// to it.
calico::Bytes b {data.data(), data.size()};
calico::BytesView v {data.data(), data.size()};

// Convenience conversion from a string.
const auto from_string = calico::stob(data);

// Convenience conversion back to a string. This operation may allocate heap memory.
assert(calico::btos(from_string) == data);

// Implicit conversions from `Bytes` to `BytesView` are allowed.
function_taking_a_bytes_view(b);

// advance_cursor() moves the start of the slice forward and truncate moves the end of the slice
// backward.
b.advance_cursor(7).truncate(5);

// Comparisons.
assert(calico::compare_three_way(b, v) != calico::ThreeWayComparison::EQ);
assert(b == calico::stob("world"));
assert(b.starts_with(calico::stob("wor")));

// Bytes objects can modify the underlying string, while BytesView objects cannot.
b[0] = '\xFF';
assert(data[7] == '\xFF');
```

### Updating a Database
Records can be added or removed using methods on the `Database` object.
Keys are unique, so inserting a record that already file_exists will cause modification of the existing value.

```C++
std::vector<calico::Record> records {
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

// Modify a record.
assert(db.insert("cyprus", "all;all").is_ok());

// Erase a record by key.
assert(db.erase("badger").is_ok());

// Erase a record using a cursor (see "Querying a Database" below).
assert(db.erase(core.find_exact("moose")).is_ok());
```

### Querying a Database
The database is queried using cursors returned by the `find*()` methods.

```C++
static constexpr auto target = "russian blue";
const auto key = calico::stob(target);

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

// find() returns a cursor on the first record that does not compare less than the given key.
const auto prefix = key.copy().truncate(key.size() / 2);
assert(db.find(prefix).key() == cursor.key());

// Cursors can be used for range queries. They can traverse the database in sequential order,
// or in reverse sequential order.
for (auto c = db.first(); c.is_valid(); ++c) {}
for (auto c = db.last(); c.is_valid(); --c) {}

// They also support equality comparison.
if (const auto boundary = db.find_exact(key); boundary.is_valid()) {
    for (auto c = db.first(); c.is_valid() && c != boundary; ++c) {}
    for (auto c = db.last(); c.is_valid() && c != boundary; --c) {}
}
```

### Transactions
In Calico DB, transactions are represented by `Transaction` objects.
Any modifying operations that take place while a `Transaction` object T is live will take place within the transaction that T represents.
Otherwise, database modifications behave as if they were atomic, incurring quite a bit of overhead from the additional commit operations.
For this reason, if one wants to modify more than a few records at a time, it is best to do it in a transaction.

```C++
// Start the transaction.
auto xact = db.start();

// Modify the database.
assert(db.erase(db.first()).is_ok());
assert(db.erase(db.last()).is_ok());

// Commit the transaction. If the transaction object goes out of scope before commit() is called,
// it will attempt to abort the transaction.
assert(xact.commit().is_ok());
```

Now imagine a situation where a transaction is unable to be completed for some reason.
Say, for example, that we become unable to write to disk at some point, and an insert fails when rebalancing the tree.
To avoid the possibility of corruption, we must refuse to perform any more work until our state can be guaranteed again.
We can attempt to restore our state by calling `abort()` on the transaction object.
We can call `abort()` as many times as we want, until it succeeds.
If we are unable to abort, we must exit and recover on the next startup.

```C++
auto xact = db.start();

// Fail to insert a record.
auto s = db.insert("key", "value");
assert(s.is_system_error());

// At this point, the database status should reflect this same error.
assert(db.status().is_system_error());

// If we are able to abort, the OK status will be restored, and we can continue using the database.
assert(xact.abort().is_ok());
assert(db.status().is_ok());
```

### Deleting a Database
```C++
// We can delete a database by passing ownership to the following static method.
calico::Database::destroy(std::move(core));
```
