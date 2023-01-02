# API
Calico DB aims to provide a simple yet robust API for working with persistent, ordered data.

+ [Bytes Objects](#bytes-objects)
+ [Opening a Database](#opening-a-database)
+ [Updating a Database](#updating-a-database)
+ [Querying a Database](#querying-a-database)
+ [Transactions](#transactions)
+ [Closing a Database](#closing-a-database)
+ [Deleting a Database](#deleting-a-database)

### Bytes Objects
Calico DB has to deal with a lot of byte sequences (keys, values, etc.).
We generally use `std::string` to represent owned byte sequences, and a hand-rolled slice object (either `Bytes` or `BytesView`) for unowned bytes.
Our slice object is more-or-less a simple wrapper around a pointer and a length.
If the pointer is const, we use a `BytesView`, which is read-only.
Otherwise, we use a `Bytes` object, which allows modification of the underlying data.

```C++
std::string s {"abc"};
std::string_view sv {"123"};

// We can create slices from existing containers...
cco::Bytes b {s};
cco::BytesView bv {sv};

// ...or from "raw parts", i.e. a pointer and a length.
cco::Bytes b2 {s.data(), s.size()};
cco::BytesView bv2 {sv.data(), sv.size()};

// Conversions are allowed from Bytes to BytesView, but not the other way.
cco::BytesView b3 {b};

// We can also create owned strings.
auto sv2 = b.to_string();
auto s2 = bv.to_string();

// Slices have methods for modifying the size and pointer position. These methods do not change the
// underlying data, they just change what range of bytes the slice is currently "viewing". advance()
// increments the underlying pointer...
b.advance(1);

// ...and truncate() decreases the size.
b.truncate(1);

// Comparison operations are implemented.
assert(b == "b");
assert(bv.starts_with("12"));

// Finally, Bytes can use the non-const overload of operator[](), allowing us to modify the original
// string.
b[0] = '\xFF';
assert(s[1] == '\xFF');
```

### Opening a Database
Opening a database is a two-step process.
First, we create the database object using the default constructor.
Next, we call `open()` to open the database connection.

```C++
// Set some initialization options. We'll use pages of size 2 KB with 2 MB of cache.
cco::Options options;
options.page_size = 0x2000;
options.cache_size = 256;
options.log_level = spdlog::level::info;

// Open or create a database at "/tmp/cats".
auto s = db.open("/tmp/cats", options);

// Handle failure. s.what() provides information about what went wrong.
if (!s.is_ok()) {

}
```

### Updating a Database
Records can be added or removed using methods on the database object.
Keys are unique, so inserting a record that already exists will cause modification of the existing value.
Note that the key cannot be empty, nor can it exceed the maximum key length (see [Info](#info-objects)).

```C++
// Insert a key-value pair. We can use arbitrary bytes for both the key and value.
auto s = db.insert("\x11\x22\x33", "\xDD\xEE\xFF");

// Again, the status object reports the outcome of the operation. Since we are not inside a transaction, all modifications
// made to the database are applied atomically. This means that if this status is OK, then our key-value pair is safely on
// disk (on the WAL disk, but not the database disk yet). This has a lot of overhead, so using a transaction is desirable
// if multiple modifications need to be performed at once.
if (!s.is_ok()) {

}

// We can erase records by key, or by passing a cursor object (see Queries below). It should be noted that a cursor used to
// erase a key will be invalidated if the operation succeeds.
s = db.erase("42");

// If the key is not found (or the cursor is invalid), we'll receive a "not found" status.
assert(s.is_not_found());
```

### Querying a Database
The database is queried using cursors returned by `find*()`, `first()`, and `last()`.
These methods, as well as various cursor operations, are explained below.

```C++
// We can find the first record greater than or equal to a given key...
auto c1 = db.find("\x10\x20\x30");

// ...or, we can try for an exact match.
auto c2 = db.find_exact("/x10/x20/x30");

// Both methods return cursors, which point to database records and can be used to perform range queries. We check if a
// cursor is valid (i.e. it points to an existing record and has an OK internal status) by writing:
if (c1.is_valid()) {

}

// As mentioned above, cursors store and provide access to a status object. We check this status using the status() 
// method. Once a cursor's status becomes non-OK, it will stay that way and the cursor can no longer be used.
[[maybe_unused]] auto s = c1.status();

// Calico DB provides methods for accessing the first and last records. Like the find*() methods, these methods return
// cursors. This lets us easily traverse all records in order.
for (auto c = db.first(); c.is_valid(); ++c) {}

// We can also traverse in reverse order...
for (auto c = db.last(); c.is_valid(); c--) {}

// ...or from the start to some arbitrary point. In this example, the cursor we are iterating to is not valid. This is
// the same as iterating until we hit the end.
for (auto c = db.first(), bounds = db.find("42"); c.is_valid() && c != bounds; c++) {}

// We can also use the key ordering.
for (auto c = db.first(); c.is_valid() && c.key() < cco::stob("42"); c++) {}
```

### Transactions
In Calico DB, transactions are represented by first-class `Transaction` objects.
Any modifying operations that take place while a `Transaction` object T is live will take place within the transaction that T represents.
Otherwise, database modifications behave as if they were atomic, incurring quite a bit of overhead from the additional commit operations.
For this reason, if one wants to modify more than a few records at a time, it is best to do it in a transaction.

```C++
// Start a transaction. All modifications made to the database while this object is live will be part of the transaction
// it represents.
auto xact = db.transaction();

auto s = db.erase(db.first());
assert_ok(s);

// If this status is OK, every change made in the transaction will be undone. 
s = xact.abort();
assert_ok(s);

// If we want to start another transaction, we need to make another call to the database.
xact = db.transaction();

s = db.erase(db.first());
assert_ok(s);

// This time we'll commit the transaction. Note that if the transaction object goes out of scope before either abort()
// or commit() is called, an abort() will be attempted automatically.
s = xact.commit();
assert_ok(s);
```

### Info Objects
We use an info object to get information about the database state.

```C++
// We can use an info object to get information about the database state.
const auto info = db.info();
[[maybe_unused]] const auto rc = info.record_count();
[[maybe_unused]] const auto pc = info.page_count();
[[maybe_unused]] const auto mk = info.maximum_key_size();

// The page size is fixed at database creation time. If the database already existed, the page size passed to the 
// constructor through cco::Options is ignored. We can query the real page size using the following line.
[[maybe_unused]] const auto ps = info.page_size();
```

### Closing a Database
Calico DB database objects will automatically close the database connection, if still active, once they go out of scope.
We can also call `close()` explicitly, to deal with any errors it might produce (otherwise they just get logged and dealt with on the next startup).
Note that regardless of the outcome of `close()`, the database will be closed and should not be used anymore.
This is unless there is an active transaction, in which case `close()` will fail until the transaction is completed.

```C++
s = db.close();
assert_ok(s);
```

### Deleting a Database
An **open** database can be deleted by passing ownership to the following static method.

```C++
s = cco::Database::destroy(std::move(db));
assert_ok(s);
```
