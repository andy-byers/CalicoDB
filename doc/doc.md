# CalicoDB Documentation

+ [Build](#build)
+ [API](#api)
    + [Slices](#slices)
    + [Opening a database](#opening-a-database)
    + [Readonly transactions](#readonly-transactions)
    + [Read-write transactions](#read-write-transactions)
    + [Tables](#tables)
    + [Cursors](#cursors)
    + [Database properties](#database-properties)
    + [Closing a database](#closing-a-database)
    + [Destroying a database](#destroying-a-database)
+ [Acknowledgements](#acknowledgements)

## Build
CalicoDB is built using CMake.
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
To build the library in release file_lock without tests, the last command would look like:
```bash
cmake -DCMAKE_BUILD_TYPE=Release -DCALICODB_Test=Off .. && cmake --build .
```

## API

### Slices
```C++
std::string str("abc");

// We can create slices from C-style strings, standard library strings, or directly from a pointer and a length.
calicodb::Slice s1(str.c_str());
calicodb::Slice s2(str);
calicodb::Slice s3(str.data(), str.size());

// A slice can be converted back to a std::string using Slice::to_string().
std::cout << s1.to_string() << '\n';

// Slices have methods for modifying the size and pointer position. These methods do not change the underlying data, 
// they just change what range of bytes the slice is currently viewing. Slice::advance() increments the underlying 
// pointer...
s1.advance(1);

// ...and truncate() decreases the size.
s1.truncate(1);

// Comparison operations are implemented.
assert(s1 == "b");
assert(s2 < "bc");
assert(s2.starts_with("ab"));
```

### Opening a database
```C++
// Set some initialization options.
const calicodb::Options options = {
    // Size of the page cache in bytes.
    .cache_size = 0x200000,
    
    // Store the WAL file in a nonstandard location. The directory "location" must already exist.
    .wal_filename = "/location/calicodb_wal",
    
    // Write info log messages to this calicodb::Sink *.
    .info_log = nullptr,
    
    // This can be used to inject a custom Env implementation. (see the tools::FakeEnv class in
    // tools/env_helpers.h for an example that stores its files in memory).
    .env = nullptr,
};

// Create or open a database at "/tmp/cats".
calicodb::DB *db;
calicodb::Status s = calicodb::DB::open(options, "/tmp/cats", db);

// Handle failure. s.to_string() provides a string representation of the status.
if (!s.is_ok()) {

}
```

### Readonly transactions
Readonly transactions are typically run through the `DB::view()` API.
```C++
calicodb::Status s = db->view([](calicodb::Txn &txn) {
    // Open tables (see #tables) and read some data. The `Txn` object is managed by the 
    // database. DB::view() will forward the status returned by this callable.
    return Status::ok();
});
```

Readonly transactions can also be run manually, however, the caller is responsible for `delete`ing the `Txn` handle when it is no longer needed.
```C++
calicodb::Txn *txn;

// Start a readonly transaction.
s = db->new_txn(false, txn);
if (!s.is_ok()) {
}

// Read some data (see #tables).

// Finish the transaction.
delete txn;
```

### Read-write transactions
Read-write transactions can be run using `DB::update()`.
`DB::update()` accepts a callable that runs a read-write transaction.
If an error is encountered during a read-write transaction, the transaction status (queried with `Txn::status()`) may be set.
If this happens, the transaction object, and any tables created from it, will return immediately with this same error whenever a read/write method is called.
The only possible course-of-action in this case is to `delete` the transaction handle and possibly try again.

```C++
s = db->update([](auto &txn) {
    // Read and/or write some records. If this callable returns an OK status,
    // `Txn::commit()` is called on `txn` and the resulting status returned.
    // Otherwise, the transaction is rolled back and the original non-OK 
    // status is forwarded to the caller. Note that Txn::commit() does not 
    // invalidate the transaction handle. This allows one to perform multiple 
    // batches of writes per DB::update().
    return Status::ok();
});
```

Like readonly transactions, read-write transactions can be run manually.
```C++
calicodb::Txn *txn;

// Start a read-write transaction.
calicodb::Status s = db->new_txn(true, txn);
if (!s.is_ok()) {
}

// Write some data...

// Commit the transaction.
s = txn->commit();
if (!s.is_ok()) {
    // If commit() failed, then there was likely a low-level I/O error. There
    // are no changes made to the database in this case.
}

// Clean up the transaction handle. If Txn::commit() was not called, all changes
// made during this transaction will be dropped.
delete txn;
```

### Tables
Tables are managed by live `Txn` objects.
This means that all table handles opened by a given `Txn` must be closed by the time the `Txn` object is destroyed.
When using the `DB::view()`/`DB::update()` API, one just needs to make sure all `Table` handles are `delete`d by the time the callable returns.

```C++
calicodb::Table *table;

// Set some initialization options. Enforces that the table "cats" must
// not exist. Note that readonly transactions cannot create new tables.
calicodb::TableOptions tbopt;
tbopt.error_if_exists = true;
tbopt.create_if_missing = true;

// Create the table. Note that this table will not persist in the database 
// unless `Txn::commit()` is called prior to the transaction ending.
calicodb::Status s = txn->new_table(tbopt, "cats", table);
if (s.is_ok()) {
    // `table` holds the address of the handle for the open table "cats". 
    // "cats" will be open until the handle is delete'd.
}

std::string value;
s = table->get("lilly", &value);
if (s.is_ok()) {
    // `value` holds the value associated with the key "lilly".
} else if (s.is_not_found()) {
    // The key "lilly" does not exist in "cats".
} else {
    // An I/O error occurred.
}

s = table->put("lilly", "calico");
if (s.is_ok()) {
    // The value for key "lilly" in table "cats" has been set to "calico".
} else {
    // An I/O error occurred.
}

s = table->erase("lilly");
if (s.is_ok()) {
    // Table "cats" is guaranteed to not have a record with key "lilly".
} else {
    // An I/O error occurred. It is not an error if the key does not exist.
}

// Close the table.
delete table;

// Remove the table named "cats" from the database. "cats" must be closed.
s = txn->drop_table("cats");
if (s.is_ok()) {
    
}
```

### Cursors

```C++
calicodb::Cursor *cursor = table->new_cursor();

cursor->seek_first();
while (cursor->is_valid()) {
    const calicodb::Slice key = cursor->key();
    const calicodb::Slice val = cursor->value();
    cursor->next();
}

cursor->seek_last();
while (cursor->is_valid()) {
    // Use the cursor.
    cursor->previous();
}

cursor->seek("freya");
while (cursor->is_valid() && cursor->key() <= "lilly") {
    // Key is in [freya,lilly].
    cursor->next();
}

delete cursor;
```

### Database properties

```C++
// Database properties are made available as strings.
std::string prop;
bool exists = db->get_property("calicodb.stats", &prop);

// Passing nullptr for the property value causes get_property() to perform a simple existence check, 
// without attempting to populate the property value string.
exists = db->get_property("calicodb.stats", nullptr);
```

### Closing a database
To close the database, just `delete` the handle.
The last connection to a particular database to close will unlink the WAL.
If a WAL is left behind after closing, then something has gone wrong.
CalicoDB will attempt recovery on the next startup.

```C++
delete db;
```

### Destroying a database

```C++
calicodb::Status s = calicodb::DB::destroy(options, "/tmp/cats");
if (s.is_ok()) {
    // Database has been destroyed.
}
```

## Acknowledgements
1. https://cstack.github.io/db_tutorial/
    + Tutorial on database development in C
2. https://www.sqlite.org/arch.html
    + Much of this project was inspired by SQLite3, both the architecture design documents and the source code
    + Especially see the B-tree design document, as well as `btree.h`, `btree.c`, and `btreeInt.h`
3. https://github.com/google/leveldb
    + Much of the API is inspired by LevelDB
    + Some parts of the CMake build process are taken from their `CMakeLists.txt`
4. https://github.com/facebook/rocksdb/wiki/Write-Ahead-Log
    + Explanation of RocksDB's WAL
    + The idea to have multiple different record types and to use a "tail" buffer are from this document
5. https://stablecog.com/
    + Used to generate the original calico cat image, which was then further modified to produce [mascot.png](mascot.png)