# CalicoDB Documentation

+ [Build](#build)
+ [API](#api)
    + [Slices](#slices)
    + [Statuses](#statuses)
    + [Opening a database](#opening-a-database)
    + [Readonly transactions](#readonly-transactions)
    + [Read-write transactions](#read-write-transactions)
    + [Manual transactions](#manual-transactions)
    + [Tables](#tables)
    + [Cursors](#cursors)
    + [Database properties](#database-properties)
    + [Closing a database](#closing-a-database)
    + [Checkpoints](#checkpoints)
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

### Statuses
```C++
// All CalicoDB routines that have the possibility of failure will return (or otherwise expose) a status object.
// Status objects have a code, a subcode, and possibly a message. The default constructor creates an OK status,
// that is, a status for which Status::is_ok() returns true (not an error status).
calicodb::Status s;

// The following static method also creates an OK status.
s = calicodb::Status::ok();

// The code and subcode can be retrieved as follows:
std::cerr << "code: " << int(s.code()) << ", subcode: " << int(s.subcode()) << '\n';

// A human-readable string representing the status can be created with:
std::cerr << s.to_string() << '\n';

// Non-OK statuses must be created through one of the static methods. Note that a status can have either a
// message, or a subcode, but not both.
s = calicodb::Status::io_error("uh oh");
s = calicodb::Status::invalid_argument();
s = calicodb::Status::busy(calicodb::Status::kRetry); // Equivalent to Status::retry()

// One can check the type of status with one of the following methods:
if (s.is_ok()) {
    
} else if (s.is_io_error()) {
    
} else if (s.is_retry()) { // Equivalent to s.is_busy() && s.subcode() == Status::kRetry
    
}
```

### Opening a database
```C++
// Set some initialization options.
const calicodb::Options options = {
    // Size of the page cache in bytes.
    1'024 * calicodb::kPageSize, 
                                                    
    // Filename to use for the WAL. If empty, creates the WAL at
    // "dbname-wal", where "dbname" is the name of the database.
    "wal-filename",

    // Destination for info log messages.
    nullptr,

    // Custom storage environment. See env.h for details.
    nullptr,

    // Action to take while waiting on a file lock.
    nullptr,

    // If true, create the database if it is missing.
    true,

    // If true, return with an error if the database already exists.
    false,

    // If true, sync the WAL file on every commit. Hurts performance quite a bit,
    // but provides extra durability.
    false,
};

// Create or open a database at "/tmp/cats".
calicodb::DB *db;
s = calicodb::DB::open(options, "/tmp/cats", db);

// Handle failure. s.to_string() provides a string representation of the status.
if (!s.is_ok()) {

}
```

### Readonly transactions
Readonly transactions are typically run through the `DB::view()` API.
```C++
s = db->view([](calicodb::Txn &txn) {
    // Open tables (see #tables) and read some data. The `Txn` object is managed by the 
    // database. DB::view() will forward the status returned by this callable.
    return calicodb::Status::ok();
});
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
    return calicodb::Status::ok();
});
```

### Manual transactions
Transactions can also be run manually.
The caller is responsible for `delete`ing the `Txn` handle when it is no longer needed.

```C++
calicodb::Txn *txn;

// Start a readonly transaction.
s = db->new_txn(false, txn);
if (!s.is_ok()) {
}

// Read some data (see #tables).

// Finish the transaction.
delete txn;

// Start a read-write transaction.
s = db->new_txn(true, txn);
if (!s.is_ok()) {
}

// Write some data...

// Commit the transaction.
s = txn->commit();
if (!s.is_ok()) {
    // If commit() failed, then there was likely a low-level I/O error. There
    // are no changes made to the database in this case.
}

// Leave txn for other examples to use.
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
s = txn->new_table(tbopt, "cats", table);
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

// As with any other object created using a new_*() method, tables are closed
// with operator delete(). We'll leave this table open so it can be used in
// other examples.

// Remove the table named "CATS" from the database. "CATS" must be closed.
s = txn->drop_table("CATS");
if (s.is_ok()) {
    
}
```

### Cursors

```C++
calicodb::Cursor *c = table->new_cursor();

c->seek_first();
while (c->is_valid()) {
    const calicodb::Slice key = c->key();
    const calicodb::Slice val = c->value();
    c->next();
}

c->seek_last();
while (c->is_valid()) {
    // Use the cursor.
    c->previous();
}

c->seek("freya");
while (c->is_valid() && c->key() <= "lilly") {
    // Key is in [freya,lilly].
    c->next();
}

delete c;
```

### Database properties

```C++
// Database properties are made available as strings.
std::string prop;
bool exists = db->get_property("calicodb.stats", &prop);
if (exists) {
    std::cout << "calicodb.stats: " << prop << '\n';
}

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
// We left this table open in #tables. It must be closed before the Txn that
// opened it is closed.
delete table;

// This transaction was started earlier, in #manual-transactions. It must be
// finished before the database is closed.
delete txn;

// Now we can close the database. See DB::update()/DB::view() for an API that
// takes away some of the pain associated with transaction lifetimes, leaving
// table management up to the user.
delete db;
```

### Checkpoints
As described in [a](design.md#shm-file)
```C++

```

### Destroying a database

```C++
s = calicodb::DB::destroy(options, "/tmp/cats");
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
4. https://stablecog.com/
    + Used to generate the original calico cat image, which was then further modified to produce [mascot.png](mascot.png)