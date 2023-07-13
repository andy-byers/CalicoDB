# CalicoDB Documentation

+ [Build](#build)
+ [API](#api)
    + [Slices](#slices)
    + [Statuses](#statuses)
    + [Opening a database](#opening-a-database)
    + [Readonly transactions](#readonly-transactions)
    + [Read-write transactions](#read-write-transactions)
    + [Manual transactions](#manual-transactions)
    + [Buckets](#buckets)
    + [Cursors](#cursors)
    + [Database properties](#database-properties)
    + [Checkpoints](#checkpoints)
    + [Closing a database](#closing-a-database)
    + [Destroying a database](#destroying-a-database)
+ [Resources](#resources)

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
std::printf("s1 = %s\n", s1.to_string().c_str());

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
std::printf("code = %d, subcode = %d\n", int(s.code()), int(s.subcode()));

// A human-readable string representing the status can be created with:
std::printf("s = %s\n", s.to_string().c_str());

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
// Set some initialization options. See include/calicodb/options.h for descriptions
// and default values.
const calicodb::Options options = {
    1'024 * calicodb::kPageSize,  // cache_size
    1'000, // auto_checkpoint
    "wal-filename", // wal_filename
    nullptr, // info_log
    nullptr, // env
    nullptr, // busy
    true, // create_if_missing
    false, // error_if_exists
    false, // temp_database
    calicodb::Options::kSyncNormal, // sync_mode
    calicodb::Options::kLockNormal, // lock_mode
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
s = db->view([](const calicodb::Tx &tx) {
    // Open buckets (see #buckets) and read some data. The `tx` object is managed by the 
    // database. DB::view() will forward the status returned by this callable.
    return calicodb::Status::ok();
});
```

### Read-write transactions
Read-write transactions can be run using `DB::update()`.
`DB::update()` accepts a callable that runs a read-write transaction.
If an error is encountered during a read-write transaction, the transaction status (queried with `Tx::status()`) may be set.
If this happens, the transaction object, and any buckets created from it, will return immediately with this same error whenever a read/write method is called.
The only possible course-of-action in this case is to `delete` the transaction handle and possibly try again.

```C++
s = db->update([](calicodb::Tx &tx) {
    // Read and/or write some records. If this callable returns an OK status,
    // `tx::commit()` is called on `tx` and the resulting status returned.
    // Otherwise, the transaction is rolled back and the original non-OK 
    // status is forwarded to the caller. Note that tx::commit() does not 
    // invalidate the transaction handle. This allows one to perform multiple 
    // batches of writes per DB::update().
    return calicodb::Status::ok();
});
```

### Manual transactions
Transactions can also be run manually.
The caller is responsible for `delete`ing the `Tx` handle when it is no longer needed.

```C++
const calicodb::Tx *reader;

// Start a readonly transaction. This overload of DB::new_tx() only accepts 
// a const Tx *. This means that the resulting transaction object cannot be 
// used to change the database contents, since only const methods are
// available on it.
s = db->new_tx(reader);
if (!s.is_ok()) {
}

// Read some data (see #buckets).

// Finish the transaction. Only 1 transaction can be live on a given DB at
// any given time (concurrent access requires each thread/process to have 
// its own DB handle).
delete reader;

calicodb::Tx *writer;

// Start a read-write transaction. The handle resulting from this call can be
// used to modify the database contents.
s = db->new_tx(calicodb::WriteTag(), writer);
if (!s.is_ok()) {
}

// Write some data...

// Commit the transaction.
s = writer->commit();
if (!s.is_ok()) {
    // If commit() failed, then there was likely a low-level I/O error. There
    // are no changes made to the database in this case.
}

// Rename to tx for other examples to use (these examples are compiled).
auto *tx = writer;
```

### Buckets
In CalicoDB, buckets are persistent mappings from string keys to string values.
Each open bucket is represented by an opaque `calicodb::Bucket` handle.
The actual bucket state is managed by the `calicodb::Tx` that opened the bucket.
Buckets are implicitly closed when the transaction finishes.

```C++
calicodb::Bucket b;

// Set some initialization options. Enforces that the bucket "cats" must
// not exist. Note that readonly transactions cannot create new buckets by
// virtue of the fact that they must be used through pointers to const, 
// and Tx::create_bucket() is not a const method. Tx::open_bucket() can be
// used by a readonly transaction to open an existing bucket.
calicodb::BucketOptions b_opt;
b_opt.error_if_exists = true;

// Create the bucket. Note that this bucket will not persist in the database 
// unless Tx::commit() is called prior to the transaction ending.
s = tx->create_bucket(b_opt, "cats", &b);
if (s.is_ok()) {
    // b holds the handle for the open bucket "cats". The bucket will remain 
    // open until either tx is delete'd, or "cats" is dropped with 
    // Tx::drop_bucket(). 
}

std::string value;
s = tx->get(b, "lilly", &value);
if (s.is_ok()) {
    // `value` holds the value associated with the key "lilly".
} else if (s.is_not_found()) {
    // The key "lilly" does not exist in "cats".
} else {
    // An I/O error occurred.
}

s = tx->put(b, "lilly", "calico");
if (s.is_ok()) {
    // The value for key "lilly" in bucket "cats" has been set to "calico".
} else {
    // An I/O error occurred.
}

s = tx->erase(b, "lilly");
if (s.is_ok()) {
    // Bucket "cats" is guaranteed to not have a record with key "lilly".
} else {
    // An I/O error occurred. It is not an error if the key does not exist.
}

// Remove the bucket named "fish" from the database.
s = tx->drop_bucket("fish");
if (s.is_ok()) {
    
}
```

### Cursors
Cursors are used to perform full-bucket scans and range queries.
They can also be used to help modify the database during [read-write transactions](#read-write-transactions).

```C++
calicodb::Cursor *c = tx->new_cursor(b);

// Scan the entire bucket forwards.
c->seek_first();
while (c->is_valid()) {
    const calicodb::Slice key = c->key();
    const calicodb::Slice val = c->value();
    c->next();
}

// Scan the entire bucket backwards.
c->seek_last();
while (c->is_valid()) {
    // Use the cursor.
    c->previous();
}

// Scan a range of keys.
c->seek("freya");
while (c->is_valid() && c->key() < "lilly") {
    // Key is in [freya,lilly).
    c->next();
}

// Insert a new record using a cursor.
s = tx->put(*c, "junie", "tabby");
if (s.is_ok()) {
    // c is placed on the newly-inserted record.
    assert(c->is_valid());
    assert(c->key() == "junie");
    assert(c->value() == "tabby");
}

// Modify a record using a cursor.
s = tx->put(*c, c->key(), "brown tabby");
if (s.is_ok()) {
}

// Erase the record pointed to by the cursor.
s = tx->erase(*c);
if (s.is_ok()) {
    // c is on the record immediately following the erased record, if
    // such a record exists.
}

delete c;
```

### Database properties

```C++
// Database properties are made available as strings.
std::string prop;
if (db->get_property("calicodb.stats", &prop)) {
    std::puts(prop.c_str());
}

// Passing nullptr for the property value causes get_property() to perform a simple existence check, 
// without attempting to populate the property value string.
if (db->get_property("calicodb.stats", nullptr)) {
    
}
```

### Checkpoints
Pages that are modified during transactions are written to the WAL, not the database file.
At some point, it is desirable to write the pages accumulated in the WAL back to the database.
This operation is called a checkpoint.
Note that automatic checkpoints can be run using the `auto_checkpoint` option (see [Opening a database](#opening-a-database)).
Automatic checkpoints are attempted when transactions are started.

```C++
// This transaction was started earlier, in #manual-transactions. It must be
// finished before the database can be checkpointed. Note that the bucket 
// handle from earlier must not be used after this next line.
delete tx;

// Now we can run a checkpoint. See DB::update()/DB::view() for an API that
// takes away some of the pain associated with transaction lifetimes.

// If the `reset` parameter to DB::checkpoint() is true, the DB will set things
// up such that the next writer writes to the start of the WAL file again. This
// involves blocking until other connections are finished with the WAL.
s = db->checkpoint(true);
if (s.is_ok()) {
    // The whole WAL was written back to the database file.
} else if (s.is_busy()) {
    // Some other connection got in the way.
} else {
    // Some other error occurred.
}
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
s = calicodb::DB::destroy(options, "/tmp/cats");
if (s.is_ok()) {
    // Database has been destroyed.
}
```

## Resources
1. [Let's Build a Simple Database](https://cstack.github.io/db_tutorial/)
    + Tutorial on database development in C
2. [Architecture of SQLite](https://www.sqlite.org/arch.html)
    + Much of this project was inspired by SQLite3, both the architecture design documents and the source code
    + Especially see the B-tree design document, as well as `btree.h`, `btree.c`, and `btreeInt.h`
3. [LevelDB](https://github.com/google/leveldb)
    + Much of the API is inspired by LevelDB
    + Some parts of the CMake build process are taken from their `CMakeLists.txt`
4. [BoltDB](https://github.com/boltdb/bolt)
    + Inspiration for the transaction API
