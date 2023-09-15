# CalicoDB Documentation

+ [Build](#build)
+ [API](#api)
    + [Slices](#slices)
    + [Statuses](#statuses)
    + [Global options](#Global options)
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
#include "calicodb/slice.h"

static constexpr auto *kCString = "abc";

// Slices can be created from C-style strings, or from a pointer and a size.
calicodb::Slice s1(kCString);
calicodb::Slice s2(kCString, 3);

// Slices have methods for modifying the size and pointer position. These methods do not change the underlying data, 
// they just change what range of bytes the slice is currently viewing. advance() increments the underlying pointer...
s1.advance(1);

// ...and truncate() decreases the size.
s1.truncate(1);

// Comparison operations are implemented.
assert(s1 == "b");
assert(s2 < "bc");
assert(s2.starts_with("ab"));
```

### Global options
Per-process options are queried and set using `calicodb::configure()`.
This API is not thread safe, and options should be set before any other library functions are called or objects created.
For example, `calicodb::configure()` can be used to set the general-purpose allocator used to get heap memory for the library.
Changing the allocator while the library is using heap memory can cause a malloc/free mismatch, which is undefined behavior.

```C++
#include "calicodb/config.h"
#include "calicodb/status.h"

// Query the general-purpose allocator that is currently in-use.
calicodb::AllocatorConfig config;
const calicodb::Status rc = calicodb::configure(calicodb::kGetAllocator, &config);
if (rc.is_ok()) {
    // config contains the default allocation routines.
    assert(config.malloc == CALICODB_DEFAULT_MALLOC);
    assert(config.realloc == CALICODB_DEFAULT_REALLOC);
    assert(config.free == CALICODB_DEFAULT_FREE);
}
```

### Statuses
All CalicoDB routines that have the possibility of failure will return (or otherwise expose) a status object.
Status objects have a code, a subcode, and possibly a message.
If a `Status` has a message, then it is stored on the heap, along with the code and subcode.
Otherwise, or if the heap allocation fails, the code and subcode are packed into the state pointer.

```C++
#include "calicodb/status.h"

// The default constructor creates an OK status. An OK status is a status for which 
// Status::is_ok() returns true (not an error status).
calicodb::Status s;

// The following static method also creates an OK status.
s = calicodb::Status::ok();

// The code and subcode can be retrieved as follows:
std::printf("code = %d, subcode = %d\n", int(s.code()), int(s.subcode()));

// A human-readable C-style string representing the status can be accessed with:
std::printf("s = %s\n", s.message());

// Non-OK statuses must be created through one of the static methods.
s = calicodb::Status::io_error();
s = calicodb::Status::invalid_argument();
s = calicodb::Status::busy(calicodb::Status::kRetry); // Equivalent to Status::retry()

// One can check the type of status with one of the following methods:
if (s.is_ok()) {
    
} else if (s.is_io_error()) {
    
} else if (s.is_retry()) { 
    // Equivalent to s.is_busy() && s.subcode() == Status::kRetry
}
```

### Opening a database

```C++
#include "calicodb/db.h"

// Set some initialization options. See include/calicodb/options.h for descriptions
// and default values.
const calicodb::Options options = {
    4'096, // page_size
    1'024 * 4'096,  // cache_size
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

// Create or open a database at the given path.
calicodb::DB *db;
s = calicodb::DB::open(options, "/tmp/calicodb_cats_example", db);

// Handle failure. s.message() provides a string representation of the status.
if (!s.is_ok()) {

}
```

### Readonly transactions
Readonly transactions are typically run using `DB::run(ReadOptions(), fn)`, where `fn` is a callback that reads from the database.

```C++
#include "calicodb/db.h"
#include "calicodb/tx.h"

const char *bucket_name = "all_kittens"; // Lambda captures are supported.
s = db->run(calicodb::ReadOptions(), [bucket_name](const calicodb::Tx &tx) {
    // Open buckets (see #buckets) and read some data. The `tx` object is managed 
    // by the database. DB::view() will forward the status returned by this callable.
    return calicodb::Status::ok();
});
```

### Read-write transactions
Read-write transactions can be run using `DB::run(WriteOptions(), fn)`, where `fn` is a callable that modifies the database.
If an error is encountered during a read-write transaction, the transaction status (queried with `Tx::status()`) may be set.
If this happens, the transaction object, and any buckets created from it, will return immediately with this same error whenever a read/write method is called.
The only possible course-of-action in this case is to return from `fn` and let the database clean up.

```C++
#include "calicodb/db.h"
#include "calicodb/tx.h"

s = db->run(calicodb::WriteOptions(), [](calicodb::Tx &tx) {
    // Read and/or write some records. If this callable returns an OK status,
    // `tx::commit()` is called on `tx` and the resulting status returned.
    // Otherwise, the transaction is rolled back and the original non-OK 
    // status is forwarded to the caller (rollback itself cannot fail). Note 
    // that calling tx::commit() early does not invalidate the transaction 
    // handle. This allows one to perform multiple batches of writes per 
    // DB::run().
    return calicodb::Status::ok();
});
```

### Manual transactions
Transactions can also be run manually.
The caller is responsible for `delete`ing the `Tx` handle when it is no longer needed.

```C++
#include "calicodb/db.h"
#include "calicodb/tx.h"

calicodb::Tx *reader;

// Start a readonly transaction.
s = db->new_tx(calicodb::ReadOptions(), reader);
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
s = db->new_tx(calicodb::WriteOptions(), writer);
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
Each open bucket is represented by a `calicodb::Cursor` over its contents.
Multiple cursors can be opened on each bucket.

```C++
#include "calicodb/cursor.h"
#include "calicodb/db.h"
#include "calicodb/tx.h"

calicodb::Cursor *c;

// Set some initialization options. Enforces that the bucket "cats" must
// not exist. Note that readonly transactions cannot create new buckets by
// virtue of the fact that they must be used through pointers to const, 
// and Tx::create_bucket() is not a const method. Tx::open_bucket() can be
// used by a readonly transaction to open an existing bucket.
calicodb::BucketOptions b_opt;
b_opt.error_if_exists = true;

// Create the bucket. Note that this bucket will not persist in the database 
// unless Tx::commit() is called prior to the transaction ending.
s = tx->create_bucket(b_opt, "cats", &c);
if (s.is_ok()) {
    // c holds a cursor over the bucket "cats". The bucket will remain open
    // until c is delete'd, which must happen before the transaction is
    // finished. 
}

// Release memory occupied by the cursor.
delete c;

// Since the bucket "cats" already exists, we can open it via the following:
s = tx->open_bucket("cats", c);
if (s.is_ok()) {
    // b holds the handle for the open bucket "cats". The bucket will remain 
    // open until either tx is delete'd, or "cats" is dropped with 
    // Tx::drop_bucket(). 
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
#include "calicodb/cursor.h"
#include "calicodb/db.h"
#include "calicodb/tx.h"

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

// Find an exact record.
c->find("lilly");
if (c->is_valid()) {
    // c->value() contains the value for "lilly".
    assert(c->key() == "lilly");
} else if (c->status().is_ok()) {
    // "lilly" was not found.
} else {
    // An I/O error occurred.
    assert(!c->status().is_ok());
}

// Insert a new record. c is used to determine what bucket to put the record in.
// If c is already positioned near where the new record should go, a root-to-leaf 
// traversal may be avoided.
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
    assert(c->is_valid());
    assert(c->key() == "junie");
    assert(c->value() == "brown tabby");
}

// Erase the record pointed to by the cursor.
s = tx->erase(*c);
if (s.is_ok()) {
    // c is on the record immediately following the erased record, if such a 
    // record exists. Otherwise, c is invalidated.
}

// For convenience, an overload of erase() is provided that takes a key directly. 
// It is not an error if the key does not exist in this case (Status::ok() is 
// returned unless there was an actual system-level error).
s = tx->erase(*c, "some key");
if (s.is_ok()) {
    // "some key" is guaranteed to be absent from the bucket that c represents.
}

delete c;
```

### Database properties

```C++
#include "calicodb/db.h"
#include "calicodb/string.h"

calicodb::String prop;
s = db->get_property("calicodb.stats", &prop);
if (s.is_ok()) {
    std::puts(prop.c_str());
} else if (s.is_no_memory()) {
    // Not enough memory to allocate the property string.
} else if (s.is_not_found()) {
    // Property does not exist.
}

// Passing nullptr for the property value causes get_property() to perform a simple existence check, 
// without attempting to populate the property value string.
s = db->get_property("calicodb.stats", nullptr);
if (s.is_ok()) {
    
}
```

### Checkpoints
Pages that are modified during transactions are written to the WAL, not the database file.
At some point, it is desirable to write the pages accumulated in the WAL back to the database.
This operation is called a checkpoint.
Note that automatic checkpoints can be run using the `auto_checkpoint` option (see [Opening a database](#opening-a-database)).
Automatic checkpoints are attempted when transactions are started.

```C++
#include "calicodb/db.h"

// This transaction was started earlier, in #manual-transactions. It must be
// finished before the database can be checkpointed. Note that the bucket 
// handle from earlier must not be used after this next line.
delete tx;

// Now we can run a checkpoint. See DB::run() for an API that takes away some
// of the pain associated with transaction lifetimes.

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
#include "calicodb/db.h"

s = calicodb::DB::destroy(options, "/tmp/calicodb_cats_example");
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
