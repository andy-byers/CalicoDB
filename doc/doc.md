# CalicoDB Documentation
CalicoDB is a persistent, transactional key-value store that aims to be useful in memory-constrained environments.
The library allows keys and values (arbitrary byte arrays) to be stored in multiple (possibly nested) buckets.
CalicoDB uses ideas from multiple popular embedded databases, such as SQLite, LevelDB, and BoltDB.

+ [Build](#build)
+ [API](#api)
    + [Statuses](#statuses)
    + [Opening a database](#opening-a-database)
    + [Readonly transactions](#readonly-transactions)
    + [Read-write transactions](#read-write-transactions)
    + [Manual transactions](#manual-transactions)
    + [Concurrency](#concurrency)
    + [Buckets](#buckets)
    + [Slices](#slices)
    + [Cursors](#cursors)
    + [Database properties](#database-properties)
    + [Checkpoints](#checkpoints)
    + [Closing a database](#closing-a-database)
    + [Destroying a database](#destroying-a-database)
    + [Global options](#global-options)
+ [Resources](#resources)

## Build
CalicoDB is built using CMake.
In the project root directory, run
```bash
mkdir -p build && cd ./build
```

followed by
```bash
cmake .. && cmake --build .
```

to build the library and tests.
To build the library without tests, the last command would look like:
```bash
cmake -DCALICODB_BuildTests=Off .. && cmake --build .
```

Additional options can be found in the toplevel CMakeLists.txt.

## API

### Statuses
All CalicoDB routines that have the possibility of failure will return (or otherwise expose) a status object that indicates what went wrong.
Status objects hold 3 pieces of information: a code, a subcode, and an optional message.
If a `Status` has a message, then it is stored on the heap, along with the code and subcode.
Otherwise, or if the heap allocation fails, the code and subcode are packed into the state pointer.

```C++
#include "calicodb/status.h"

// Create an "OK" status.
calicodb::Status s;
assert(s.is_ok());
assert(s == calicodb::Status::ok());

// Create an error status.
s = calicodb::Status::io_error();
s = calicodb::Status::invalid_argument("error message");
s = calicodb::Status::busy(calicodb::Status::kRetry); // Equivalent to Status::retry()

// Check the status type.
if (s.is_ok()) {
    // ...
} else if (s.is_invalid_argument()) {
    // ...
}

// A human-readable C-style string representing the status can be accessed with:
std::cerr << s.message();
```

### Opening a database
A CalicoDB database lives in a single file on disk.
The name of the database is the same as that of the file.

```C++
#include "calicodb/db.h"

calicodb::DB *db;
calicodb::Options options;
options.create_if_missing = true;
s = calicodb::DB::open(options, "/tmp/calicodb_example", db);
assert(s.is_ok());
```

### Readonly transactions
Readonly transactions are typically run using `DB::view(fn)`, where `fn` is a callback that reads from the database.

```C++
#include "calicodb/tx.h"

const char *bucket_name = "all_kittens"; // Lambda captures are supported.
s = db->view([bucket_name](const calicodb::Tx &tx) {
    // Open buckets (see #buckets) and read some data. The `tx` object is managed 
    // by the database. DB::view() will forward the status returned by this callable.
    return calicodb::Status::ok();
});
```

### Read-write transactions
Read-write transactions can be run using `DB::update(fn)`, where `fn` is a callable that modifies the database.
If an error is encountered during a read-write transaction, the transaction status (queried with `Tx::status()`) may be set.
If this happens, the transaction object, and any buckets created from it, will return immediately with this same error whenever a read/write method is called.
The only possible course-of-action in this case is to return from `fn` and let the database clean up.

```C++
#include "calicodb/tx.h"

s = db->update([](calicodb::Tx &tx) {
    // Read and/or write some records. If this callable returns an OK status,
    // Tx::commit() is called on `tx` and the resulting status returned.
    // Otherwise, the transaction is rolled back and the original non-OK 
    // status is forwarded to the caller (rollback itself cannot fail). Note 
    // that calling Tx::commit() early does not invalidate the transaction 
    // handle.
    return calicodb::Status::ok();
});
```

### Manual transactions
Transactions can also be run manually.
The caller is responsible for `delete`ing the `Tx` handle when it is no longer needed.

```C++
calicodb::Tx *reader;

// Start a readonly transaction.
s = db->new_reader(reader);
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
s = db->new_writer(writer);
if (!s.is_ok()) {
}

// Write some data...

// Commit the transaction. Note that file locks are not released until the Tx
// object is deleted.
s = writer->commit();
if (!s.is_ok()) {
    // If commit() failed, then there was likely a low-level I/O error. There
    // are no changes made to the database in this case.
}

// Rename to tx for other examples to use (these examples are compiled).
auto *tx = writer;
```

### Concurrency
The concurrency code in CalicoDB is based off of SQLite's WAL module.
Both multithread and multiprocess concurrency are supported, with some caveats.
First, `DB` handles are not safe to use from multiple threads simultaneously: each thread in a given process must have its own database connection.
Second, only a single writer is allowed to access the database at any given time, but readers can run at the same time as the writer.
Also, a `calicodb::kCheckpointPassive` checkpoint can run at the same time as a reader or writer.

### Buckets
In CalicoDB, buckets are persistent, ordered mappings from keys to values.
Each open bucket is represented by a `calicodb::Bucket` handle.
The database manages a "main bucket" that is created when the first transaction starts.
Additional buckets can be created inside this bucket.

```C++
#include "calicodb/bucket.h"

calicodb::Bucket &b = tx->main_bucket();

s = b.put("lilly", "calico");
if (s.is_ok()) {
    // Mapping from "lilly" -> "tuxedo" has been added to b.
}

std::string value;
s = b.get("lilly", &value);
if (s.is_ok()) {
    // value contains "calico".
} else if (s.is_not_found()) {
    // Key "lilly" was not found in b.
} else {
    // Some other error occurred. Probably an I/O error.
}

s = b.erase("key");
if (s.is_ok()) {
    // No record with key "key" exists in b.
} else {
    // An error occurred. Note it is not an error if "key" does not exist. 
}
```

A nested bucket is a bucket that is rooted at some record in another bucket.
Records representing buckets cannot be accessed or modified via the normal put-get-erase machinery.
They must be managed using the `Bucket::*_bucket()` methods.
If a record/bucket is accessed via the wrong method, a status will be returned for which `Status::is_incompatible_value()` evaluates to true.

```C++
// Create a sub-bucket. Note that this bucket will not persist in the database 
// unless Tx::commit() is called prior to the transaction ending. It is an
// error if the bucket already exists. Note that the second parameter to
// create_bucket() is optional. If omitted, the bucket is created but not opened.
calicodb::Bucket *b2 = nullptr;
s = b.create_bucket("cats", &b2);
if (s.is_ok()) {
    // b holds a handle to the bucket "cats". The bucket will remain open until
    // b is delete'd, which must happen before the transaction is finished. 
}

// Release memory occupied by the sub-bucket.
delete b2;

// Since the bucket "cats" already exists, we can open it via the following:
s = b.open_bucket("cats", b2);
assert(s.is_ok());

// Buckets can be nested arbitrarily.
calicodb::Bucket *b3 = nullptr;
s = b2->create_bucket_if_missing("nest", &b3);
assert(s.is_ok());

// Remove the nested bucket. If the removed bucket itself contains any 
// buckets, those buckets are removed as well. Note that we can access 
// the records and sub-buckets in "nest" through b3 until it is delete'd.
s = b2->drop_bucket("nest");
assert(s.is_ok());

delete b2;

// Close the last (and only) handle to "nest". Since it has been dropped
// already, its pages are recycled now.
delete b3;
```

### Slices
The `Bucket::*()` methods above accept keys and values as `calicodb::Slice` objects.
`calicodb::Slice` is used to represent an unowned pointer to `char` and a length.
Slices can be created from C-style strings, `CALICODB_STRING`, or directly from their constituent parts.
`CALICODB_STRING`, the user-facing string type used by CalicoDB, defaults to `std::string`, but can be redefined if necessary.

```C++
#include "calicodb/slice.h"

static constexpr auto *kCString = "abc";
CALICODB_STRING cpp_string(kCString);

calicodb::Slice s1(kCString);
calicodb::Slice s2(cpp_string);
calicodb::Slice s3(kCString, 1);

// Conversion back to CALICODB_STRING.
cpp_string = s3.to_string();
```

Slices have some convenience methods for modifying the size and pointer position. 
These methods do not change the underlying data, they just change what range of bytes the slice is currently viewing. 
Comparisons operators are also defined.

```C++
// advance() increments the underlying pointer...
s1.advance(1);

// ...and truncate() decreases the length.
s1.truncate(1);
```

### Cursors
Cursors are used to perform full-bucket scans and range queries.

```C++
#include "calicodb/cursor.h"

// Allocate a cursor handle. c must be delete'd when it is no longer needed.
auto *c = b.new_cursor();

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
```

Cursors can also be used to help modify the database during [read-write transactions](#read-write-transactions).

```C++
// Modify a record.
s = b.put(*c, "cute");
if (s.is_ok()) {
    // c is left on the modified record.
    assert(c->is_valid());
    assert(c->key() == "lilly");
    assert(c->value() == "cute");
}

// Erase a record.
s = b.erase(*c);
if (s.is_ok()) {
    // c is on the record immediately following the erased record, if such a 
    // record exists. Otherwise, c is invalidated.
    assert(!c->is_valid() || "lilly" < c->key());
}
```

Since CalicoDB supports nested buckets, a valid cursor can be on either a normal record or a bucket record.
Given that `Cursor::is_valid()` has returned true and the cursor has not moved, `Cursor::is_bucket()` can be used to check if the cursor is positioned on a bucket record.
Bucket records always return an empty slice from `Cursor::value()`, and `Cursor::key()` returns the nested bucket's name (which can be passed to `Bucket::open_bucket()`).
```C++
if (!c->is_valid()) {
    // Not on a record
} else if (c->is_bucket()) {
    // On a bucket record
} else {
    // On a normal record
}
```

Close the cursor.
```C++
delete c;
```

### Database properties

```C++
calicodb::Stats stats;
s = db->get_property("calicodb.stats", &stats);
if (s.is_ok()) {
    // stats contains database statistics.
} else if (s.is_not_found()) {
    // Property does not exist.
}

// Passing nullptr for the property value causes get_property() to perform a simple existence check, 
// without attempting to populate the property value.
s = db->get_property("calicodb.stats", nullptr);
assert(s.is_ok());
```

### Checkpoints
Pages that are modified during transactions are written to the WAL, not the database file.
At some point, it is desirable to write the pages accumulated in the WAL back to the database.
This operation is called a checkpoint.
Note that automatic checkpoints can be run using the `auto_checkpoint` option (see [Opening a database](#opening-a-database)).
Automatic checkpoints are attempted when transactions are started.

```C++
// This transaction was started earlier, in #manual-transactions. It must be
// finished before the database can be checkpointed.
delete tx;

// Now we can run a checkpoint. See DB::view/DB::update for an API that eases
// some pain associated with transaction lifetimes.

calicodb::CheckpointInfo info = {};

// If the `mode` parameter to DB::checkpoint() is equal to kCheckpointRestart, 
// the DB will set things up such that the next writer writes to the start of 
// the WAL file again. This involves blocking until other connections are 
// finished with the WAL.
s = db->checkpoint(calicodb::kCheckpointRestart, &info);
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
The last connection to a particular database to close will checkpoint and unlink the WAL file.
If a WAL is left behind after closing, then something has gone wrong.
CalicoDB will attempt recovery on the next startup.

```C++
delete db;
```

### Destroying a database

```C++
s = calicodb::DB::destroy(options, "/tmp/calicodb_example");
assert(s.is_ok());
```

### Global options
Per-process options are queried and set using `calicodb::configure()`.
This API is not thread safe, and options should be set before any other library functions are called or objects created.
For example, `calicodb::configure()` can be used to set the general-purpose allocator used to get heap memory for the library.
Changing the allocator while the library is using heap memory can cause a malloc/free mismatch, which is undefined behavior.

```C++
#include "calicodb/config.h"

// Set allocation routines that always fail.
calicodb::AllocatorConfig config = {
    [](auto) -> void * {return nullptr;},
    [](auto *, auto) -> void * {return nullptr;},
    [](auto *) -> void {},
};
calicodb::Status rc = calicodb::configure(calicodb::kReplaceAllocator, &config);
assert(rc.is_ok());

// Use the default allocator again.
rc = calicodb::configure(calicodb::kRestoreAllocator, nullptr);
assert(rc.is_ok());
```

## Resources
1. [Let's Build a Simple Database](https://cstack.github.io/db_tutorial/)
    + Tutorial on database development in C
2. [Architecture of SQLite](https://www.sqlite.org/arch.html)
    + Much of this project was inspired by SQLite3, both the architecture design documents and the source code
    + Especially see the B-tree design document, as well as `btree.h`, `btree.c`, and `btreeInt.h`
3. [LevelDB](https://github.com/google/leveldb)
    + `Env`, `Cursor`, `Slice`, and `Status` APIs are inspired by LevelDB
    + Some parts of the CMake build process are taken from their `CMakeLists.txt`
4. [BoltDB](https://github.com/boltdb/bolt)
    + Inspiration for the transaction API
