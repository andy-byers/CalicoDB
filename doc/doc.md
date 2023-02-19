# Calico DB Documentation
Calico DB is designed to be as simple as possible.
The API is based off that of LevelDB, but the backend uses a B<sup>+</sup>-tree rather than a log-structured merge (LSM) tree.

+ [Build](#build)
+ [API](#api)
  + [Slices](#slices)
  + [Opening a database](#opening-a-database)
  + [Updating a database](#updating-a-database)
  + [Querying a database](#querying-a-database)
  + [Vacuuming a database](#vacuuming-a-database)
  + [Closing a database](#closing-a-database)
  + [Transactions](#transactions)
  + [Destroying a database](#destroying-a-database)
+ [Architecture](#architecture)
+ [Acknowledgements](#acknowledgements)

## Build
Calico DB is built using CMake.
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
To build the library in release mode without tests, the last command would look like:
```bash
cmake -DCMAKE_BUILD_TYPE=Release -DCALICO_BuildTests=Off .. && cmake --build .
```

## API

### Slices
```C++
std::string str {"abc"};

// We can create slices from C-style strings, standard library strings, or directly from a pointer and a length.
Calico::Slice s1 {str.c_str()};
Calico::Slice s2 {str};
Calico::Slice s3 {str.data(), str.size()};

// Slices can be converted back to owned strings using Slice::to_string().
printf("%s\n", s1.to_string().c_str());

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
const Calico::Options options {
    // Use pages of size 2 KiB, a 2 MiB page cache, and a 1 MiB WAL write buffer.
    .page_size = 0x2000,
    .cache_size = 0x200000,
    .wal_buffer_size = 0x100000,
    
    // Store the WAL segments in a separate location. The directory calico_wal must already exist.
    // WAL segments will look like "calico_wal_#", where # is the segment ID.
    .wal_prefix = "calico_wal_",
    
    // The database instance will write info log messages at the specified log level, to the object
    // passed in the "info_log" member.
    .log_level = Calico::LogLevel::TRACE,
    .info_log = nullptr,
    
    // This can be used to inject a custom storage implementation. (see the DynamicMemory class in
    // tools/tools.h for an example that stores its files in-memory)
    .storage = nullptr,
};

// Create or open a database at "/tmp/cats".
Calico::Database *db;
auto s = Calico::Database::open("/tmp/cats", options, &db);

// Handle failure. s.what() provides information about what went wrong in the form of a Slice. Its "data" member is 
// null-terminated, so it can be printed like in the following block.
if (!s.is_ok()) {
    std::fprintf(stderr, "error: %s\n", s.what().data());
    return 1;
}
```

### Updating a database
Errors returned by methods that modify the database are fatal and the database will refuse to perform any more work.
The next time that the database is opened, recovery will be run to undo any uncommitted changes.

```C++
// Insert some key-value pairs.
if (const Calico::Status s = db->put("a", "1"); s.is_system_error()) {
    // Handle a system-level or I/O error.
} else if (s.is_invalid_argument()) {
    // Key was too long. This can be prevented by querying the maximum key size after database creation.
}

if (const auto s = db->put("b", "2"); s.is_ok()) {
    // Record was inserted.
}
if (const auto s = db->put("c", "3"); s.is_ok()) {

}

// Keys are unique within the entire database instance, so inserting a record with a key already in the database will 
// cause the old record to be overwritten.
if (const auto s = db->put("c", "123"); s.is_ok()) {

}

// Erase a record by key.
if (const auto s = db->erase("c"); s.is_ok()) {
    // Record was erased.
} else if (s.is_not_found()) {
    // Key does not exist.
}
```

### Querying a database

```C++
// Query a value by key.
std::string value;
if (const auto s = db->get("a", value); s.is_ok()) {
    // value is populated with the record's value.
} else if (s.is_not_found()) {
    // Key does not exist.
} else {
    // Actual error.
}

// Allocate a cursor.
Calico::Cursor *cursor = db->new_cursor();

// Seek to the first record greater than or equal to the given key.
cursor->seek("a");

if (cursor->is_valid()) {
    // If the cursor is valid, these calls can be made:
    const Calico::Slice k = cursor->key();
    const Calico::Slice v = cursor->value();
} else if (cursor->status().is_not_found()) {
    // Key was greater than any key in the database.
} else {
    // Handle a system-level read error. These are not considered fatal errors.
}

// Iterate through the whole database in order.
cursor->seek_first();
for (; cursor->is_valid(); cursor->next()) {

}

// Iterate in reverse order.
cursor->seek_last();
for (; cursor->is_valid(); cursor->previous()) {

}

// Iterate through a half-open range of keys [a, f).
cursor->seek("a");
for (; cursor->is_valid() && cursor->key() < "f"; cursor->next()) {

}

// Free the cursor.
delete cursor;
```

### Vacuuming a database

```C++
if (const auto s = db->vacuum(); s.is_ok()) {
    // Unused database pages have been reclaimed and the file truncated. Note that calls to vacuum() must occur first
    // in a transaction, i.e. vacuum() must follow a successful call to commit(), or open().
}
```

### Transactions
A transaction represents a unit of work in Calico DB.
The first transaction is started when the database is opened. 
Otherwise, transaction boundaries are defined by calls to `Database::commit()`.
All updates that haven't been committed when the database is closed will be reverted on the next startup.

```C++
if (const auto s = db->put("c", "3"); !s.is_ok()) {
    
}
if (const auto s = db->put("d", "4"); !s.is_ok()) {
    
}

if (const auto s = db->commit(); s.is_ok()) {
    // Changes are safely on disk (in the WAL). If we crash from here on out, the changes will be reapplied
    // during recovery.
}
```

### Database properties

```C++
std::string prop;
bool exists;

// Database properties are made available as strings.
exists = db->get_property("calico.counts", prop);
exists = db->get_property("calico.stats", prop);
```

### Closing a database 

```C++
delete db;
```

### Destroying a database

```C++
if (const auto s = Calico::Database::destroy("/tmp/cats", options); s.is_ok()) {
    // Database has been destroyed.
} else if (s.is_not_found()) {
    // The database does not exist.
} else if (s.is_system_error()) {
    // A system-level error has occurred.
}
```

## Architecture
Calico DB uses a B<sup>+</sup>-tree backend and a write-ahead log (WAL).
Other core modules are located in the `src` directory.
The database is generally represented on disk by a single directory.
The B<sup>+</sup>-tree containing the record store is located in a file called `data` in the main directory.
The WAL segment files can either be located in the main directory, or in a different location, depending on the `wal_prefix` initialization option.
The info log will be created in the main directory as well, unless a custom `Logger *` is passed to the database.
Calico DB runs in a single thread.

### Storage
The storage module handles platform-specific filesystem operations and I/O.
Users can override classes in [`calico/storage.h`](../include/calico/storage.h).
Then, a pointer to the custom `Storage` object can be passed to the database during when it is opened.
See [`test/tools`](../test/tools) for an example that stores the database in memory.

### Pager
The pager module provides in-memory caching for database pages read by the `storage` module.
It is the pager's job to maintain consistency between database pages on disk and in memory.

### Tree
The B<sup>+</sup>-tree logic can be found in the tree module.
The tree is of variable order, so splits are performed when nodes have run out of physical space.
The implementation is pretty straightforward.
We basically do as little as possible to make sure that the tree ordering remains correct.
This results in a less-balanced tree, but seems to be good for write performance.

### WAL
The WAL record format is similar to that of `RocksDB`.
Additionally, we have 2 WAL payload types: deltas and full images.
A full image is generated the first time a page is modified during a transaction.
A full image contain a copy of the page, before anything was changed, and can be used to undo all modifications made during the transaction.
Further modifications to the page will produce deltas, which record only the changed portions of the page (just the "after" contents).
Note that full image records are always disjoint (w.r.t. the affected page IDs) within a single transaction.
This means that they can be applied to the database in any order and produce the same results.
Deltas are not disjoint, so they must be read in order.

### Consistency
Calico DB must enforce certain rules to maintain consistency between the WAL and the database.
First, we need to make sure that all updates are written to the WAL before affected pages are written back to the database file.
The WAL keeps track of the last LSN it flushed to disk (the `flushed_lsn`).
This value is queried by the pager to make sure that unprotected pages are never written back.
The pager keeps track of a few more variables to ensure consistency: the `page_lsn`, the `record_lsn`, the `commit_lsn`, and the `recovery_lsn`.
The `page_lsn` (per page) is the LSN of the last WAL record generated for the page.
This is the value that is compared with the WAL's `flushed_lsn` to make sure the page is safe to write out.
The `record_lsn` (also per page) is the last `page_lsn` value that we already have on disk.
It is saved (in-memory only) when the page is first read into memory, and each time the page is written back.
Then, the lowest `record_lsn` is tracked in the pager's `recovery_lsn`.
The `recovery_lsn` represents the oldest WAL record that we still need.
It is reported back to the WAL intermittently so that obsolete segment files can be removed.

## Acknowledgements
1. https://cstack.github.io/db_tutorial/
    + Awesome tutorial on database development in C
2. https://www.sqlite.org/arch.html
    + Much of this project was inspired by SQLite3, both the architecture design documents and the source code
    + Especially see the B-tree design document, as well as `btree.h`, `btree.c`, and `btreeInt.h`
3. https://github.com/google/leveldb
    + Much of the API is inspired by LevelDB
    + Some parts of the CMake build process is taken from their `CMakeLists.txt`
4. https://github.com/facebook/rocksdb/wiki/Write-Ahead-Log
    + Nice explanation of RocksDB's WAL
    + The idea to have multiple different record types and to use a "tail" buffer are from this document
5. https://arpitbhayani.me/blogs/2q-cache
    + Nice description of the 2Q cache replacement policy
6. https://stablecog.com/
    + Used to generate the original calico cat image, which was then further modified to produce [mascot.png](mascot.png)