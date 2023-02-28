# CalicoDB Documentation

+ [Build](#build)
+ [API](#api)
  + [Slices](#slices)
  + [Opening a database](#opening-a-database)
  + [Updating a database](#updating-a-database)
  + [Querying a database](#querying-a-database)
  + [Vacuuming a database](#vacuuming-a-database)
  + [Transactions](#transactions)
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
To build the library in release mode without tests, the last command would look like:
```bash
cmake -DCMAKE_BUILD_TYPE=Release -DCALICO_BuildTests=Off .. && cmake --build .
```

## API

### Slices
```C++
std::string str {"abc"};

// We can create slices from C-style strings, standard library strings, or directly from a pointer and a length.
calicodb::Slice s1 {str.c_str()};
calicodb::Slice s2 {str};
calicodb::Slice s3 {str.data(), str.size()};

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
const calicodb::Options options {
    // Use pages of size 2 KiB, a 2 MiB page cache, and a 1 MiB WAL write buffer.
    .page_size = 0x2000,
    .cache_size = 0x200000,
    .wal_buffer_size = 0x100000,
    
    // Store the WAL segments in a separate location. The directory "location" must already exist.
    // WAL segments will look like "/location/calico_wal_#", where # is the segment ID.
    .wal_prefix = "/location/calico_wal_",
    
    // The database instance will write info log messages at the specified log level, to the object
    // passed in the "info_log" member.
    .log_level = calicodb::LogLevel::TRACE,
    .info_log = nullptr,
    
    // This can be used to inject a custom storage implementation. (see the DynamicMemory class in
    // tools/tools.h for an example that stores its files in-memory)
    .storage = nullptr,
};

// Create or open a database at "/tmp/cats".
calicodb::DB *db;
auto s = calicodb::DB::open("/tmp/cats", options, &db);

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
if (const calicodb::Status s = db->put("a", "1"); s.is_system_error()) {
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
calicodb::Cursor *cursor = db->new_cursor();

// Seek to the first record greater than or equal to the given key.
cursor->seek("a");

if (cursor->is_valid()) {
    // If the cursor is valid, these calls can be made:
    const calicodb::Slice k = cursor->key();
    const calicodb::Slice v = cursor->value();
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
    // Unused database pages have been reclaimed and the file truncated. At present, vacuum is considered part of a
    // transaction, even though it doesn't change the logical contents of the database. This may change in the future.
}
```

### Transactions
A transaction represents a unit of work in CalicoDB.
The first transaction is started when the database is opened. 
Otherwise, transaction boundaries are defined by calls to `DB::commit()`.
All updates that haven't been committed when the database is closed will be reverted.

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
if (const auto s = calicodb::DB::destroy("/tmp/cats", options); s.is_ok()) {
    // Database has been destroyed.
} else if (s.is_not_found()) {
    // The database does not exist.
} else if (s.is_system_error()) {
    // A system-level error has occurred.
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
5. https://arpitbhayani.me/blogs/2q-cache
    + Description of the 2Q cache replacement policy
6. https://stablecog.com/
    + Used to generate the original calico cat image, which was then further modified to produce [mascot.png](mascot.png)