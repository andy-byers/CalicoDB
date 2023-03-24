# CalicoDB Documentation

+ [Build](#build)
+ [API](#api)
  + [Slices](#slices)
  + [Opening a database](#opening-a-database)
  + [Updating a database](#updating-a-database)
  + [Querying a database](#querying-a-database)
  + [Vacuuming a database](#vacuuming-a-database)
  + [Tables](#tables)
  + [Checkpoints](#checkpoints)
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
cmake -DCMAKE_BUILD_TYPE=Release -DCALICODB_BuildTests=Off .. && cmake --build .
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
    // Use pages of size 2 KB and a 2 MB page cache.
    .page_size = 0x2000,
    .cache_size = 0x200000,
    
    // Store the WAL segments in a separate location. The directory "location" must already exist.
    // WAL segments will look like "/location/calicodb_wal_#", where # is the segment ID.
    .wal_prefix = "/location/calicodb_wal_",
    
    .info_log = nullptr,
    
    // This can be used to inject a custom Env implementation. (see the tools::FakeEnv class in
    // tools/tools.h for an example that stores its files in memory).
    .env = nullptr,
};

// Create or open a database at "/tmp/cats".
calicodb::DB *db;
calicodb::Status s = calicodb::DB::open(options, "/tmp/cats", db);

// Handle failure. s.to_string() provides a string representation of the status.
if (!s.is_ok()) {

}
```

### Updating a database
In CalicoDB, records are stored in tables, which are on-disk mappings from keys to values.
Keys within each table are unique, however, tables may have overlapping key ranges.
Errors returned by methods that modify tables are fatal and the database will refuse to perform any more work.
The next time that the database is opened, recovery will be run to undo any changes that occurred after the last checkpoint (see [Checkpoints](#checkpoints)).
The database will always keep 1 table open, called the default table.
Additional tables are managed using methods on the `DB` object (see [Tables](#tables)).
When naming tables, note that the prefix "calicodb." is reserved for internal use.

```C++
// Insert some key-value pairs into the default table.
calicodb::Status s = db->put("lilly", "calico");
if (s.is_io_error()) {
    // Handle a system-level or I/O error.
}

s = db->put("freya", "orange tabby");
if (s.is_ok()) {
    // Record was inserted.
}

// Keys are unique within each table, so inserting a record with a key that already exists will 
// cause the old value to be overwritten.
s = db->put("lilly", "sweet calico");
if (s.is_ok()) {
    // Value was modified.
}

// Erase a record.
s = db->erase("42");
if (s.is_ok()) {
    // Record was erased.
} else if (s.is_not_found()) {
    // Key does not exist.
} else {
    // An error occurred.
}
```

### Querying a database

```C++
// Query a value by key. Note that the "value" parameter is a pointer, indicating 
// that it is optional. If omitted, the DB will check if the key "lilly" exists, 
// without attempting to determine its value.
std::string value;
calicodb::Status s = db->get("lilly", &value);
if (s.is_ok()) {
    // value is populated with the record's value.
} else if (s.is_not_found()) {
    // Key does not exist.
} else {
    // An error occurred.
}

// Allocate a cursor. The cursor will only view records from the table it was
// created on. Modifications to the table will invalidate the cursor, including
// calls to DB::vacuum(). Cursors are invalid upon creation. They must have
// either seek(), seek_first(), or seek_last() called before is_valid() will
// return true.
calicodb::Cursor *cursor = db->new_cursor();

// Seek to the first record in the default table with a key greater than or equal 
// to the given key.
cursor->seek("freya");

if (cursor->is_valid()) {
    // If the cursor is valid, these calls can be made:
    calicodb::Slice k = cursor->key();
    calicodb::Slice v = cursor->value();
} else if (cursor->status().is_not_found()) {
    // Key was greater than any key in the table, or the table is empty.
} else {
    // Handle a system-level read error. These are not considered fatal errors.
}

// Iterate through the whole table in order.
cursor->seek_first();
for (; cursor->is_valid(); cursor->next()) {

}

// Iterate through in reverse order.
cursor->seek_last();
for (; cursor->is_valid(); cursor->previous()) {

}

// Iterate through a half-open range of keys ["freya", "lilly").
cursor->seek("freya");
for (; cursor->is_valid() && cursor->key() < "lilly"; cursor->next()) {

}

// Free the cursor.
delete cursor;
```

### Vacuuming a database

```C++
calicodb::Status s = db->vacuum();
if (s.is_ok()) {
    // Unused database pages have been reclaimed and the file truncated. At present, vacuum is considered part of a
    // transaction, even though it doesn't change the logical contents of the database. This may change in the future.
}
```

### Tables

```C++
calicodb::TableOptions table_options = {
    // Pass AccessMode::kReadOnly to open in read-only mode.
    .mode = AccessMode::kReadWrite,
};

// Open or create a table.
calicodb::Table *table_1, *table_2;
calicodb::Status s = db->create_table(table_options, "name_1", table_1);
if (s.is_ok()) {
    // "table_1" contains a heap-allocated table handle.
}

// Now, "table_1" can be passed as the first parameter to DB methods that access or modify data. Those calls
// will be directed to the table represented by "table_1", rather than the default table.
s = db->put(*table_1, "key", "value");
assert(s.is_ok());

// Any number of tables may be open at the same time, however, each table can only have 1 handle in existence.
s = db->create_table(table_options, "name_2", table_2);
assert(s.is_ok());

// Tables should be closed when they are no longer needed.
s = db->close_table(table_1);
assert(s.is_ok());

// Tables can also be dropped, which will erase their records and remove them from the table list. Note that
// close_table() and drop_table() take ownership of the heap-allocated table handle, as well as NULL-out the
// pointer. Calling close_table() or drop_table() on a nullptr is a NOOP.
s = db->drop_table(table_2);
assert(s.is_ok());

std::vector<std::string> tables;
s = db->list_tables(tables);
if (s.is_ok()) {
    // "tables" contains the name of each table, in unspecified order. The default table is not included.
}
```

### Checkpoints
CalicoDB uses the concept of a checkpoint to provide guarantees about the logical contents of a database.
Any work that took place before a successful checkpoint will persist, even if the program crashes afterward.
It should also be noted that this also applies to creation and removal of tables.
At this point, checkpoints are global: they affect every table with pending updates.
Further work may go toward implementing per-table checkpoints.

```C++
// Add some more records.
calicodb::Status s = db->put("fanny", "persian");
assert(s.is_ok());

s = db->put("myla", "brown-tabby");
assert(s.is_ok());

// Perform a checkpoint.
s = db->checkpoint();
if (s.is_ok()) {
    // Changes are safely on disk (in the WAL, and maybe partially in the database). If we crash from 
    // here on out, the changes will be reapplied from the WAL the next time the database is opened.
}
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
During close, the database is made consistent and the whole WAL is removed.
If an instance leaves any WAL segments behind after closing, then something has gone wrong.
CalicoDB will attempt recovery on the next startup.
It should be noted that all tables opened by the database must be closed before the database itself is closed.

```C++
delete db;
```

### Destroying a database

```C++
calicodb::Status s = calicodb::DB::destroy(options, "/tmp/cats");
if (s.is_ok()) {
    // Database has been destroyed.
} else if (s.is_not_found()) {
    // The database does not exist.
} else if (s.is_io_error()) {
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
5. https://stablecog.com/
    + Used to generate the original calico cat image, which was then further modified to produce [mascot.png](mascot.png)