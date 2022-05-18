
Cub DB is an embedded key-value database written in C++17.

### Tentative Public API

Classes:
+ `Database`: Top-level database connection
  + `Reader get_reader()`:
  + `Writer get_writer()`:
+ `Handle`: Handle for interacting with the database
  + `bool increment()`:
  + `bool decrement()`:
  + `RefBytes key()`:
  + `std::string value()`:
  + `bool find(const std::string&)`:
  + `void find_min()`:
  + `void find_max()`:
  + `Reader`: Handle for performing database queries
  + `Writer`: Handle for performing database updates
    + `void modify(const std::string&)`: Modify the record that the cursor is over, leaving the cursor position unchanged.
    + `bool insert(const std::string&, const std::string&)`: Insert a new record, returning true if the key already exists in the database, leaving the cursor on the just-inserted record.
    + `bool remove()`: Remove the record that the cursor is over, leaving the cursor on the proceeding record.

### Internals

### Features

+ Durability provided through write-ahead logging
+ Multiple readers or a single writer can be active concurrently
+ Readers and writers can be interconverted
+ Uses a dynamic-order B-tree to store the whole database in a single file (excluding the WAL)

```
using namespace cub;
Options options;
options.page_size = 0x2000;
options.frame_count = 100;
auto db = Database::open("example_db", options);

// TASK: We have N I/O events to complete. Actions include searching (single elements or ranges), inserting, removing,
//       or modifying. We should be able to run a loop on the main thread and spawn readers or writers as necessary,
//       dispatching them to other threads from a thread pool.

while (!events.is_empty()) {
    auto event = event.dequeue();
    if (event.is_read) {
        if (event.type == Event::FIND) {
            get_thread_and_find(db.get_reader());
        } else if (event.type == Event::FIND_RANGE) {
            get_thread_and_find_range(db.get_reader());
        }
    } else {
        if (event.type == Event::INSERT) {
            get_thread_and_insert(db.get_writer());
        } else if (event.type == Event::REMOVE) {
            get_thread_and_remove(db.get_writer());
        }
    }
}

auto token = db.get_reader();
<read some stuff>
token.upgrade();
<write some stuff and maybe commit>
token.downgrade();
<read more stuff!>
```

### Project Source Tree Overview

```
CubDB
  ┣━╸examples
  ┣━╸include
  ┣━╸src
  ┃  ┣━╸file
  ┃  ┣━╸pool
  ┃  ┣━╸tree
  ┃  ┣━╸utils
  ┃  ┗━╸wal
  ┗━╸test
      ┣━╸integration
      ┣━╸tools
      ┗━╸unit
```

+ `/include`: Public API
+ `/src/file`: OS file module
+ `/src/pool`: Buffer pool module
+ `/src/tree`: B-tree module
+ `/src/utils`: Utility module
+ `/src/wal`: Write-ahead logging module
+ `/test/integration`: Integration tests
+ `/test/tools`: Test tools
+ `/test/unit`: Unit tests

Read-only sequential file handle to read the file header [temporary]
Read-write random-access file handle for the database pager
Read-only random-access file handle for the WALReader
Write-only log file handle for the WALWriter