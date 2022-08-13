# API
Calico DB aims to provide a simple yet robust API for manipulating ordered data.

+ [Opening a Database](#opening-a-database)
+ [Closing a Database](#closing-a-database)
+ [Bytes Objects](#bytes-objects)
+ [Updating a Database](#updating-a-database)
+ [Querying a Database](#querying-a-database)
+ [Errors](#errors)
+ [Transactions](#transactions)
+ [Deleting a Database](#deleting-a-database)

### Opening a Database
Opening a database is a two-step process.
First, we create the database object using a public constructor.
Next, we call `open()` to open the database connection.

```C++
// Set some options. We'll create a database at "tmp/cats" with pages of size 8 KB and 
// 128 cache frames (4 MB total). We'll also enable logging.
calico::Options options;
options.path = "/tmp/cats";
options.page_size = 0x8000;
options.frame_count = 128;
options.log_level = spdlog::level::info;

// Create the database object. Note that we could just as easily use a smart pointer or
// new/delete to manage the resource.
calico::Database db {options};

// Open the database connection.
if (const auto s = db.open(); !s.is_ok()) {
    fmt::print("(1/2) cannot open database\n");
    fmt::print("(2/2) (reason) {}\n", s.what());
    std::exit(EXIT_FAILURE);
}
// This will be true until db.file_close() is called.
assert(db.is_open());
```

### Closing a Database

```C++
assert(db.file_close().is_ok());
```

### Bytes Objects
Calico DB uses `Bytes` and `BytesView` objects to represent unowned, contiguous sequences of bytes.
`Bytes` objects can modify the underlying data while `BytesView` objects cannot.

```C++
auto function_taking_a_bytes_view = [](calico::BytesView) {};

std::string data {"Hello, world!"};

// Construct slices from a string. The string still owns the memory, the slices just refer
// to it.
calico::Bytes b {data.data(), data.size()};
calico::BytesView v {data.data(), data.size()};

// Convenience conversion from a string.
const auto from_string = calico::stob(data);

// Convenience conversion back to a string. This operation may allocate heap memory.
assert(calico::btos(from_string) == data);

// Implicit conversions from `Bytes` to `BytesView` are allowed.
function_taking_a_bytes_view(b);

// advance() moves the start of the slice forward and truncate moves the end of the slice
// backward.
b.advance(7).truncate(5);

// Comparisons.
assert(calico::compare_three_way(b, v) != calico::ThreeWayComparison::EQ);
assert(b == calico::stob("world"));
assert(b.starts_with(calico::stob("wor")));

// Bytes objects can modify the underlying string, while BytesView objects cannot.
b[0] = '\xFF';
assert(data[7] == '\xFF');
```

### Updating a Database
Records can be added or removed using methods on the `Database` object.
Keys are unique, so inserting a record that already file_exists will cause modification of the existing value.

```C++
std::vector<calico::Record> records {
    {"bengal", "short;spotted,marbled,rosetted"},
    {"turkish vankedisi", "long;white"},
    {"moose", "???"},
    {"abyssinian", "short;ticked tabby"},
    {"russian blue", "short;blue"},
    {"american shorthair", "short;all"},
    {"badger", "???"},
    {"manx", "short,long;all"},
    {"chantilly-tiffany", "long;solid,tabby"},
    {"cyprus", "..."},
};

// Insert some records.
for (const auto &record: records)
    assert(db.insert(record).is_ok());

// Modify a record.
assert(db.insert("cyprus", "all;all").is_ok());

// Erase a record by key.
assert(db.erase("badger").is_ok());

// Erase a record using a cursor (see "Querying a Database" below).
assert(db.erase(db.find_exact("moose")).is_ok());
```

### Querying a Database
The database is queried using cursors returned by the `find*()` methods.

```C++
static constexpr auto target = "russian blue";
const auto key = calico::stob(target);

// find_exact() looks for a record that compares equal to the given key and returns a cursor
// pointing to it.
auto cursor = db.find_exact(key);

// If the cursor is valid (i.e. is_valid() returns true) we are safe to use any of the getter
// methods.
assert(cursor.is_valid());
assert(cursor.key() == key);
assert(cursor.value() == "short;blue");

// If we cannot find an exact match, an invalid cursor will be returned.
assert(not db.find_exact("not_found").is_valid());

// If a cursor encounters an error at any point, it will also become invalidated. In this case,
// it will modify its status (returned by cursor.status()) to contain information about the error.
assert(db.find_exact("").status().is_invalid_argument());

// find() returns a cursor on the first record that does not compare less than the given key.
const auto prefix = key.copy().truncate(key.size() / 2);
assert(db.find(prefix).key() == cursor.key());

// Cursors can be used for range queries. They can traverse the database in sequential order,
// or in reverse sequential order.
for (auto c = db.find_minimum(); c.is_valid(); ++c) {}
for (auto c = db.find_maximum(); c.is_valid(); --c) {}

// They also support equality comparison.
if (const auto boundary = db.find_exact(key); boundary.is_valid()) {
    for (auto c = db.find_minimum(); c.is_valid() && c != boundary; ++c) {}
    for (auto c = db.find_maximum(); c.is_valid() && c != boundary; --c) {}
}
```

### Errors
Methods on the database object that can fail will generally return a `calico::Status` object (similar to and inspired by LevelDB's status object).
If a method returning a cursor encounters an error, the error status will be made available in the cursor's status field.
If an error occurs that could potentially lead to corruption of the database contents, the database object will lock up and refuse to perform any more work.
Rather, the exceptional status that caused the lockup will be returned each time a method call is made.
An error such as this could be caused, for example, by becoming unable to write to disk in the middle of a tree balancing operation.
The lockup can be resolved by a successful call to abort(), which attempts roll back the current transaction.
abort() is reentrant, so it can be called again if it fails.
A good rule of thumb is that if one receives a system error from a call that can modify the database, i.e. insert(), erase(), or commit(), then one should try to abort().
If this isn't possible, it's best to just exit the program.
The next time that the database is started up, it will perform the necessary recovery.

### Deleting a Database
```C++
// We can delete a database by passing ownership to the following static method.
calico::Database::destroy(std::move(db));
```
