
#include "calico/calico.h"
#include <chrono>
#include <filesystem>
#include <spdlog/fmt/fmt.h>

namespace {

constexpr auto PATH = "/tmp/calico_usage";

#define USAGE_ASSERT_OK(status, message) \
    do { \
        if (!(status).is_ok()) { \
            fmt::print(__FILE__ ": {}\n", (message)); \
            fmt::print(__FILE__ ": (reason) {}\n", (status).what()); \
            std::exit(EXIT_FAILURE); \
        } \
    } while (0)

auto bytes_objects()
{
    auto function_taking_a_bytes_view = [](cco::BytesView) {};

    std::string data {"Hello, world!"};

    // Construct slices from a string. The string still owns the memory, the slices just refer
    // to it.
    cco::Bytes b {data.data(), data.size()};
    cco::BytesView v {data.data(), data.size()};

    // Convenience conversion from a string.
    const auto from_string = cco::stob(data);

    // Convenience conversion back to a string. This operation may allocate heap memory.
    assert(cco::btos(from_string) == data);

    // Implicit conversions from `Bytes` to `BytesView` are allowed.
    function_taking_a_bytes_view(b);

    // advance() moves the start of the slice forward and truncate moves the end of the slice
    // backward.
    b.advance(7).truncate(5);

    // Comparisons.
    assert(cco::compare_three_way(b, v) != cco::ThreeWayComparison::EQ);
    assert(b == cco::stob("world"));
    assert(b.starts_with(cco::stob("wor")));

    // Bytes objects can modify the underlying string, while BytesView objects cannot.
    b[0] = '\xFF';
    assert(data[7] == '\xFF');
}

auto reads_and_writes(cco::Database &db)
{
    static constexpr auto setup_error_message = "cannot setup \"reads_and_writes\" example";
    USAGE_ASSERT_OK(db.insert("2000-10-23 14:23:05", ""), setup_error_message);
    USAGE_ASSERT_OK(db.insert("2000-04-09 09:03:34", ""), setup_error_message);
    USAGE_ASSERT_OK(db.insert("2000-11-01 21:15:45", ""), setup_error_message);
    USAGE_ASSERT_OK(db.insert("2000-09-17 02:54:32", ""), setup_error_message);

    // To insert a new record, we write the following:
    auto s = db.insert("2000-02-06 12:32:19", "");

    // Keys are unique in a Calico DB database, so inserting a record that already exists
    // will overwrite the current value.

    //
}

auto updating_a_database(cco::Database &db)
{
    std::vector<cco::Record> records {
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

    // Keys are unique, so inserting a record with an existing key will modify the
    // existing value.
    assert(db.insert("cyprus", "all;all").is_ok());

    // Erase a record by key.
    assert(db.erase("badger").is_ok());

    // Erase a record using a cursor (see "Querying a Database" below).
    assert(db.erase(db.find_exact("moose")).is_ok());
}

auto querying_a_database(cco::Database &db)
{
    static constexpr auto target = "russian blue";
    const auto key = cco::stob(target);

    // find_exact() looks for a record that compares equal to the given key and returns a cursor
    // pointing to it.
    auto cursor = db.find_exact(key);

    // If the cursor is valid (i.e. is_valid() returns true) we are safe to use any of the getter
    // methods.
    assert(cursor.is_valid());
    assert(cursor.key() == key);
    assert(cursor.value() == "short;blue");

    // If we cannot find an exact match, an invalid cursor will be returned.
    assert(not db.find_exact("not found").is_valid());

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
}

auto batch_writes(cco::Database &db)
{
    cco::Batch batch;

    // Updates made to the batch object are saved in RAM initially.
    batch.insert("opossum", "pretty cute");
    batch.erase("manx");

    // Then, when apply() is called, they are applied to the database in an atomic transaction.
    assert(db.apply(batch).is_ok());

    // If apply() succeeded, then the database will have the entire batch of updates persisted
    // on disk.
    assert(db.find_exact("opossum").is_valid());
    assert(not db.find_exact("manx").is_valid());
}

auto deleting_a_database(cco::Database db)
{
    // We can delete a database by passing ownership to the following static method.
    assert(cco::Database::destroy(std::move(db)).is_ok());
}

auto open_database() -> cco::Database
{
    cco::Options options;
    options.path = PATH;
    options.page_size = 0x8000;
    options.frame_count = 128;
    cco::Database db {options};

    if (const auto s = db.open(); !s.is_ok()) {
        fmt::print("(1/2) cannot open database\n");
        fmt::print("(2/2) (reason) {}\n", s.what());
        std::exit(EXIT_FAILURE);
    }
    assert(db.is_open());
    return db;
}

} // namespace

auto main(int, const char *[]) -> int
{
    std::error_code error;
    std::filesystem::remove_all(PATH, error);

    bytes_objects();
    auto db = open_database();
    reads_and_writes(db);
    updating_a_database(db);
    querying_a_database(db);
    batch_writes(db);
    deleting_a_database(std::move(db));
    return 0;
}
