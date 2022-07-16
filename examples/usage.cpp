
#include "calico/calico.h"
#include <filesystem>
#include <spdlog/fmt/fmt.h>

namespace {

constexpr auto PATH = "/tmp/calico_usage";

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

    // Bytes objects can modify the underlying string, while BytesView objects cannot.
    b[0] = '\xFF';
    assert(data[7] == '\xFF');
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
    for (const auto &[key, value]: records) {
        const auto s = db.insert(key, value);
        if (!s.is_ok()) {
            fmt::print("cannot insert record ({}, {})", key, value);
            fmt::print(s.what());
            return;
        }
    }
    assert(db.insert("bengal", "short;spotted,marbled,rosetted").is_ok());
    assert(db.insert("turkish vankedisi", "long;white").is_ok());
    assert(db.insert("moose", "???").is_ok());
    assert(db.insert("abyssinian", "short;ticked tabby").is_ok());
    assert(db.insert("russian blue", "short;blue").is_ok());
    assert(db.insert("american shorthair", "short;all").is_ok());
    assert(db.insert("badger", "???").is_ok());
    assert(db.insert("manx", "short,long;all").is_ok());
    assert(db.insert("chantilly-tiffany", "long;solid,tabby").is_ok());
    assert(db.insert("cyprus", "...").is_ok());

    // Modify a record.
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
    assert(cursor.is_valid());
    assert(cursor.key() == key);

    // If there isn't such a record, the cursor will be invalid.
    assert(not db.find_exact("not found").is_valid());

    // find() returns a cursor on the first record that does not compare less than the given key.
    const auto prefix = key.copy().truncate(key.size() / 2);
    assert(db.find(prefix).key() == cursor.key());

    // Cursors can be used for range queries. They can traverse the database in sequential order,
    // or in reverse sequential order.
    for (auto c = db.find_minimum(); c.is_valid(); c++) {}
    for (auto c = db.find_maximum(); c.is_valid(); c--) {}

    // They also support equality comparison.
    if (const auto boundary = db.find_exact(key); boundary.is_valid()) {
        for (auto c = db.find_minimum(); c.is_valid() && c != boundary; c++) {}
        for (auto c = db.find_maximum(); c.is_valid() && c != boundary; c--) {}
    }
}

auto transactions(cco::Database &db)
{
    // Commit all the updates we made in the previous examples.
    assert(db.commit().is_ok());

    // Make some changes and abort the transaction.
    assert(db.insert("opossum", "pretty cute").is_ok());
    assert(db.erase(db.find_minimum()).is_ok());
    assert(db.erase(db.find_maximum()).is_ok());
    assert(db.abort().is_ok());

    // All updates since the last call to commit() have been reverted.
    auto opossum = db.find_exact("opposum");
    assert(not opossum.is_valid() and opossum.status().is_ok());
    assert(db.find_minimum().key() == cco::stob("abyssinian"));
    assert(db.find_maximum().key() == cco::stob("turkish vankedisi"));
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
        fmt::print("(2/2) {}\n", s.what());
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
    updating_a_database(db);
    querying_a_database(db);
    transactions(db);
    deleting_a_database(std::move(db));
    return 0;
}
