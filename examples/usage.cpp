
#include "calico/calico.h"
#include <filesystem>
#include <spdlog/fmt/fmt.h>

namespace {

constexpr auto PATH = "/tmp/calico_usage";

auto bytes_objects()
{
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

    // Advance and truncate with chaining.
    b.advance(7).truncate(5);

    // Comparisons.
    assert(calico::compare_three_way(b, v) != calico::ThreeWayComparison::EQ);
    assert(b == calico::stob("world"));
}

auto updating_a_database(calico::Database &db)
{
    // Insert some records. If a record is already in the database, insert() will return false.
    assert(db.insert(calico::stob("bengal"), calico::stob("short;spotted,marbled,rosetted")));
    assert(db.insert(calico::stob("turkish vankedisi"), calico::stob("long;white")));
    assert(db.insert(calico::stob("abyssinian"), calico::stob("short;ticked tabby")));
    assert(db.insert({"russian blue", "short;blue"}));
    assert(db.insert({"american shorthair", "short;all"}));
    assert(db.insert({"badger", "???"}));
    assert(db.insert({"manx", "short,long;all"}));
    assert(db.insert({"chantilly-tiffany", "long;solid,tabby"}));
    assert(db.insert({"cyprus", "all;all"}));

    // Erase a record by key.
    assert(db.erase(calico::stob("badger")));
}

auto querying_a_database(calico::Database &db)
{
    static constexpr auto target = "russian blue";
    const auto key = calico::stob(target);

    // find() looks for a record that compares equal to the given key and returns a cursor
    // pointing to it.
    auto cursor = db.find(key);
    assert(cursor.is_valid());
    assert(cursor.key() == key);

    // We can use lower_bound() to find the first record that does not compare less than
    // the given key...
    const auto prefix = key.copy().truncate(key.size() / 2);
    assert(db.lower_bound(prefix).key() == cursor.key());

    // ...and upper_bound() to find the first record that compares greater (the same
    // record in this case).
    assert(db.upper_bound(prefix).key() == cursor.key());

    // Cursors returned from the find*() and *_bound() methods can be used for range queries.
    // They can traverse the database in sequential order, or in reverse sequential order.
    for (auto c = db.find_minimum(); c.is_valid(); c++) {}
    for (auto c = db.find_maximum(); c.is_valid(); c--) {}

    // They also support equality comparison.
    if (const auto boundary = db.find(key); boundary.is_valid()) {
        for (auto c = db.find_minimum(); c.is_valid() && c != boundary; c++) {}
        for (auto c = db.find_maximum(); c.is_valid() && c != boundary; c--) {}
    }
}

auto transactions(calico::Database &db)
{
    // Commit all the updates we made in the previous examples.
    db.commit();

    // Make some changes and abort the transaction.
    db.insert({"opossum", "pretty cute"});
    assert(db.erase(db.find_minimum()));
    assert(db.erase(db.find_maximum()));
    db.abort();

    // All updates since the last call to commit() have been reverted.
    assert(not db.find(calico::stob("opposum")).is_valid());
    assert(db.find_minimum().key() == calico::stob("abyssinian"));
    assert(db.find_maximum().key() == calico::stob("turkish vankedisi"));
}

//auto deleting_a_database(calico::Database db)
//{
//    // We can delete a database by passing ownership to the following static method.
//    calico::Database::destroy(std::move(db));
//}

} // namespace

auto main(int, const char *[]) -> int
{
    std::error_code error;
    std::filesystem::remove_all(PATH, error);

    try {
        calico::Options options;
        auto db = calico::Database::open(PATH, options);
        bytes_objects();
        updating_a_database(db);
        querying_a_database(db);
        transactions(db);
//        deleting_a_database(std::move(db));
    } catch (const calico::CorruptionError &error) {
        printf("CorruptionError: %s\n", error.what());
    } catch (const calico::IOError &error) {
        printf("IOError: %s\n", error.what());
    } catch (const std::invalid_argument &error) {
        printf("std::invalid_argument: %s\n", error.what());
    } catch (const std::system_error &error) {
        printf("std::system_error (errno=%d): %s\n", error.code().value(), error.what());
    } catch (const std::exception &error) {
        printf("std::exception: %s\n", error.what());
    } catch (...) {
        puts("...");
    }
    namespace co = calico;

    co::Options options;
    options.page_size = 0x2000;
    options.use_transactions = false;

    auto store = co::Database::temp(options);

    for (auto c = store.find_minimum(); c.is_valid(); c++)
        fmt::print("{}, {}\n", co::btos(c.key()), c.value());

    co::Database::destroy(std::move(store));
    return 0;
}
