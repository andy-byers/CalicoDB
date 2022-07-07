
#include "calico/calico.h"
#include <filesystem>

namespace {

constexpr auto PATH = "/tmp/calico_usage";
namespace cco = calico;

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

    // Advance and truncate with chaining.
    b.advance(7).truncate(5);

    // Comparisons.
    assert(cco::compare_three_way(b, v) != cco::ThreeWayComparison::EQ);
    assert(b == cco::stob("world"));
}

auto updating_a_database(cco::Database &db)
{
    namespace cco = cco;

    // Insert some records. If a record is already in the database, insert() will return false.
    assert(db.insert(cco::stob("bengal"), cco::stob("short;spotted,marbled,rosetted")));
    assert(db.insert(cco::stob("turkish vankedisi"), cco::stob("long;white")));
    assert(db.insert(cco::stob("abyssinian"), cco::stob("short;ticked tabby")));
    assert(db.insert({"russian blue", "short;blue"}));
    assert(db.insert({"american shorthair", "short;all"}));
    assert(db.insert({"badger", "???"}));
    assert(db.insert({"manx", "short,long;all"}));
    assert(db.insert({"chantilly-tiffany", "long;solid,tabby"}));
    assert(db.insert({"cyprus", "all;all"}));

    // Erase a record by key.
    assert(db.erase(cco::stob("badger")));
}

auto querying_a_database(cco::Database &db)
{
    namespace cco = cco;
    static constexpr auto target = "russian blue";
    const auto key = cco::stob(target);

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

auto transactions(cco::Database &db)
{
    namespace cco = cco;

    // Commit all the updates we made in the previous examples.
    db.commit();

    // Make some changes and abort the transaction.
    db.insert({"opossum", "pretty cute"});
    assert(db.erase(db.find_minimum()));
    assert(db.erase(db.find_maximum()));
    db.abort();

    // All updates since the last call to commit() have been reverted.
    assert(not db.find(cco::stob("opposum")).is_valid());
    assert(db.find_minimum().key() == cco::stob("abyssinian"));
    assert(db.find_maximum().key() == cco::stob("turkish vankedisi"));
}

auto deleting_a_database(cco::Database db)
{
    // We can delete a database by passing ownership to the following static method.
    cco::Database::destroy(std::move(db));
}

} // namespace

auto main(int, const char *[]) -> int
{
    std::error_code error;
    std::filesystem::remove_all(PATH, error);

    try {
        cco::Options options;
        auto db = cco::Database::open(PATH, options);
        bytes_objects();
        updating_a_database(db);
        querying_a_database(db);
        transactions(db);
        deleting_a_database(std::move(db));
    } catch (const cco::CorruptionError &error) {
        printf("CorruptionError: %s\n", error.what());
    } catch (const cco::IOError &error) {
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
    return 0;
}
