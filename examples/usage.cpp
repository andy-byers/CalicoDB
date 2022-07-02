
#include "calico/calico.h"
#include <filesystem>

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
    // Insert some records.
    assert(db.insert(calico::stob(""), calico::stob("")));
    assert(db.insert(calico::stob(""), calico::stob("")));
    assert(db.insert(calico::stob(""), calico::stob("")));
    assert(db.insert({"", ""}));
    assert(db.insert({"", ""}));
    assert(db.insert({"", ""}));

    // Update an existing record (keys are unique). write() returns false if the record was
    // already in the database.
    assert(!db.insert(calico::stob(""), calico::stob("")));

    // Erase a record.
    assert(db.erase(calico::stob("")));
}

auto querying_a_database(calico::Database &db)
{
    static constexpr auto target = "sun bear";
    const auto key = calico::stob(target);

    // By default, find() looks for the first record with a key equal to the given key, and
    // returns a cursor pointing to it.
    auto cursor = db.find(key);
    assert(cursor.is_valid());
    assert(cursor.key() == key);

    // We can use the second parameter to leave the cursor on the next record if the provided
    // key does not exist.
    const auto prefix = key.range(0, key.size() / 2);
    assert(db.find(prefix, true).value() == "a");

    // Cursors returned from the find*() methods can be used for range queries. They can be used
    // to traverse the database in sequential order, or in reverse sequential order.
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
    db.insert({"a", "1"});
    db.insert({"b", "2"});
    db.commit();

    db.insert({"c", "3"});
    assert(db.erase(calico::stob("a")));
    assert(db.erase(calico::stob("b")));
    db.abort();

    // Database still contains {"a", "1"} and {"b", "2"}.
    assert(db.find(calico::stob("a")).value() == "1");
    assert(db.find(calico::stob("b")).value() == "2");
}

auto deleting_a_database(calico::Database db)
{
    // We can delete a database by passing ownership to the following static method.
    calico::Database::destroy(std::move(db));
}

} // namespace

auto main(int, const char *[]) -> int
{
    std::filesystem::remove(PATH);

    try {
        calico::Options options;
        auto db = calico::Database::open(PATH, options);
        bytes_objects();
        updating_a_database(db);
        querying_a_database(db);
        transactions(db);
        deleting_a_database(std::move(db));
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
    return 0;
}
