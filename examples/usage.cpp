
#include "calico/calico.h"
#include <filesystem>

namespace {

constexpr auto PATH = "/tmp/calico_usage";

auto bytes_objects()
{
    auto function_taking_a_bytes_view = [](calico::BytesView) {};

    std::string data {"Hello, bears!"};

    // Construct slices from a string. The string still owns the memory, the slices just refer to it.
    calico::Bytes b {data.data(), data.size()};
    calico::BytesView v {data.data(), data.size()};

    // Convenience conversion from a string.
    const auto from_string = calico::stob(data);

    // Convenience conversion back to a string. This operation must allocate a new string.
    assert(calico::btos(from_string) == data);

    // Implicit conversions from `Bytes` to `BytesView` are allowed.
    function_taking_a_bytes_view(b);

    // Advance and truncate with chaining.
    b.advance(7).truncate(5);

    // Comparisons.
    assert(calico::compare_three_way(b, v) != calico::ThreeWayComparison::EQ);
    assert(b == calico::stob("bears"));
}

auto updating_a_database(calico::Database &db)
{
    // Insert some records.
    assert(db.write(calico::stob("grizzly bear"), calico::stob("big")));
    assert(db.write(calico::stob("kodiak bear"), calico::stob("awesome")));
    assert(db.write(calico::stob("polar bear"), calico::stob("cool")));
    assert(db.write({"sun bear", "respectable"}));
    assert(db.write({"panda bear", "rare"}));
    assert(db.write({"black bear", "lovable"}));

    // Update an existing record (keys are always unique). write() returns false if the record was already in the database.
    assert(!db.write(calico::stob("grizzly bear"), calico::stob("huge")));

    // Erase a record.
    assert(db.erase(calico::stob("grizzly bear")));
}

auto querying_a_database(calico::Database &db)
{
    // We can require an exact match.
    const auto record = db.read(calico::stob("sun bear"), calico::Ordering::EQ);
    assert(record->value == "respectable");

    // Or, we can look for the first record with a key less than or greater than the given key.
    const auto less_than = db.read(calico::stob("sun bear"), calico::Ordering::LT);
    const auto greater_than = db.read(calico::stob("sun bear"), calico::Ordering::GT);
    assert(less_than->value == "cool");

    // Whoops, there isn't a key greater than "sun bear".
    assert(greater_than == std::nullopt);

    // We can also search for the minimum and maximum.
    const auto smallest = db.read_minimum();
    const auto largest = db.read_maximum();
}

auto cursor_objects(calico::Database &db)
{
    auto cursor = db.get_cursor();
    assert(cursor.has_record());

    // Seek to extrema.
    cursor.find_maximum();
    cursor.find_minimum();

    // Forward traversal.
    assert(cursor.increment());
    assert(cursor.increment(2) == 2);

    // Reverse traversal.
    assert(cursor.decrement());
    assert(cursor.decrement(2) == 2);

    // Key and value access. For the key, we first convert to std::string, since key() returns a BytesView.
    const auto key = calico::btos(cursor.key());
    const auto value = cursor.value();
    printf("Record {%s, %s}\n", key.c_str(), value.c_str()); // Record {black bear, lovable}
}

auto transactions(calico::Database &db)
{
    db.write(calico::stob("a"), calico::stob("1"));
    db.write(calico::stob("b"), calico::stob("2"));
    db.commit();

    db.write(calico::stob("c"), calico::stob("3"));
    assert(db.erase(calico::stob("a")));
    assert(db.erase(calico::stob("b")));
    db.abort();

    // Database still contains {"a", "1"} and {"b", "2"}.
    assert(db.read(calico::stob("a"), calico::Ordering::EQ)->value == "1");
    assert(db.read(calico::stob("b"), calico::Ordering::EQ)->value == "2");
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
        cursor_objects(db);
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
