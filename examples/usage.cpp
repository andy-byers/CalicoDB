
#include <cub/cub.h>
#include <filesystem>

namespace {

constexpr auto PATH = "/tmp/cub_usage";

auto bytes_objects()
{
    auto function_taking_a_bytes_view = [](cub::BytesView) {};

    std::string data {"Hello, bears!"};

    // Construct slices from a string. The string still owns the memory, the slices just refer to it.
    cub::Bytes b {data.data(), data.size()};
    cub::BytesView v {data.data(), data.size()};

    // Convenience conversion from a string.
    const auto from_string = cub::stob(data);

    // Convenience conversion back to a string. This operation must allocate a new string.
    assert(cub::btos(from_string) == data);

    // Implicit conversions from `Bytes` to `BytesView` are allowed.
    function_taking_a_bytes_view(b);

    // Advance and truncate with chaining.
    b.advance(7).truncate(5);

    // Comparisons.
    assert(cub::compare_three_way(b, v) != cub::ThreeWayComparison::EQ);
    assert(b == cub::stob("bears"));
}

auto updating_a_database(cub::Database &db)
{
    // Insert some records.
    assert(db.write(cub::stob("grizzly bear"), cub::stob("big")));
    assert(db.write(cub::stob("kodiak bear"), cub::stob("awesome")));
    assert(db.write(cub::stob("polar bear"), cub::stob("cool")));
    assert(db.write({"sun bear", "respectable"}));
    assert(db.write({"panda bear", "rare"}));
    assert(db.write({"black bear", "lovable"}));

    // Update an existing record (keys are always unique). write() returns false if the record was already in the database.
    assert(!db.write(cub::stob("grizzly bear"), cub::stob("huge")));

    // Erase a record.
    assert(db.erase(cub::stob("grizzly bear")));
}

auto querying_a_database(cub::Database &db)
{
    // We can require an exact match.
    const auto record = db.read(cub::stob("sun bear"), cub::Ordering::EQ);
    assert(record->value == "respectable");

    // Or, we can look for the first record with a key less than or greater than the given key.
    const auto less_than = db.read(cub::stob("sun bear"), cub::Ordering::LT);
    const auto greater_than = db.read(cub::stob("sun bear"), cub::Ordering::GT);
    assert(less_than->value == "cool");

    // Whoops, there isn't a key greater than "sun bear".
    assert(greater_than == std::nullopt);

    // We can also search for the minimum and maximum.
    const auto smallest = db.read_minimum();
    const auto largest = db.read_maximum();
}

auto cursor_objects(cub::Database &db)
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
    const auto key = cub::btos(cursor.key());
    const auto value = cursor.value();
    printf("Record {%s, %s}\n", key.c_str(), value.c_str()); // Record {black bear, lovable}
}

auto transactions(cub::Database &db)
{
    db.write(cub::stob("a"), cub::stob("1"));
    db.write(cub::stob("b"), cub::stob("2"));
    db.commit();

    db.write(cub::stob("c"), cub::stob("3"));
    assert(db.erase(cub::stob("a")));
    assert(db.erase(cub::stob("b")));
    db.abort();

    // Database still contains {"a", "1"} and {"b", "2"}.
    assert(db.read(cub::stob("a"), cub::Ordering::EQ)->value == "1");
    assert(db.read(cub::stob("b"), cub::Ordering::EQ)->value == "2");
}

auto deleting_a_database(cub::Database db)
{
    // We can delete a database by passing ownership to the following static method.
    cub::Database::destroy(std::move(db));
}

} // namespace

auto main(int, const char *[]) -> int
{
    std::filesystem::remove(PATH);

    try {
        cub::Options options;
        auto db = cub::Database::open(PATH, options);
        bytes_objects();
        updating_a_database(db);
        querying_a_database(db);
        cursor_objects(db);
        transactions(db);
        deleting_a_database(std::move(db));
    } catch (const cub::CorruptionError &error) {
        printf("CorruptionError: %s\n", error.what());
    } catch (const cub::IOError &error) {
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
