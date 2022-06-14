
#include <filesystem>
#include <cub/cub.h>

namespace {

constexpr auto PATH = "/tmp/cub_usage";

auto updating_a_database(cub::Database &db)
{
    // Insert some records.
    assert(db.write(cub::_b("grizzly bear"), cub::_b("big")));
    assert(db.write(cub::_b("kodiak bear"), cub::_b("awesome")));
    assert(db.write(cub::_b("polar bear"), cub::_b("cool")));
    assert(db.write(cub::_b("sun bear"), cub::_b("respectable")));
    assert(db.write(cub::_b("panda bear"), cub::_b("rare")));
    assert(db.write(cub::_b("black bear"), cub::_b("lovable")));

    // Update an existing record (keys are always unique). write() returns false if the record was already in the database.
    assert(!db.write(cub::_b("grizzly bear"), cub::_b("huge")));

    // Erase a record.
    assert(db.erase(cub::_b("grizzly bear")));
}

auto querying_a_database(cub::Database &db)
{
    static constexpr bool require_exact{};

    // We can require an exact match.
    const auto record = db.read(cub::_b("sun bear"), cub::Comparison::EQ);
    assert(record->value == "respectable");

    // Or, we can look for the first record with a key less than or greater than the given key.
    const auto less_than = db.read(cub::_b("sun bear"), cub::Comparison::LT);
    const auto greater_than = db.read(cub::_b("sun bear"), cub::Comparison::GT);
    assert(less_than->value == "cool");

    // Whoops, there isn't a key greater than "sun bear".
    assert(greater_than == std::nullopt);

    // We can also search for the minimum and maximum.
    const auto smallest = db.read_minimum();
    const auto largest = db.read_maximum();
}

auto cursor_objects(cub::Database &db)
{
    // The database will remain immutable until all cursors are closed.
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
    const auto key = cub::_s(cursor.key());
    const auto value = cursor.value();
    printf("Record {%s, %s}\n", key.c_str(), value.c_str()); // Record {black bear, lovable}
}

auto batch_objects(cub::Database &db)
{
    // Create a new batch.
    auto batch = db.get_batch();
    batch.write(cub::_b("hello"), cub::_b("1"));
    batch.write(cub::_b("bears"), cub::_b("2"));
    batch.write(cub::_b("world"), cub::_b("3"));

    // Checkpoint our changes.
    batch.commit();

    batch.erase(cub::_b("bears"));

    // Discard all changes since the last commit/abort.
    batch.abort();

    // We can also read from the database using a batch object. This can be useful when we need some read access during an
    // atomic update routine.
    assert(batch.read(cub::_b("hello"), cub::Comparison::EQ)->value == "1");
    assert(batch.read(cub::_b("bears"), cub::Comparison::EQ)->value == "2");
    assert(batch.read(cub::_b("world"), cub::Comparison::EQ)->value == "3");
    const auto minimum = batch.read_minimum();
    const auto maximum = batch.read_maximum();

    // Batches automatically commit when they go out of scope. When this one is destroyed, only {"bears", "2"} will be
    // persisted.
    batch.erase(cub::_b(minimum->key));
    batch.erase(cub::_b(maximum->key));
}

auto transactions(cub::Database &db)
{
    db.write(cub::_b("a"), cub::_b("1"));
    db.write(cub::_b("b"), cub::_b("2"));
    db.commit();

    db.write(cub::_b("c"), cub::_b("3"));
    assert(db.erase(cub::_b("a")));
    assert(db.erase(cub::_b("b")));
    db.abort();

    // Database still contains {"a", "1"} and {"b", "2"}.
    assert(db.read(cub::_b("a"), cub::Comparison::EQ)->value == "1");
    assert(db.read(cub::_b("b"), cub::Comparison::EQ)->value == "2");
}

} // <anonymous>

auto main(int, const char*[]) -> int
{
    using namespace cub;
    std::filesystem::remove(PATH);

    try {
        Options options;
        auto db = Database::open(PATH, options);
        updating_a_database(db);
        querying_a_database(db);
        batch_objects(db);
        cursor_objects(db);
        transactions(db);
    } catch (const CorruptionError &error) {
        printf("CorruptionError: %s\n", error.what());
    } catch (const IOError &error) {
        printf("IOError: %s\n", error.what());
    } catch (const std::invalid_argument &error) {
        printf("std::invalid_argument: %s\n", error.what());
    } catch (const std::system_error &error) {
        printf("std::system_error: %s\n", error.what());
    } catch (const std::exception &error) {
        printf("std::exception: %s\n", error.what());
    } catch (...) {
        puts("\n");
    }
    return 0;
}