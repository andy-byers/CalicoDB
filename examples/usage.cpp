
#include "calico/calico.h"

#define USAGE_ASSERT_OK(s) \
    do { \
        if (!s.is_ok()) { \
            fmt::print(stderr, "{}\n", s.what()); \
            std::exit(EXIT_FAILURE); \
        } \
    } while (0)

auto main(int, const char *[]) -> int
{
    namespace cco = calico;

    cco::Database db;
    cco::Options options;
    options.page_size = 0x2000;
    options.frame_count = 128;
    options.log_level = spdlog::level::info;

    std::string key {"key"};
    std::string value {"value"};

    // Open the database connection.
    auto s = db.open("/tmp/cats", options);
    USAGE_ASSERT_OK(s);

    // Start a transaction.
    auto xact = db.transaction();

    // Insert some (name, coat_color) pairs.
    s = db.insert("lilly", "classic calico");
    USAGE_ASSERT_OK(s);

    s = db.insert("freya", "muted-orange tabby");
    USAGE_ASSERT_OK(s);

    // We can search the database at any time.
    assert(db.find_exact("lilly").is_valid());

    // Commit the transaction. If this succeeds, we will have 2 records safely in the database.
    s = xact.commit();
    USAGE_ASSERT_OK(s);

    {
        // Start another transaction and let it go out of scope without calling commit(). This causes the transaction
        // to be aborted. Note that we could have called unwanted.abort() to achieve the same effect.
        auto unwanted = db.transaction();
        s = db.insert(key, value);
        USAGE_ASSERT_OK(s);
    }
    // We can use an info object to get info about the database instance.
    const auto info = db.info();
    fmt::print("{} records on {} page\n", info.record_count(), info.page_count());
    fmt::print("maximum key width is {} bytes\n", info.maximum_key_size());

    // Modifications made to the database outside the control of a transaction object are wrapped in their own
    // transaction. If the operation fails, the database will attempt to revert any changes made so far. If even this
    // fails, the database will need to be closed and reopened to allow recovery to take place.
    s = db.insert(key, value);
    USAGE_ASSERT_OK(s);

    // We can erase records by key, or using a cursor object (see below).
    s = db.erase(key);
    USAGE_ASSERT_OK(s);

    // Iterate through the database in order, using a cursor.
    for (auto c = db.first(); c.is_valid(); ++c)
        fmt::print("{} is a {}\n", cco::btos(c.key()), c.value());

    // Iterate through the database in reverse order.
    for (auto c = db.last(); c.is_valid(); --c)
        fmt::print("{} is a {}\n", cco::btos(c.key()), c.value());

    // Close the database. We must ensure that all transactions are finished beforehand.
    s = db.close();
    USAGE_ASSERT_OK(s);
    return 0;
}
