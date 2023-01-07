///* api.cpp: Example usage of the Calico DB API.
// *
// * Code from this file is automatically embedded in markdown files using @tokusumi/markdown-embed-code. When editing this file, be sure to
// * check the line numbers and comments to make sure everything is consistent. Also, this file should be kept short as it is already a bit of
// * a nightmare to edit.
// *
// * TODO: Above statement is a lie. We don't have @tokusumi/markdown-embed-code set up yet! Copy-pasting the code fragments into api.md for now...
// */
//
//#include "calico/calico.h"
//#include "spdlog/spdlog.h"
//#include "spdlog/fmt/fmt.h"
//
//static auto assert_ok(const Calico::Status &s)
//{
//    if (!s.is_ok()) {
//        fmt::print(stderr, "{}\n", s.what().data());
//        std::exit(EXIT_FAILURE);
//    }
//}
//
//auto main(int, const char *[]) -> int
//{
//    /* slices */
//
//    {
//        std::string str {"abc"};
//
//        // We can create slices in a few different ways.
//        Calico::Slice s {str};
//        Calico::Slice s2 {str.data(), str.size()};
//
//        // Create an owned string.
//        auto str2 = s.to_string();
//
//        // Slices have methods for modifying the size and pointer position. These methods do not change the
//        // underlying data, they just change what range of bytes the slice is currently "viewing". advance()
//        // increments the underlying pointer...
//        s.advance(1);
//
//        // ...and truncate() decreases the size.
//        s.truncate(1);
//
//        // Comparison operations are implemented.
//        assert(s == "b");
//        assert(s2.starts_with("ab"));
//    }
//
//    /* opening-a-database (1) */
//
//    // Create the database object.
//    Calico::Database db;
//
//    /* opening-a-database (2) */
//
//    {
//        Calico::Options options;
//
//        // Use pages of size 2 KB with 2 MB of cache.
//        options.page_size = 0x2000;
//        options.cache_size = 0x200000;
//
//        // Flush 256 WAL blocks per segment.
//        options.wal_limit = 256;
//
//        // Use 15% of the cache memory for WAL scratch.
//        options.wal_split = 15;
//
//        // Store the WAL segments in a separate location.
////        options.wal_prefix = "/tmp/calico_wal";
//
//        // Write colorful log messages to stderr.
//        options.log_level = Calico::LogLevel::TRACE;
//        options.log_target = Calico::LogTarget::STDERR_COLOR;
//
//        // Open or create a database at "/tmp/cats".
//        auto s = db.open("/tmp/cats", options);
//
//        // Handle failure. s.what() provides information about what went wrong.
//        if (!s.is_ok()) {
//            fmt::print("error: {}\n", s.what().data());
//            std::exit(EXIT_FAILURE);
//        }
//    }
//
//    /* updating-a-database */
//
//    {
//        // Insert a key-value pair. We can use arbitrary bytes for both the key and value.
//        auto s = db.insert("\x11\x22\x33", "\xDD\xEE\xFF");
//
//        // Again, the status object reports the outcome of the operation. Since we are not inside a transaction, all modifications
//        // made to the database are applied atomically. This means that if this status is OK, then our key-value pair is safely on
//        // disk (on the WAL disk, but not the database disk yet). This has a lot of overhead, so using a transaction is desirable
//        // if multiple modifications need to be performed at once.
//        if (!s.is_ok()) {
//        }
//
//        // We can erase records by key, or by passing a cursor object (see Queries below). It should be noted that a cursor used to
//        // erase a key will be invalidated if the operation succeeds.
//        s = db.erase("42");
//
//        // If the key is not found (or the cursor is invalid), we'll receive a "not found" status.
//        assert(s.is_not_found());
//    }
//
//    /* querying-a-database */
//
//    {
//        // We can find the first record greater than or equal to a given key...
//        auto c1 = db.find("\x10\x20\x30");
//
//        // ...or, we can try for an exact match.
//        auto c2 = db.find_exact("\x10\x20\x30");
//
//        // Both methods return cursors, which point to database records and can be used to perform range queries. We check if a
//        // cursor is valid (i.e. it points to an existing record and has an OK internal status) by writing:
//        if (c1.is_valid()) {
//
//        }
//
//        // As mentioned above, cursors store and provide access to a status object. We check this status using the status()
//        // method. Once a cursor's status becomes non-OK, it will stay that way and the cursor can no longer be used.
//        [[maybe_unused]] auto s = c1.status();
//
//        // Calico DB provides methods for accessing the first and last records. Like the find*() methods, these methods return
//        // cursors. This lets us easily traverse all records in order.
//        for (auto c = db.first(); c.is_valid(); ++c) {}
//
//        // We can also traverse in reverse order...
//        for (auto c = db.last(); c.is_valid(); c--) {}
//
//        // ...or from the start to some arbitrary point. In this example, the cursor we are iterating to is not valid. This is
//        // the same as iterating until we hit the end.
//        for (auto c = db.first(), bounds = db.find("42"); c.is_valid() && c != bounds; c++) {}
//
//        // We can also use key comparisons.
//        for (auto c = db.first(); c.is_valid() && c.key() < "42"; c++) {}
//    }
//
//    /* transaction-objects */
//
//    {
//        // Start a transaction. All modifications made to the database while this object is live will be part of the transaction
//        // it represents.
//        auto xact = db.transaction();
//
//        auto s = db.erase(db.first());
//        assert_ok(s);
//
//        // If this status is OK, every change made in the transaction will be undone.
//        s = xact.abort();
//        assert_ok(s);
//
//        // If we want to start another transaction, we need to make another call to the database.
//        xact = db.transaction();
//
//        s = db.erase(db.first());
//        assert_ok(s);
//
//        // This time we'll commit the transaction. Note that if the transaction object goes out of scope before either abort()
//        // or commit() is called, an abort() will be attempted automatically.
//        s = xact.commit();
//        assert_ok(s);
//    }
//
//    /* info-objects */
//
//    {
//        // We can use an info object to get information about the database state.
//        const auto info = db.info();
//        [[maybe_unused]] const auto rc = info.record_count();
//        [[maybe_unused]] const auto pc = info.page_count();
//        [[maybe_unused]] const auto ks = info.maximum_key_size();
//        [[maybe_unused]] const auto hr = info.cache_hit_ratio();
//
//        // The page size is fixed at database creation time. If the database already existed, the page size passed to the
//        // constructor through Calico::Options is ignored. We can query the real page size using the following line.
//        [[maybe_unused]] const auto ps = info.page_size();
//    }
//
//    /* closing-a-database */
//
//    {
//        auto s = db.close();
//        assert_ok(s);
//    }
//
//    // NOTE: Reopen the database so destroy() works.
//    assert_ok(db.open("/tmp/cats"));
//
//    /* destroying-a-database */
//
//    {
//        auto s = Calico::Database::destroy(std::move(db));
//        assert_ok(s);
//    }
//
//    return 0;
//}

#include <calico/calico.h>
#include <chrono>
#include <filesystem>

auto main(int, char const *[]) -> int
{
    using SystemClock = std::chrono::system_clock;

    static constexpr auto PATH = "__calico_example__";

    // We need 20 chars to hold "YYYY-mm-dd HH:MM:SS" + '\0'.
    char key_buffer[20];

    // Use strings of form "%Y-%m-%d %H:%M:%S" as keys to get a temporal ordering. We could omit the separating
    // characters to save a bit of space, but we'll leave them in so that we can print the keys directly.
    const auto now = [&key_buffer] {
        const auto datetime = SystemClock::to_time_t(SystemClock::now());
        std::strftime(key_buffer, sizeof(key_buffer), "%F %T", std::localtime(&datetime));
        return key_buffer;
    };

    const auto expect = [](const auto &s) {
        if (!s.is_ok()) {
            // The message allocated by the status object is null-terminated, so we can use the pointer as a
            // C-style string. We could also call to_string() on it to get an owned string.
            printf("error: %s", s.what().data());
            std::exit(EXIT_FAILURE);
        }
    };

    std::filesystem::remove_all(PATH);

    Calico::Database db;
    expect(db.open(PATH));

    expect(db.insert(std::string {now()} + " #0", "0"));
    expect(db.insert(std::string {now()} + " #1", "1"));
    expect(db.insert(std::string {now()} + " #2", "2"));
    expect(db.insert(std::string {now()} + " #3", "3"));
    expect(db.insert(std::string {now()} + " #4", "4"));

    for (auto c = db.first(); c.is_valid(); ++c)
        printf("%s @ %s\n", c.value().c_str(), c.key().to_string().c_str());
    return 0;
}