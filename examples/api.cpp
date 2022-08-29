/* api.cpp: Example usage of the Calico DB API.
 *
 * Code from this file is automatically embedded in markdown files using @tokusumi/markdown-embed-code. When editing this file, be sure to
 * check the line numbers and comments to make sure everything is consistent. Also, this file should be kept short as it is already a bit of
 * a nightmare to edit.
 */

#include "calico/calico.h"

static auto assert_ok(const calico::Status &s)
{
    if (!s.is_ok()) {
        fmt::print(stderr, "{}\n", s.what());
        std::exit(EXIT_FAILURE);
    }
}

auto main(int, const char *[]) -> int
{
    namespace cco = calico;

    /** bytes-objects **/

    {
        auto function_taking_a_bytes_view = [](cco::BytesView) {};

        std::string data {"Hello, world!"};

        // Construct slices from a string. The string still owns the memory, the slices just refer
        // to it.
        cco::Bytes b {data.data(), data.size()};
        cco::BytesView v {data.data(), data.size()};

        // Convenience conversion from a string.
        const auto from_string = cco::stob(data);

        // Convenience conversion to a string_view.
        assert(cco::btos(from_string) == data);

        // Implicit conversions from `Bytes` to `BytesView` are allowed.
        function_taking_a_bytes_view(b);

        // advance() moves the start of the slice forward and truncate() moves the end of the slice
        // backward.
        b.advance(7).truncate(5);

        // Comparisons.
        assert(b == cco::stob("world"));
        assert(b.starts_with(cco::stob("wor")));

        // Bytes objects can modify the underlying data, while BytesView objects cannot.
        b[0] = '\xFF';
        assert(data[7] == '\xFF');
    }

    /** opening-a-database (1) **/

    // Create the database object.
    cco::Database db;

    /** opening-a-database (2) **/

    {
        // Set some initialization options. We'll use pages of size 2 KB with 2 MB of cache.
        cco::Options options;
        options.page_size = 0x2000;
        options.frame_count = 256;
        options.log_level = spdlog::level::info;

        // Open or create a database at "/tmp/cats".
        auto s = db.open("/tmp/cats", options);

        // Handle failure. s.what() provides information about what went wrong.
        if (!s.is_ok()) {

        }
    }

    /** updating-a-database **/

    {
        // Insert a key-value pair. We can use arbitrary bytes for both the key and value.
        auto s = db.insert("\x11\x22\x33", "\xDD\xEE\xFF");

        // Again, the status object reports the outcome of the operation. Since we are not inside a transaction, all modifications
        // made to the database are applied atomically. This means that if this status is OK, then our key-value pair is safely on
        // disk (on the WAL disk, but not the database disk yet). This has a lot of overhead, so using a transaction is desirable
        // if multiple modifications need to be performed at once.
        if (!s.is_ok()) {

        }

        // We can erase records by key, or by passing a cursor object (see Queries below).
        s = db.erase("42");

        // If the key is not found (or the cursor is invalid), we'll receive a "not found" status.
        assert(s.is_not_found());
    }

    /** querying-a-database **/

    {
        // We can find the first record greater than or equal to a given key...
        auto c1 = db.find("\x10\x20\x30");

        // ...or, we can try for an exact match.
        auto c2 = db.find_exact("/x10/x20/x30");

        // Both methods return cursors, which point to database records and can be used to perform range queries. We check if a
        // cursor is pointing to a record by writing:
        if (c1.is_valid()) {

        }

        // Calico DB provides methods for accessing the first and last records. Like the find*() methods, these methods return
        // cursors. This lets us easily traverse all records in order.
        for (auto c = db.first(); c.is_valid(); ++c) {}

        // We can also traverse in reverse order...
        for (auto c = db.last(); c.is_valid(); c--) {}

        // ...or from the start to some arbitrary point. In this example, the cursor we are iterating to is not valid. This is
        // the same as iterating until we hit the end.
        for (auto c = db.first(); c.is_valid() && c != db.find("42"); c++) {}

        // We can also use the key ordering.
        for (auto c = db.first(); c.is_valid() && c.key() < cco::stob("42"); c++) {}
    }

    /** transaction-objects **/

    {
        // Start a transaction. All modifications made to the database while this object is live will be part of the transaction
        // it represents.
        auto xact = db.transaction();

        auto s = db.erase(db.first());
        assert_ok(s);

        // If this status is OK, every change made in the transaction will be undone. If we receive a non-OK status, we are
        // permitted to retry as many times as we would like, at least, until we receive an OK status.
        s = xact.abort();
        assert_ok(s);

        // If we want to start another transaction, we need to make another call to the database.
        xact = db.transaction();

        s = db.erase(db.first());
        assert_ok(s);

        // This time we'll commit the transaction. If this call returns an OK status, we cannot use the transaction anymore.
        // If an error was encountered, we can still call abort() to attempt to resolve the problem. In fact, if a transaction
        // object ever goes out of scope before a call to either abort() or commit() succeeds, it will automatically attempt
        // to abort the transaction.
        s = xact.commit();
        assert_ok(s);
    }

    /** info-objects **/

    {
        // We can use an info object to get info about the database instance.
        const auto info = db.info();
        [[maybe_unused]] const auto rc = info.record_count();
        [[maybe_unused]] const auto pc = info.page_count();
        [[maybe_unused]] const auto mk = info.maximum_key_size();
    }

    return 0;
}
