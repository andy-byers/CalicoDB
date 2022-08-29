/* api.cpp: Example usage of the Calico DB API.
 *
 * Code from this file is automatically embedded in markdown files using @tokusumi/markdown-embed-code. When editing this file, be sure to
 * check the line numbers and comments to make sure everything is consistent. Also, this file should be kept short as it is already a bit of
 * a nightmare to edit.
 *
 * TODO: Above statement is a lie. We don't have @tokusumi/markdown-embed-code set up yet! Copy-pasting the code fragments into api.md for now...
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
        std::string s {"abc"};
        std::string_view sv {"123"};

        // We can create slices from std::string and std::string_view using the convenience functions...
        auto b = cco::stob(s); // Creates a Bytes object.
        auto bv = cco::stob(sv); // Creates a BytesView object.

        // ...or from raw parts using the constructor.
        cco::Bytes b2 {s.data(), s.size()};
        cco::BytesView bv2 {sv.data(), sv.size()};

        // Conversions are allowed from Bytes to BytesView, but not the other way.
        cco::BytesView b3 {b};

        // Both classes can be easily converted into std::string_view. If we want an owned std::string back,
        // however, we must perform the conversion explicitly.
        auto sv2 = cco::btos(b);
        auto s2 = std::string {sv2};

        // Slices have methods for modifying the size and pointer position. These methods do not change the
        // underlying data, they just change what range of bytes the slice is currently "viewing". advance()
        // increments the underlying pointer...
        b.advance(1);

        // ...and truncate() decreases the size.
        b.truncate(1);

        // Comparison operations are implemented.
        assert(b == cco::stob("b"));
        assert(bv.starts_with(cco::stob("ab")));

        // Finally, Bytes can use the non-const overload of operator[](), allowing us to modify the original
        // string.
        b[0] = '\xFF';
        assert(s[1] == '\xFF');
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
        // We can use an info object to get information about the database state.
        const auto info = db.info();
        [[maybe_unused]] const auto rc = info.record_count();
        [[maybe_unused]] const auto pc = info.page_count();
        [[maybe_unused]] const auto mk = info.maximum_key_size();
    }

    return 0;
}
