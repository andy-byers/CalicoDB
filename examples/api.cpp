/* api.cpp: Example usage of the Calico DB API. */

#include "calico/calico.h"
#include <filesystem>

auto main(int, const char *[]) -> int
{
    /* slices */

    {
        std::string str {"abc"};

        // We can create slices from C-style strings, standard library strings, or directly from a pointer and
        // a length.
        Calico::Slice s1 {str.c_str()};
        Calico::Slice s2 {str};
        Calico::Slice s3 {str.data(), str.size()};

        // Slices can be converted back to owned strings using to_string().
        printf("%s\n", s1.to_string().c_str());

        // Slices have methods for modifying the size and pointer position. These methods do not change the
        // underlying data, they just change what range of bytes the slice is currently viewing. advance()
        // increments the underlying pointer...
        s1.advance(1);

        // ...and truncate() decreases the size.
        s1.truncate(1);

        // Comparison operations are implemented.
        assert(s1 == "b");
        assert(s2 < "bc");
        assert(s2.starts_with("ab"));
    }

    /* opening-a-database */

    // Set some initialization options.
    Calico::Options options;

    // Use pages of size 2 KiB, a 2 MiB page cache, and a 1 MiB WAL write buffer.
    options.page_size = 0x2000;
    options.page_cache_size = 0x200000;
    options.wal_buffer_size = 0x100000;

    // Store the WAL segments in a separate location.
    options.wal_prefix = "/tmp/cats_wal";

    // Write colorful log messages to stderr.
    options.log_level = Calico::LogLevel::TRACE;
    options.log_target = Calico::LogTarget::STDERR_COLOR;

    Calico::Database *db;

    {
        std::filesystem::remove_all("/tmp/cats");
        std::filesystem::remove_all(options.wal_prefix.to_string());

        // Create a database at "/tmp/cats".
        auto s = Calico::Database::open("/tmp/cats", options, &db);

        // Handle failure. s.what() provides information about what went wrong in the form of a Slice. Its "data" member is null-
        // terminated, so it can be printed like in the following block.
        if (!s.is_ok()) {
            fprintf(stderr, "error: %s\n", s.what().data());
            return 1;
        }
    }

    /* updating-a-database */

    {
        // Insert a key-value pair. We can use arbitrary bytes for both the key and value, including NULL bytes, provided the slice
        // object knows the proper string length.
        auto s = db->put("\x11\x22\x33", {"\xDD\xEE\x00\xFF", 4});

        // Again, the status object reports the outcome of the operation. Since we are not inside a transaction, all modifications
        // made to the database are applied atomically. This means that if this status is OK, then our key-value pair is safely on
        // disk (on the WAL disk, but not the database disk yet). This has a lot of overhead, so using a transaction is desirable
        // if multiple modifications need to be performed at once.
        if (!s.is_ok()) {

        }

        // We can erase records by key.
        s = db->erase("42");

        // If the key is not found, we'll receive a "not found" status.
        if (s.is_not_found()) {

        }
    }

    /* querying-a-database */

    {
        // Query a value by key.
        std::string value;
        if (db->get("\x10\x20\x30", value).is_ok()) {

        }

        // Allocate a cursor.
        auto *cursor = db->new_cursor();

        // Seek to the first record greater than or equal to the given key.
        cursor->seek("\x10\x20\x30");

        if (cursor->is_valid()) {
            // If the cursor is valid, these methods can be called.
            (void)cursor->key();
            (void)cursor->value();
        } else {
            // Otherwise, the error status can be queried.
            (void)cursor->status();
        }

        cursor->seek_first();
        for (; cursor->is_valid(); cursor.next()) {

        }

        cursor.seek_last();
        for (; cursor.is_valid(); cursor.previous()) {

        }

        // Cursors can be copied.
        auto bounds = cursor;
        bounds->seek_last();

        // Note that out-of-range cursors will never compare equal. If "bounds" was out of range, this would cause an infinite
        // loop without the is_valid() check.
        cursor->seek_first();
        for (; cursor->is_valid() && cursor != bounds; cursor->next()) {

        }

        cursor->seek_last();
        for (; cursor->is_valid() && cursor->key() >= "42"; cursor->previous()) {

        }

        delete cursor;
    }

    /* transactions */

    {
        auto s = db->erase("k");
        if (!s.is_ok()) {

        }

        // If this status is OK, every change made since the last call to commit (or abort) will be undone.
        s = db->abort();
        if (!s.is_ok()) {

        }

        s = db->erase("42");
        if (!s.is_ok()) {

        }

        // This time we'll commit the change.
        s = db->commit();
        if (!s.is_ok()) {

        }
    }

    /* statistics-objects */

    {
        // We can use a statistics object to get information about the database.
        const auto stat = db->statistics();
        (void)stat.record_count();
        (void)stat.page_count();
        (void)stat.maximum_key_size();
        (void)stat.cache_hit_ratio();
        (void)stat.pager_throughput();
        (void)stat.wal_throughput();
        (void)stat.data_throughput();

        // The page size is fixed at database creation time. If the database already existed, the page size passed to the
        // constructor through Calico::Options is ignored. We can query the real page size using the following line.
        (void)stat.page_size();
    }

    /* closing-a-database */

    {
        delete db;
    }

    /* destroying-a-database */

    {
        auto s = Calico::Database::destroy("/tmp/cats", options);
        if (!s.is_ok()) {

        }
    }

    return 0;
}