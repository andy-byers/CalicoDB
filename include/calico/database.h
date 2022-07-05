#ifndef CALICO_DATABASE_H
#define CALICO_DATABASE_H

#include <memory>
#include <optional>
#include "bytes.h"

namespace calico {

class Cursor;
class Info;

/**
 * Represents a relationship between two keys.
 *
 * @see Database::read()
 */
enum class Ordering: unsigned {
    LT = 1, ///< Less than
    LE = 2, ///< Less than or equal to
    EQ = 3, ///< Equal to
    GE = 4, ///< Greater than or equal to
    GT = 5, ///< Greater than
};

/**
 * An object that represents a Cub DB database.
 */
class Database {
public:

    /**
     * Open or create a Cub DB database.
     *
     * @param path The path to the database storage.
     * @param options Options to apply to the database.
     * @return A Cub DB database, located at the provided path on disk.
     */
    static auto open(const std::string &path, Options options) -> Database;

    /**
     * Create an in-memory Cub DB database.
     *
     * @param options Options to apply to the database.
     * @return An in-memory Cub DB database.
     */
    static auto temp(Options options) -> Database;

    /**
     * Destroy a database.
     *
     * Warning: this method is dangerous. It deletes the database and WAL files and cannot be undone. Use
     * at your own risk.
     *
     * @param db The database to destroy.
     */
    static auto destroy(Database db) -> void;

    /**
     * Search for a record.
     *
     * @param key The key to search for.
     * @return A valid cursor positioned on the record with the given key, or an invalid cursor otherwise.
     */
    [[nodiscard]] auto find(BytesView key) const -> Cursor;

    /**
     * Find the first record that does not compare less than the given key.
     *
     * @param key The key to compare against.
     * @return A cursor positioned on the first record not less than the given key, or an invalid cursor if no
     *         such record exists.
     */
    [[nodiscard]] auto lower_bound(BytesView key) const -> Cursor;

    /**
     * Find the first record that compares greater than the given key.
     *
     * @param key The key to compare against.
     * @return A cursor positioned on the first record greater than the given key, or an invalid cursor if no
     *         such record exists.
     */
    [[nodiscard]] auto upper_bound(BytesView key) const -> Cursor;

    /**
     * Search for the smallest key.
     *
     * @return A cursor positioned on the record with the smallest key, or std::nullopt if the database is empty.
     */
    [[nodiscard]] auto find_minimum() const -> Cursor;

    /**
     * Search for the largest key.
     *
     * @return The record with the largest key, or std::nullopt if the database is empty.
     */
    [[nodiscard]] auto find_maximum() const -> Cursor;

    /**
     * Insert a new record or update an existing one.
     *
     * @param key The key to write.
     * @param value The value to write.
     * @return True if the record was not already in the database, false otherwise.
     */
    auto insert(BytesView, BytesView) -> bool;

    /**
     * Insert a new record or update an existing one.
     *
     * @param record The record to write.
     * @return True if the record was not already in the database, false otherwise.
     */
    auto insert(const Record&) -> bool;

    /**
     * Erase a record given its key.
     *
     * @param key The key of the record to erase.
     * @return True if the record was found (and thus erased), false otherwise.
     */
    auto erase(BytesView key) -> bool;

    /**
     * Erase a record given a cursor pointing to it.
     *
     * @param cursor A cursor pointing to the record to erase.
     * @return True if the record was found (and thus erased), false otherwise.
     */
    auto erase(Cursor cursor) -> bool;

    /**
     * Commit the current transaction.
     *
     * @return True if there were changes to commit, false otherwise.
     */
    auto commit() -> bool;

    /**
     * Abort the current transaction.
     *
     * @return True if there were changes to abort, false otherwise.
     */
    auto abort() -> bool;

    /**
     * Open an object that can be used to get information about this database.
     *
     * @return A database information object.
     */
    [[nodiscard]] auto info() const -> Info;

    class Impl;
    virtual ~Database();
    Database(Database&&) noexcept;
    Database& operator=(Database&&) noexcept;

private:
    Database();
    std::unique_ptr<Impl> m_impl;
};

class Info {
public:
    virtual ~Info() = default;

    /**
     * Get the hit ratio for the buffer pool page cache.
     *
     * @return A page cache hit ratio in the range 0.0 to 1.0, inclusive.
     */
    [[nodiscard]] auto cache_hit_ratio() const -> double;

    /**
     * Get the number of records in the database.
     *
     * @return The number of records currently in the database.
     */
    [[nodiscard]] auto record_count() const -> Size;

    /**
     * Get the database size in pages.
     *
     * @return The database size in pages.
     */
    [[nodiscard]] auto page_count() const -> Size;

    /**
     * Get the database page size.
     *
     * @return The page size in bytes.
     */
    [[nodiscard]] auto page_size() const -> Size;

    /**
     * Get the maximum allowed key size.
     *
     * @return The maximal key length in characters.
     */
    [[nodiscard]] auto maximum_key_size() const -> Size;

    /**
     * Determine if the database uses transactions.
     *
     * @return True if the database uses transactions, false otherwise.
     */
    [[nodiscard]] auto uses_transactions() const -> bool;

    /**
     * Determine if the database exists only in-memory.
     *
     * @return True if the database is an in-memory database, false otherwise.
     */
    [[nodiscard]] auto is_temp() const -> bool;

private:
    friend class Database;
    Info() = default;
    Database::Impl *m_db {}; ///< Pointer to the database this object was opened on.
};

} // calico

#endif // CALICO_DATABASE_H
