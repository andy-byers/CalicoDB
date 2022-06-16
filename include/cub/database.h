#ifndef CUB_DATABASE_H
#define CUB_DATABASE_H

#include <memory>
#include <optional>
#include "bytes.h"

namespace cub {

class Info;
class Cursor;

/**
 * Represents a relationship between two keys.
 *
 * @see Database::read()
 */
enum class Ordering {
    LT, ///< Less than
    LE, ///< Less than or equal to
    EQ, ///< Equal to
    GE, ///< Greater than or equal to
    GT, ///< Greater than
};

/**
 * An object that represents a Cub DB database.
 */
class Database {
public:

    /**
     * Open or create a Cub DB database.
     *
     * @param path The path to the database file.
     * @param options The options to apply to the database.
     * @return A Cub DB database, located at the provided path on disk.
     */
    static auto open(const std::string &path, const Options &options) -> Database;

    /**
     * Create an in-memory Cub DB database.
     *
     * @param page_size Size of a page of memory in bytes (must be a power of two).
     * @param use_transactions True if the database should use transactions, false otherwise.
     * @return An in-memory Cub DB database.
     */
    static auto temp(Size page_size, bool use_transactions = true) -> Database;

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
     * Read a record from the database.
     *
     * This method will search for the first record with a key with the given relationship to the provided
     * key. For example, if Ordering::GT is used, we look for the first key greater than the given key.
     *
     * @param key The key to search for.
     * @param ordering Relationship between the target record key and the given key.
     * @return The record with the desired relationship to the given key, or std::nullopt if that record
     *         does not exist.
     */
    [[nodiscard]] auto read(BytesView key, Ordering ordering = Ordering::EQ) const -> std::optional<Record>;

    /**
     * Read the record with the smallest key.
     *
     * @return The record with the smallest key, or std::nullopt if the database is empty.
     */
    [[nodiscard]] auto read_minimum() const -> std::optional<Record>;

    /**
     * Read the record with the largest key.
     *
     * @return The record with the largest key, or std::nullopt if the database is empty.
     */
    [[nodiscard]] auto read_maximum() const -> std::optional<Record>;

    /**
     * Write a new record, or update an existing one.
     *
     * @param key The key to write.
     * @param value The value to write.
     * @return True if the record was not already in the database, false otherwise.
     */
    auto write(BytesView, BytesView) -> bool;

    /**
     * Write a new record, or update an existing one.
     *
     * @param record The record to write.
     * @return True if the record was not already in the database, false otherwise.
     */
    auto write(const Record&) -> bool;

    /**
     * Erase a record.
     *
     * @param key The key of the record to erase.
     * @return True if the record was found (and thus erased), false otherwise.
     */
    auto erase(BytesView) -> bool;

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
     * Open a cursor.
     *
     * @return An open cursor.
     */
    [[nodiscard]] auto get_cursor() const -> Cursor;

    /**
     * Open an object that can be used to get information about this database.
     *
     * @return A database information object.
     */
    [[nodiscard]] auto get_info() const -> Info;

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

private:
    friend class Database;
    Info() = default;
    Database::Impl *m_db {}; ///< Pointer to the database this object was opened on.
};

} // cub

#endif // CUB_DATABASE_H
