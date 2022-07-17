#ifndef CCO_DATABASE_H
#define CCO_DATABASE_H

#include "status.h"
#include <memory>
#include <optional>

namespace cco {

class Cursor;
class Info;

/**
 * An object that represents a Calico DB database.
 */
class Database {
public:
    explicit Database(const Options&);
    [[nodiscard]] static auto destroy(Database db) -> Status;
    [[nodiscard]] auto close() -> Status;
    [[nodiscard]] auto open() -> Status;

    [[nodiscard]] auto is_open() const -> bool;

    /**
     * Find the record with a given key.
     *
     * @param key The key to search for.
     * @return A valid cursor positioned on the record with the given key, or an invalid cursor otherwise.
     */
    [[nodiscard]] auto find_exact(BytesView key) const -> Cursor;
    [[nodiscard]] auto find_exact(const std::string &key) const -> Cursor;

    /**
     * Find the first record that is greater than or equal to the given key.
     *
     * @param key The key to compare against.
     * @return A cursor positioned on the first record greater than or equal to than the given key, or an
     *         invalid cursor if no such record exists.
     */
    [[nodiscard]] auto find(BytesView key) const -> Cursor;
    [[nodiscard]] auto find(const std::string &key) const -> Cursor;

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
    auto insert(BytesView key, BytesView value) -> Status;
    auto insert(const std::string &key, const std::string &value) -> Status;
    auto insert(const Record&) -> Status;

    /**
     * Erase a record given its key.
     *
     * @param key The key of the record to erase.
     * @return True if the record was found (and thus erased), false otherwise.
     */
    auto erase(BytesView key) -> Status;
    auto erase(const std::string &key) -> Status;
    auto erase(const Cursor &cursor) -> Status;

    [[nodiscard]] auto status() const -> Status;
    [[nodiscard]] auto abort() -> Status;
    [[nodiscard]] auto commit() -> Status;

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

    Options m_options;
    std::unique_ptr<Impl> m_impl;
};

class Info {
public:
    virtual ~Info() = default;

    /**
     * Get the hit ratio for the buffer pool page cache.
     *
     * @return A page cache hit ratio in the view 0.0 to 1.0, inclusive.
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

} // cco

#endif // CCO_DATABASE_H
