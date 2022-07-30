#ifndef CCO_DATABASE_H
#define CCO_DATABASE_H

#include "status.h"
#include <memory>

namespace cco {

class Cursor;
class Batch;
class Info;

/**
 * A CalicoDB database!
 */
class Database {
public:
    /**
     * Create a database object.
     *
     * Note that a newly constructed database will not be open.
     *
     * @see open()
     * @param options Initialization options for the database.
     */
    explicit Database(Options options) noexcept;

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
    [[nodiscard]] auto insert(BytesView key, BytesView value) -> Status;
    [[nodiscard]] auto insert(const std::string &key, const std::string &value) -> Status;
    [[nodiscard]] auto insert(const Record&) -> Status;

    /**
     * Erase a record given its key.
     *
     * @param key The key of the record to erase.
     * @return True if the record was found (and thus erased), false otherwise.
     */
    [[nodiscard]] auto erase(BytesView key) -> Status;
    [[nodiscard]] auto erase(const std::string &key) -> Status;
    [[nodiscard]] auto erase(const Cursor &cursor) -> Status;

    /**
     * Open an object that can be used to get information about this database.
     *
     * @return A database information object.
     */
    [[nodiscard]] auto info() const -> Info;
    [[nodiscard]] auto status() const -> Status;
    [[nodiscard]] auto commit() -> Status;
    [[nodiscard]] auto abort() -> Status;

    class Impl;
    virtual ~Database();
    Database(Database&&) noexcept;
    Database& operator=(Database&&) noexcept;

private:
    Database();

    Options m_options;
    std::unique_ptr<Impl> m_impl;
};

} // cco

#endif // CCO_DATABASE_H
