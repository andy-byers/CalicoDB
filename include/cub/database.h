#ifndef CUB_DATABASE_H
#define CUB_DATABASE_H

#include <memory>
#include <optional>
#include "bytes.h"

namespace cub {

class Info;
class Cursor;
class Batch;
class Lock;

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
     * @return An in-memory Cub DB database.
     */
    static auto temp(Size page_size) -> Database;

    /**
     * Read a record from the database.
     *
     * This method will search for a record with a key that is either less than, equal to, or greater than,
     * the given key, depending on the value of the second parameter.
     *
     * @param key The given key.
     * @param relation Relationship of the target record key to the given key.
     * @return The record with the desired relationship to the given key, or std::nullopt if that record
     *         does not exist.
     */
    [[nodiscard]] auto read(BytesView key, Comparison relation = Comparison::EQ) const -> std::optional<Record>;

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
     * Write a record to the database, or update an existing record.
     *
     * @param key The key to write.
     * @param value The value to write.
     * @return True if the record was not already in the database, false otherwise.
     */
    auto write(BytesView, BytesView) -> bool;
    auto erase(BytesView) -> bool;
    auto commit() -> void;
    auto abort() -> void;
    auto get_cursor() -> Cursor;
    auto get_lock() -> Lock;
    auto get_info() -> Info;

    [[nodiscard]] auto read(Lock&, BytesView key, Comparison relation = Comparison::EQ) const -> std::optional<Record>;
    [[nodiscard]] auto read_minimum(Lock&) const -> std::optional<Record>;
    [[nodiscard]] auto read_maximum(Lock&) const -> std::optional<Record>;
    auto write(Lock&, BytesView, BytesView) -> bool;
    auto erase(Lock&, BytesView) -> bool;
    auto commit(Lock&) -> void;
    auto abort(Lock&) -> void;

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
    explicit Info(Database::Impl*);
    virtual ~Info() = default;
    [[nodiscard]] auto cache_hit_ratio() const -> double;
    [[nodiscard]] auto record_count() const -> Size;
    [[nodiscard]] auto page_count() const -> Size;

private:
    Database::Impl *m_db;
};

} // cub

#endif // CUB_DATABASE_H
