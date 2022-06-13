#ifndef CUB_DATABASE_H
#define CUB_DATABASE_H

#include <memory>
#include <optional>
#include "bytes.h"

namespace cub {

class Info;
class Cursor;
class Batch;

class Database {
public:
    class Impl;

    static auto open(const std::string&, const Options&) -> Database;
    static auto temp(Size) -> Database;
    virtual ~Database();
    [[nodiscard]] auto read(BytesView, bool) const -> std::optional<Record>;
    [[nodiscard]] auto read_minimum() const -> std::optional<Record>;
    [[nodiscard]] auto read_maximum() const -> std::optional<Record>;
    auto write(BytesView, BytesView) -> bool;
    auto erase(BytesView) -> bool;
    auto commit() -> void;
    auto abort() -> void;
    auto get_cursor() -> Cursor;
    auto get_batch() -> Batch;
    auto get_info() -> Info;

    // TODO: noexcept?
    Database(Database&&) noexcept;
    Database& operator=(Database&&) noexcept;

private:
    Database();
    std::unique_ptr<Impl> m_impl;
};

class Info {
public:
    explicit Info(Database::Impl*);
    [[nodiscard]] auto cache_hit_ratio() const -> double;
    [[nodiscard]] auto record_count() const -> Size;
    [[nodiscard]] auto page_count() const -> Size;

private:
    Database::Impl *m_db;
};

} // cub

#endif // CUB_DATABASE_H
