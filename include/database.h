#ifndef CUB_DATABASE_H
#define CUB_DATABASE_H

#include <memory>
#include <optional>
#include "bytes.h"
#include "common.h"

namespace cub {

class Cursor;
class Info;

class Database {
public:
    class Impl;

    static auto open(const std::string&, const Options&) -> Database;
    virtual ~Database();
    auto lookup(BytesView, bool) -> std::optional<std::string>; // TODO: Make const
    auto lookup_minimum() -> std::optional<std::string>;
    auto lookup_maximum() -> std::optional<std::string>;
    auto insert(BytesView, BytesView) -> void;
    auto remove(BytesView) -> bool;
    auto commit() -> void;
    auto abort() -> void;
    auto get_cursor() -> Cursor;
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
    auto cache_hit_ratio() const -> double;
    auto record_count() const -> Size;
    auto page_count() const -> Size;
    auto transaction_size() const -> Size;

private:
    Database::Impl *m_db;
};

} // cub

#endif // CUB_DATABASE_H
