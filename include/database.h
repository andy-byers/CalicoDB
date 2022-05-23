#ifndef CUB_DATABASE_H
#define CUB_DATABASE_H

#include <memory>
#include "bytes.h"
#include "common.h"

namespace cub {

class ITree;
class IBufferPool;
class Tree;
class BufferPool;
class Cursor;
class Info;

class Database {
public:
    class Impl;

    static auto open(const std::string&, const Options&) -> Database;
    virtual ~Database();
    auto lookup(BytesView, std::string&) -> bool; // TODO: Make const
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

} // db

#endif // CUB_DATABASE_H
