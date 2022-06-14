#ifndef CUB_DB_READER_IMPL_H
#define CUB_DB_READER_IMPL_H

#include "cub/cursor.h"
#include "tree/iterator.h"
#include <optional>
#include <shared_mutex>
#include <vector>
#include "page/node.h"
#include "utils/types.h"

namespace cub {

class ITree;
struct PID;

class Cursor::Impl {
public:
    explicit Impl(ITree*, std::shared_mutex&);
    ~Impl() = default;
    [[nodiscard]] auto has_record() const -> bool;
    [[nodiscard]] auto is_minimum() const -> bool;
    [[nodiscard]] auto is_maximum() const -> bool;
    [[nodiscard]] auto key() const -> BytesView;
    [[nodiscard]] auto value() const -> std::string;
    auto reset() -> void;
    auto increment() -> bool;
    auto decrement() -> bool;
    auto find(BytesView) -> bool;
    auto find_minimum() -> void;
    auto find_maximum() -> void;

//    Impl(Impl&&) = default;
//    Impl &operator=(Impl&&) = default;

private:
    std::shared_lock<std::shared_mutex> m_lock;
    Iterator m_cursor;
};

} // cub

#endif // CUB_DB_READER_IMPL_H
