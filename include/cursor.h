#ifndef CUB_CURSOR_H
#define CUB_CURSOR_H

#include <memory>
#include "common.h"
#include "page/node.h"
#include "bytes.h"
#include "utils/types.h"

namespace cub {

struct PID;

class Cursor {
public:
    virtual ~Cursor();
    [[nodiscard]] auto has_record() const -> bool;
    [[nodiscard]] auto key() const -> BytesView;
    [[nodiscard]] auto value() const -> std::string;
    auto reset() -> void;
    auto increment() -> bool;
    auto increment(Size) -> Size;
    auto decrement() -> bool;
    auto decrement(Size) -> Size;
    auto find(BytesView) -> bool;
    auto find_minimum() -> void;
    auto find_maximum() -> void;

    Cursor(Cursor&&) noexcept;
    Cursor &operator=(Cursor&&) noexcept;

private:
    friend class Database;
    Cursor();

    class Impl;
    std::unique_ptr<Impl> m_impl;
};

} // db

#endif // CUB_CURSOR_H
