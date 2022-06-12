#ifndef CUB_CURSOR_H
#define CUB_CURSOR_H

#include <memory>
#include "bytes.h"

namespace cub {

class Cursor {
public:
    virtual ~Cursor();
    [[nodiscard]] auto has_record() const -> bool;
    [[nodiscard]] auto is_minimum() const -> bool;
    [[nodiscard]] auto is_maximum() const -> bool;
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

    Cursor(Cursor &&) noexcept;
    Cursor &operator=(Cursor &&) noexcept;

private:
    friend class Database;
    Cursor();

    class Impl;
    std::unique_ptr<Impl> m_impl;
};

} // cub

#endif // CUB_CURSOR_H
