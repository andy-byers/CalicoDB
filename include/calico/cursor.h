#ifndef CALICO_CURSOR_H
#define CALICO_CURSOR_H

#include <memory>
#include <optional>
#include "status.h"

namespace Calico {

class Node;
struct CursorActions;

class Cursor final {
public:
    ~Cursor() = default;
    [[nodiscard]] auto is_valid() const -> bool;
    [[nodiscard]] auto status() const -> Status;
    [[nodiscard]] auto key() const -> Slice;
    [[nodiscard]] auto value() const -> std::string;
    auto increment() -> bool;
    auto decrement() -> bool;
    auto operator++() -> Cursor &;
    auto operator++(int) -> Cursor;
    auto operator--() -> Cursor &;
    auto operator--(int) -> Cursor;
    auto operator==(const Cursor &rhs) const -> bool;
    auto operator!=(const Cursor &rhs) const -> bool;

private:
    Cursor() = default;

    struct Position {
        static constexpr Size LEFT {0};
        static constexpr Size CURRENT {1};
        static constexpr Size RIGHT {2};

        auto operator==(const Position &rhs) const -> bool;
        [[nodiscard]] auto is_minimum() const -> bool;
        [[nodiscard]] auto is_maximum() const -> bool;

        Size ids[3] {0, 1, 0};
        std::uint16_t cell_count {};
        std::uint16_t index {};
    };

    friend class CursorInternal;

    mutable Status m_status {Status::ok()};
    CursorActions *m_actions {};
    Position m_position;
};

} // namespace Calico

#endif // CALICO_CURSOR_H
