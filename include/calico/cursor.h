#ifndef CALICO_CURSOR_H
#define CALICO_CURSOR_H

#include "status.h"
#include <memory>
#include <optional>

namespace Calico {

class Node;
class NodeManager; // TODO: remove
class Internal; // TODO: remove
struct CursorActions;

class Cursor final {
public:
    ~Cursor() = default;
    [[nodiscard]] auto is_valid() const -> bool;
    [[nodiscard]] auto status() const -> Status;
    [[nodiscard]] auto key() const -> BytesView;
    [[nodiscard]] auto value() const -> std::string;
    auto increment() -> bool;
    auto decrement() -> bool;
    auto operator++() -> Cursor&;
    auto operator++(int) -> Cursor;
    auto operator--() -> Cursor&;
    auto operator--(int) -> Cursor;
    auto operator==(const Cursor&) const -> bool;
    auto operator!=(const Cursor&) const -> bool;

private:
    Cursor() = default;

    struct Position {
        static constexpr Size LEFT {0};
        static constexpr Size CURRENT {1};
        static constexpr Size RIGHT {2};

        auto operator==(const Position &rhs) const -> bool;
        [[nodiscard]] auto is_minimum() const -> bool;
        [[nodiscard]] auto is_maximum() const -> bool;

        std::uint64_t ids[3] {0, 1, 0};
        std::uint16_t cell_count {};
        std::uint16_t index {};
    };

    friend class CursorInternal;

    mutable Status m_status {Status::not_found("not found")};
    CursorActions *m_actions {};
    Position m_position;
};

} // namespace Calico

#endif // CALICO_CURSOR_H
