#ifndef CALICO_CURSOR_H
#define CALICO_CURSOR_H

#include <memory>
#include <optional>
#include "status.h"

namespace Calico {

struct Node;
struct CursorActions;

class Cursor final {
    friend class CursorInternal;

    struct Position {
        static constexpr Size LEFT {0};
        static constexpr Size CENTER {1};
        static constexpr Size RIGHT {2};

        auto operator==(const Position &rhs) const -> bool;
        [[nodiscard]] auto is_minimum() const -> bool;
        [[nodiscard]] auto is_maximum() const -> bool;

        Size ids[3] {0, 1, 0};
        std::uint16_t cell_count {};
        std::uint16_t index {};
    };

    mutable Status m_status {Status::ok()};
    mutable std::string m_value;
    CursorActions *m_actions {};
    Position m_position;

public:
    Cursor() = default;

    [[nodiscard]] auto is_valid() const -> bool;
    [[nodiscard]] auto status() const -> Status;
    [[nodiscard]] auto key() const -> Slice;
    [[nodiscard]] auto value() const -> Slice;

    auto seek(const Slice &key) -> void;
    auto seek_first() -> void;
    auto seek_last() -> void;
    auto next() -> void;
    auto previous() -> void;

    /*
     * Position-based comparison between cursors can be faster than comparing keys.
     */
    auto operator==(const Cursor &rhs) const -> bool;
    auto operator!=(const Cursor &rhs) const -> bool;
};

} // namespace Calico

#endif // CALICO_CURSOR_H
