#ifndef CALICO_CURSOR_H
#define CALICO_CURSOR_H

#include <memory>
#include <optional>
#include "status.h"

namespace Calico {

struct CursorActions;

class Cursor final {
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

private:
    friend class CursorInternal;

    struct Position {
        auto operator==(const Position &rhs) const -> bool;

        Size pid {};
        std::uint16_t index {};
        std::uint16_t count {};
    };

    mutable Status m_status {Status::ok()};
    mutable std::string m_buffer;
    CursorActions *m_actions {};
    Position m_loc;
};

} // namespace Calico

#endif // CALICO_CURSOR_H
