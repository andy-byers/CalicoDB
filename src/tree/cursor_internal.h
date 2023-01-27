#ifndef CALICO_TREE_CURSOR_INTERNAL_H
#define CALICO_TREE_CURSOR_INTERNAL_H

#include <calico/cursor.h>
#include <functional>
#include "utils/expected.hpp"
#include <utils/types.h>

namespace Calico {

class Node__;

inline auto default_error_status() -> Status
{
    return Status::not_found("cursor is invalid");
}

struct CursorActions {
    using CollectCallback = std::function<tl::expected<std::string, Status>(const Node__ &, std::uint16_t)>;
    using AcquireCallback = std::function<tl::expected<Node__, Status>(Id, bool)>;
    using ReleaseCallback = std::function<tl::expected<void, Status>(Node__)>;

    CollectCallback collect;
    AcquireCallback acquire;
    ReleaseCallback release;
};

class CursorInternal {
public:
    [[nodiscard]] static auto make_cursor(CursorActions *actions) -> Cursor;
    [[nodiscard]] static auto id(const Cursor &) -> Size;
    [[nodiscard]] static auto index(const Cursor &) -> Size;
    [[nodiscard]] static auto seek_left(Cursor &) -> bool;
    [[nodiscard]] static auto seek_right(Cursor &) -> bool;
    [[nodiscard]] static auto is_last(const Cursor &) -> bool;
    [[nodiscard]] static auto is_first(const Cursor &) -> bool;
    static auto move_to(Cursor &, Node__, Size) -> void;
    static auto invalidate(Cursor &, const Status & = default_error_status()) -> void;

    static auto TEST_validate(const Cursor &) -> void;
};

} // namespace Calico

#endif // CALICO_TREE_CURSOR_INTERNAL_H
