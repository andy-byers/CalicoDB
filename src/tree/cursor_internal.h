#ifndef CALICO_TREE_CURSOR_INTERNAL_H
#define CALICO_TREE_CURSOR_INTERNAL_H

#include "calico/cursor.h"

namespace calico {

class Node;
class NodePool;
class Internal;

inline auto default_error_status() -> Status
{
    return Status::not_found("cursor is invalid");
}

class CursorInternal {
public:
    [[nodiscard]] static auto make_cursor(NodePool &, Internal &) -> Cursor;
    [[nodiscard]] static auto id(const Cursor &) -> Size;
    [[nodiscard]] static auto index(const Cursor &) -> Size;
    [[nodiscard]] static auto seek_left(Cursor &) -> bool;
    [[nodiscard]] static auto seek_right(Cursor &) -> bool;
    static auto move_to(Cursor &, Node, Size) -> void;
    static auto invalidate(Cursor &, const Status & = default_error_status()) -> void;

    static auto TEST_validate(const Cursor &) -> void;
};

} // namespace calico

#endif // CALICO_TREE_CURSOR_INTERNAL_H
