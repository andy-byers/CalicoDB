#ifndef CCO_TREE_CURSOR_INTERNAL_H
#define CCO_TREE_CURSOR_INTERNAL_H

#include "calico/cursor.h"

namespace cco {

class Node;
class NodePool;
class Internal;

class CursorInternal {
public:
    [[nodiscard]] static auto make_cursor(NodePool&, Internal&) -> Cursor;
    [[nodiscard]] static auto id(const Cursor&) -> Index;
    [[nodiscard]] static auto index(const Cursor&) -> Index;
    [[nodiscard]] static auto seek_left(Cursor&) -> bool;
    [[nodiscard]] static auto seek_right(Cursor&) -> bool;
    static auto move_to(Cursor&, Node, Index) -> void;
    static auto invalidate(Cursor&, const Status& = Status::not_found()) -> void;

    static auto TEST_validate(const Cursor&) -> void;
};

} // cco

#endif // CCO_TREE_CURSOR_INTERNAL_H
