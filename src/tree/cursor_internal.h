#ifndef CALICO_TREE_CURSOR_INTERNAL_H
#define CALICO_TREE_CURSOR_INTERNAL_H

#include "node.h"
#include "tree.h"
#include "utils/expected.hpp"
#include <calico/cursor.h>
#include <functional>
#include <utils/types.h>

namespace Calico {

class CursorInternal {
    friend class Cursor;

    [[nodiscard]] static auto action_collect(const Cursor &cursor, Node node, Size index) -> tl::expected<std::string, Status>;
    [[nodiscard]] static auto action_acquire(const Cursor &cursor, Id pid) -> tl::expected<Node, Status>;
    [[nodiscard]] static auto action_search(const Cursor &cursor, const Slice &key) -> tl::expected<SearchResult, Status>;
    [[nodiscard]] static auto action_lowest(const Cursor &cursor) -> tl::expected<Node, Status>;
    [[nodiscard]] static auto action_highest(const Cursor &cursor) -> tl::expected<Node, Status>;
    static auto action_release(const Cursor &cursor, Node node) -> void;

    static auto seek_left(Cursor &cursor) -> void;
    static auto seek_right(Cursor &cursor) -> void;
    static auto seek_first(Cursor &cursor) -> void;
    static auto seek_last(Cursor &cursor) -> void;
    static auto seek_to(Cursor &cursor, Node node, Size index) -> void;
    static auto seek(Cursor &cursor, const Slice &key) -> void;

public:
    [[nodiscard]] static auto make_cursor(BPlusTree &tree) -> Cursor;
    static auto invalidate(const Cursor &cursor, const Status &error) -> void;

    static auto TEST_validate(const Cursor &cursor) -> void;
};

} // namespace Calico

#endif // CALICO_TREE_CURSOR_INTERNAL_H
