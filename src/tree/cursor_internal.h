#ifndef CALICO_TREE_CURSOR_INTERNAL_H
#define CALICO_TREE_CURSOR_INTERNAL_H

#include <calico/cursor.h>
#include <functional>
#include "utils/expected.hpp"
#include <utils/types.h>

namespace Calico {

class BPlusTree;
class Node;

struct CursorActions {
    using Collect = tl::expected<std::string, Status> (*)(BPlusTree &, Node, Size);
    using Acquire = tl::expected<Node, Status> (*)(BPlusTree &, Id, bool);
    using Release = void (*)(BPlusTree &, Node);

    [[nodiscard]] auto collect(Node node, Size index) -> tl::expected<std::string, Status>;
    [[nodiscard]] auto acquire(Id pid, bool upgrade) -> tl::expected<Node, Status>;
    auto release(Node node) -> void;

    BPlusTree *tree_ptr {};
    Collect collect_ptr;
    Acquire acquire_ptr;
    Release release_ptr;
};

class CursorInternal {
    friend class Cursor;

    [[nodiscard]] static auto id(const Cursor &) -> Size;
    [[nodiscard]] static auto index(const Cursor &) -> Size;
    [[nodiscard]] static auto is_last(const Cursor &) -> bool;
    [[nodiscard]] static auto is_first(const Cursor &) -> bool;
    static auto seek_left(Cursor &) -> bool;
    static auto seek_right(Cursor &) -> bool;
    static auto move_to(Cursor &, Node, Size) -> void;

public:
    [[nodiscard]] static auto make_cursor(BPlusTree &tree) -> Cursor;
    [[nodiscard]] static auto find(BPlusTree &tree, const Slice &key) -> Cursor;
    [[nodiscard]] static auto find_first(BPlusTree &tree) -> Cursor;
    [[nodiscard]] static auto find_last(BPlusTree &tree) -> Cursor;
    static auto invalidate(Cursor &, const Status &) -> void;

    static auto TEST_validate(const Cursor &) -> void;
};

} // namespace Calico

#endif // CALICO_TREE_CURSOR_INTERNAL_H
