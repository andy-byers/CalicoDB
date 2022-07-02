#ifndef CALICO_TREE_INTERNAL_H
#define CALICO_TREE_INTERNAL_H

#include <spdlog/spdlog.h>
#include "node_pool.h"
#include "interface.h"
#include "utils/scratch.h"

namespace calico {

class Internal final {
public:
    struct Position {
        Node node;
        Index index {};
    };

    struct Result {
        Node node;
        Index index {};
        bool flag {};
    };

    struct Parameters {
        NodePool *pool {};
        Size cell_count {};
    };

    explicit Internal(Parameters);
    ~Internal() = default;
    [[nodiscard]] auto collect_value(const Node&, Index) const -> std::string;
    auto find_root(bool) -> Node;
    auto find_external(BytesView, bool) -> Result;
    auto find_local_min(Node) -> Position;
    auto find_local_max(Node) -> Position;
    auto make_cell(BytesView, BytesView, bool) -> Cell;
    auto positioned_insert(Position, BytesView, BytesView) -> void;
    auto positioned_modify(Position, BytesView) -> void;
    auto positioned_remove(Position) -> void;
    auto save_header(FileHeader&) const -> void;
    auto load_header(const FileHeader&) -> void;

    [[nodiscard]] auto cell_count() const -> Size
    {
        return m_cell_count;
    }

private:
    auto balance_after_overflow(Node) -> void;
    auto split_non_root(Node) -> Node;
    auto split_root(Node) -> Node;

    auto balance_after_underflow(Node, BytesView) -> void;
    auto fix_non_root(Node, Node&, Index) -> bool;
    auto fix_root(Node) -> void;
    auto rotate_left(Node&, Node&, Node&, Index) -> void;
    auto rotate_right(Node&, Node&, Node&, Index) -> void;
    auto external_rotate_left(Node&, Node&, Node&, Index) -> void;
    auto external_rotate_right(Node&, Node&, Node&, Index) -> void;
    auto internal_rotate_left(Node&, Node&, Node&, Index) -> void;
    auto internal_rotate_right(Node&, Node&, Node&, Index) -> void;

    auto maybe_fix_child_parent_connections(Node &node) -> void;

    ScratchManager m_scratch;
    NodePool *m_pool;
    Size m_cell_count {};
};

} // calico

#endif // CALICO_TREE_INTERNAL_H
