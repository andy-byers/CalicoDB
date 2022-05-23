
#ifndef CUB_TREE_TREE_H
#define CUB_TREE_TREE_H

#include "free_list.h"
#include "interface.h"
#include "utils/scratch.h"

namespace cub {

class Cell;
class FreeList;
class IBufferPool;
class Node;

/**
 *
 */
class Tree: public ITree {
public:
    struct Parameters {
        IBufferPool *buffer_pool{};
        PID free_start{};
        Size free_count{};
        Size cell_count{};
        Size node_count{};
    };
    
    explicit Tree(Parameters);
    ~Tree() override = default;
    [[nodiscard]] auto node_count() const -> Size override {return m_node_count;}
    [[nodiscard]] auto cell_count() const -> Size override {return m_cell_count;}
    [[nodiscard]] auto collect_value(const Node&, Index) const -> std::string override;
    auto find_root(bool) -> Node override;
    auto find_ge(BytesView, bool) -> Result override;
    auto find_local_min(Node) -> Position override;
    auto find_local_max(Node) -> Position override;
    auto save_header(FileHeader&) -> void override;
    auto lookup(BytesView, std::string&) -> bool override;
    auto insert(BytesView, BytesView) -> void override;
    auto remove(BytesView) -> bool override;
    auto allocate_node(PageType) -> Node override;
    auto acquire_node(PID, bool) -> Node override;
    auto destroy_node(Node) -> void override;
    auto make_cell(Node&, BytesView, BytesView) -> Cell;

protected: // TODO
    auto on_cursor_close(bool) -> void;

    auto positioned_insert(Position, BytesView, BytesView) -> void;
    auto positioned_modify(Position, BytesView) -> void;
    auto positioned_remove(Position) -> void;
    auto do_insert(Node&, Index, Cell) -> void;
    auto do_remove(Node, Index) -> void;

    // Overflow chains.
    auto allocate_overflow_chain(BytesView) -> PID;
    auto destroy_overflow_chain(PID, Size) -> void;
    auto collect_overflow_chain(PID, Bytes) const -> void;

    // Overflow handling.
    auto balance_after_overflow(Node) -> void;
    auto split_non_root(Node) -> Node;
    auto split_root(Node) -> Node;

    // Underflow handling.
    auto balance_after_underflow(Node, BytesView) -> void;
    auto merge_left(Node&, Node&, Node, Index) -> void;
    auto merge_right(Node&, Node&, Node, Index) -> void;
    auto rotate_left(Node&, Node&, Node&, Index) -> void;
    auto rotate_right(Node&, Node&, Node&, Index) -> void;
    auto fix_non_root(Node, Node&, Index) -> void;
    auto fix_root(Node) -> void;

    // Helpers.
    auto do_split_non_root(Node&, Node&, Scratch) -> Cell;
    auto do_split_root(Node&, Node&) -> void;
    auto do_merge_root(Node&, Node) -> void;
    auto maybe_fix_child_parent_connections(Node &node) -> void;
    auto transfer_cell(Node&, Node&, Index) -> void;
    auto can_merge_siblings(const Node&, const Node&, const Node&, Index) -> bool;

    ScratchManager m_scratch;
    FreeList m_free_list;
    IBufferPool *m_pool{};
    Size m_node_count{};
    Size m_cell_count{};
};

} // db

#endif // CUB_TREE_TREE_H
