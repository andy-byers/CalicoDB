#ifndef CUB_TREE_ITERATOR_H
#define CUB_TREE_ITERATOR_H

#include <optional>
#include <shared_mutex>
#include <vector>
#include "page/node.h"
#include "utils/types.h"

namespace cub {

class ITree;
struct PID;

class Iterator {
public:
    explicit Iterator(ITree*);
    ~Iterator() = default;
    [[nodiscard]] auto has_record() const -> bool;
    [[nodiscard]] auto is_minimum() const -> bool;
    [[nodiscard]] auto is_maximum() const -> bool;
    [[nodiscard]] auto key() const -> BytesView;
    [[nodiscard]] auto value() const -> std::string;
    auto reset() -> void;
    auto increment() -> bool;
    auto decrement() -> bool;
    auto find(BytesView) -> bool;
    auto find_minimum() -> void;
    auto find_maximum() -> void;

    Iterator(Iterator &&) = default;
    Iterator &operator=(Iterator &&) = default;

private:
    [[nodiscard]] auto can_decrement() const -> bool;
    [[nodiscard]] auto can_increment() const -> bool;
    [[nodiscard]] auto is_end_of_tree() const -> bool;
    [[nodiscard]] auto is_end_of_node() const -> bool;
    [[nodiscard]] auto has_node() const -> bool {return m_node != std::nullopt;}
    auto find_aux(BytesView key) -> bool;
    auto increment_external() -> void;
    auto increment_internal() -> void;
    auto decrement_internal() -> void;
    auto decrement_external() -> void;
    auto goto_inorder_successor() -> void;
    auto goto_inorder_predecessor() -> void;
    auto goto_child(Index) -> void;
    auto goto_parent() -> void;
    auto find_local_min() -> void;
    auto find_local_max() -> void;
    auto move_cursor(PID) -> void;

    Unique<ITree*> m_source;        ///< Tree that the cursor belongs to
    std::vector<Index> m_traversal; ///< Cell IDs encountered on the current traversal
    std::optional<Node> m_node;     ///< Node that the cursor is over
    Index m_index {};               ///< Position in the current node
};

} // cub

#endif // CUB_TREE_ITERATOR_H
