#ifndef CALICO_DB_READER_IMPL_H
#define CALICO_DB_READER_IMPL_H

#include "calico/cursor.h"
#include <optional>
#include <shared_mutex>
#include <vector>
#include "page/node.h"
#include "utils/types.h"

namespace calico {

class ITree;
struct PID;

class Cursor::Impl {
public:
    Impl(ITree*);
    ~Impl() = default;
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

    //    Impl(Impl&&) = default;
    //    Impl &operator=(Impl&&) = default;

private:
    [[nodiscard]] auto can_decrement() const -> bool;
    [[nodiscard]] auto can_increment() const -> bool;
    [[nodiscard]] auto is_end_of_tree() const -> bool;
    [[nodiscard]] auto is_end_of_node() const -> bool;
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

    [[nodiscard]] auto has_node() const -> bool
    {
        return m_node != std::nullopt;
    }

    Unique<ITree*> m_tree;          ///< Tree that the cursor belongs to
    std::vector<Index> m_traversal; ///< Cell IDs encountered on the current traversal
    std::optional<Node> m_node;     ///< Node that the cursor is over
    Index m_index {};               ///< Position in the current node
};

} // calico

#endif // CALICO_DB_READER_IMPL_H