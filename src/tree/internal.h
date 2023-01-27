#ifndef CALICO_TREE_INTERNAL_H
#define CALICO_TREE_INTERNAL_H

#include "node_manager.h"
#include "page/node.h"
#include "tree.h"
#include "utils/scratch.h"
#include <spdlog/spdlog.h>

namespace Calico {

class System;

class Internal final {
public:
    struct Position {
        Node__ node;
        Size index {};
    };

    struct SearchResult {
        Id id;
        Size index {};
        bool was_found {};
    };

    struct Parameters {
        NodeManager *pool {};
        Size cell_count {};
    };

    explicit Internal(NodeManager &pool);

    ~Internal() = default;
    [[nodiscard]] auto collect_value(const Node__ &, Size) const -> tl::expected<std::string, Status>;
    [[nodiscard]] auto find_external(const Slice &) -> tl::expected<SearchResult, Status>;
    [[nodiscard]] auto find_minimum() -> tl::expected<SearchResult, Status>;
    [[nodiscard]] auto find_maximum() -> tl::expected<SearchResult, Status>;
    [[nodiscard]] auto find_root(bool) -> tl::expected<Node__, Status>;
    [[nodiscard]] auto make_cell(const Slice &, const Slice &, bool) -> tl::expected<Cell__, Status>;
    [[nodiscard]] auto positioned_insert(Position, const Slice &, const Slice &) -> tl::expected<void, Status>;
    [[nodiscard]] auto positioned_modify(Position, const Slice &) -> tl::expected<void, Status>;
    [[nodiscard]] auto positioned_remove(Position) -> tl::expected<void, Status>;
    auto save_state(FileHeader__ &header) const -> void;
    auto load_state(const FileHeader__ &header) -> void;

    [[nodiscard]] auto cell_count() const -> Size
    {
        return m_cell_count;
    }

    [[nodiscard]] auto maximum_key_size() const -> Size
    {
        return m_maximum_key_size;
    }

private:
    [[nodiscard]] auto balance_after_overflow(Node__) -> tl::expected<void, Status>;
    [[nodiscard]] auto split_non_root(Node__) -> tl::expected<Node__, Status>;
    [[nodiscard]] auto split_root(Node__) -> tl::expected<Node__, Status>;

    [[nodiscard]] auto balance_after_underflow(Node__, const Slice &) -> tl::expected<void, Status>;
    [[nodiscard]] auto fix_non_root(Node__, Node__ &, Size) -> tl::expected<bool, Status>;
    [[nodiscard]] auto fix_root(Node__) -> tl::expected<void, Status>;
    [[nodiscard]] auto rotate_left(Node__ &, Node__ &, Node__ &, Size) -> tl::expected<void, Status>;
    [[nodiscard]] auto rotate_right(Node__ &, Node__ &, Node__ &, Size) -> tl::expected<void, Status>;
    [[nodiscard]] auto external_rotate_left(Node__ &, Node__ &, Node__ &, Size) -> tl::expected<void, Status>;
    [[nodiscard]] auto external_rotate_right(Node__ &, Node__ &, Node__ &, Size) -> tl::expected<void, Status>;
    [[nodiscard]] auto internal_rotate_left(Node__ &, Node__ &, Node__ &, Size) -> tl::expected<void, Status>;
    [[nodiscard]] auto internal_rotate_right(Node__ &, Node__ &, Node__ &, Size) -> tl::expected<void, Status>;

    [[nodiscard]] auto maybe_fix_child_parent_connections(Node__ &) -> tl::expected<void, Status>;

    Size m_maximum_key_size {};
    std::array<StaticScratch, 3> m_scratch;
    NodeManager *m_pool {};
    Size m_cell_count {};
};

} // namespace Calico

#endif // CALICO_TREE_INTERNAL_H
