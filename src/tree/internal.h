#ifndef CCO_TREE_INTERNAL_H
#define CCO_TREE_INTERNAL_H

#include "interface.h"
#include "node_pool.h"
#include "page/node.h"
#include "utils/scratch.h"
#include <spdlog/spdlog.h>

namespace cco {

class Internal final {
public:
    struct Position {
        Node node;
        Index index {};
    };

    struct SearchResult {
        PageId id;
        Index index {};
        bool was_found {};
    };

    struct Parameters {
        NodePool *pool {};
        Size cell_count {};
    };

    explicit Internal(Parameters);
    ~Internal() = default;
    [[nodiscard]] auto collect_value(const Node &, Index) const -> Result<std::string>;
    [[nodiscard]] auto find_external(BytesView) -> Result<SearchResult>;
    [[nodiscard]] auto find_minimum() -> Result<SearchResult>;
    [[nodiscard]] auto find_maximum() -> Result<SearchResult>;
    [[nodiscard]] auto find_root(bool) -> Result<Node>;
    [[nodiscard]] auto make_cell(BytesView, BytesView, bool) -> Result<Cell>;
    [[nodiscard]] auto positioned_insert(Position, BytesView, BytesView) -> Result<void>;
    [[nodiscard]] auto positioned_modify(Position, BytesView) -> Result<void>;
    [[nodiscard]] auto positioned_remove(Position) -> Result<void>;
    auto save_header(FileHeaderWriter &) const -> void;
    auto load_header(const FileHeaderReader &) -> void;

    [[nodiscard]] auto cell_count() const -> Size
    {
        return m_cell_count;
    }

    [[nodiscard]] auto maximum_key_size() const -> Size
    {
        return m_maximum_key_size;
    }

private:
    // TODO: This implementation will need to handle arbitrary failures during the splits and merges.
    //       The only way I can think to do this is transactionally using the WAL. We'll have to save
    //       some state that indicates that we need recovery and refuse to continue modifying/reading
    //       the database if that variable is set. We could also try to roll back automatically and
    //       somehow indicate that this has happened.
    [[nodiscard]] auto balance_after_overflow(Node) -> Result<void>;
    [[nodiscard]] auto split_non_root(Node) -> Result<Node>;
    [[nodiscard]] auto split_root(Node) -> Result<Node>;

    [[nodiscard]] auto balance_after_underflow(Node, BytesView) -> Result<void>;
    [[nodiscard]] auto fix_non_root(Node, Node &, Index) -> Result<bool>;
    [[nodiscard]] auto fix_root(Node) -> Result<void>;
    [[nodiscard]] auto rotate_left(Node &, Node &, Node &, Index) -> Result<void>;
    [[nodiscard]] auto rotate_right(Node &, Node &, Node &, Index) -> Result<void>;
    [[nodiscard]] auto external_rotate_left(Node &, Node &, Node &, Index) -> Result<void>;
    [[nodiscard]] auto external_rotate_right(Node &, Node &, Node &, Index) -> Result<void>;
    [[nodiscard]] auto internal_rotate_left(Node &, Node &, Node &, Index) -> Result<void>;
    [[nodiscard]] auto internal_rotate_right(Node &, Node &, Node &, Index) -> Result<void>;

    [[nodiscard]] auto maybe_fix_child_parent_connections(Node &) -> Result<void>;

    Size m_maximum_key_size {};
    ScratchManager m_scratch;
    NodePool *m_pool;
    Size m_cell_count {};
};

} // namespace cco

#endif // CCO_TREE_INTERNAL_H
