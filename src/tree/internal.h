#ifndef CCO_TREE_INTERNAL_H
#define CCO_TREE_INTERNAL_H

#include <spdlog/spdlog.h>
#include "node_pool.h"
#include "interface.h"
#include "page/node.h"
#include "utils/scratch.h"

namespace cco {

class Internal final {
public:
    struct Position {
        page::Node node;
        Index index {};
    };

    struct SearchResult_ {
        PID id;
        Index index {};
        bool was_found {};
    };

    struct FindResult {
        page::Node node;
        Index index {};
        bool flag {};
    };

    struct Parameters {
        NodePool *pool {};
        Size cell_count {};
    };

    explicit Internal(Parameters);
    ~Internal() = default;
    [[nodiscard]] auto collect_value(const page::Node&, Index) const -> Result<std::string>;
    [[nodiscard]] auto find_external_(BytesView) -> Result<SearchResult_>;
    [[nodiscard]] auto find_minimum() -> Result<SearchResult_>;
    [[nodiscard]] auto find_maximum() -> Result<SearchResult_>;
    [[nodiscard]] auto find_root(bool) -> Result<page::Node>;
    [[nodiscard]] auto find_external(BytesView, bool) -> Result<FindResult>;
    [[nodiscard]] auto find_local_min(page::Node) -> Result<Position>;
    [[nodiscard]] auto find_local_max(page::Node) -> Result<Position>;
    [[nodiscard]] auto make_cell(BytesView, BytesView, bool) -> Result<page::Cell>;
    [[nodiscard]] auto positioned_insert(Position, BytesView, BytesView) -> Result<void>;
    [[nodiscard]] auto positioned_modify(Position, BytesView) -> Result<void>;
    [[nodiscard]] auto positioned_remove(Position) -> Result<void>;
    auto save_header(page::FileHeaderWriter&) const -> void;
    auto load_header(const page::FileHeaderReader&) -> void;

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
    [[nodiscard]] auto balance_after_overflow(page::Node) -> Result<void>;
    [[nodiscard]] auto split_non_root(page::Node) -> Result<page::Node>;
    [[nodiscard]] auto split_root(page::Node) -> Result<page::Node>;

    [[nodiscard]] auto balance_after_underflow(page::Node, BytesView) -> Result<void>;
    [[nodiscard]] auto fix_non_root(page::Node, page::Node&, Index) -> Result<bool>;
    [[nodiscard]] auto fix_root(page::Node) -> Result<void>;
    [[nodiscard]] auto rotate_left(page::Node&, page::Node&, page::Node&, Index) -> Result<void>;
    [[nodiscard]] auto rotate_right(page::Node&, page::Node&, page::Node&, Index) -> Result<void>;
    [[nodiscard]] auto external_rotate_left(page::Node&, page::Node&, page::Node&, Index) -> Result<void>;
    [[nodiscard]] auto external_rotate_right(page::Node&, page::Node&, page::Node&, Index) -> Result<void>;
    [[nodiscard]] auto internal_rotate_left(page::Node&, page::Node&, page::Node&, Index) -> Result<void>;
    [[nodiscard]] auto internal_rotate_right(page::Node&, page::Node&, page::Node&, Index) -> Result<void>;

    [[nodiscard]] auto maybe_fix_child_parent_connections(page::Node&) -> Result<void>;

    Size m_maximum_key_size {};
    utils::ScratchManager m_scratch;
    NodePool *m_pool;
    Size m_cell_count {};
};

} // cco

#endif // CCO_TREE_INTERNAL_H
