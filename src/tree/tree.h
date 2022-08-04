
#ifndef CCO_TREE_TREE_H
#define CCO_TREE_TREE_H

#include "interface.h"
#include "internal.h"
#include <spdlog/spdlog.h>

namespace cco {

class Cursor;
class IBufferPool;

class Tree : public ITree {
public:
    struct Parameters {
        IBufferPool *buffer_pool {};
        spdlog::sink_ptr log_sink;
        PageId free_start {};
        Size cell_count {};
    };

    ~Tree() override = default;

    [[nodiscard]] auto cell_count() const -> Size override
    {
        return m_internal.cell_count();
    }

    [[nodiscard]] auto internal() const -> const Internal & override
    {
        return m_internal;
    }

    [[nodiscard]] auto pool() const -> const NodePool & override
    {
        return m_pool;
    }

    auto internal() -> Internal & override
    {
        return m_internal;
    }

    auto pool() -> NodePool & override
    {
        return m_pool;
    }

    [[nodiscard]] static auto open(const Parameters &) -> Result<std::unique_ptr<ITree>>;
    [[nodiscard]] auto insert(BytesView, BytesView) -> Result<bool> override;
    [[nodiscard]] auto erase(Cursor) -> Result<bool> override;
    [[nodiscard]] auto root(bool) -> Result<Node> override;
    [[nodiscard]] auto allocate_root() -> Result<Node> override;
    auto save_header(FileHeaderWriter &) const -> void override;
    auto load_header(const FileHeaderReader &) -> void override;
    auto find_exact(BytesView) -> Cursor override;
    auto find(BytesView key) -> Cursor override;
    auto find_minimum() -> Cursor override;
    auto find_maximum() -> Cursor override;
    auto TEST_validate_node(PageId) -> void override;

private:
    struct SearchResult {
        Node node;
        Index index {};
        bool was_found {};
    };
    explicit Tree(const Parameters &);
    auto find_aux(BytesView) -> Result<SearchResult>;

    NodePool m_pool;
    Internal m_internal;
    std::shared_ptr<spdlog::logger> m_logger;
};

} // namespace cco

#endif // CCO_TREE_TREE_H
