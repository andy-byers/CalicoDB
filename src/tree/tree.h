
#ifndef CCO_TREE_TREE_H
#define CCO_TREE_TREE_H

#include <spdlog/spdlog.h>
#include "interface.h"
#include "internal.h"

namespace cco {

class Cursor;
class IBufferPool;

class Tree: public ITree {
public:
    struct Parameters {
        IBufferPool *buffer_pool {};
        spdlog::sink_ptr log_sink;
        PID free_start {};
        Size free_count {};
        Size cell_count {};
        Size node_count {};
    };
    
    ~Tree() override = default;

    [[nodiscard]] auto node_count() const -> Size override
    {
        return m_pool.node_count();
    }

    [[nodiscard]] auto cell_count() const -> Size override
    {
        return m_internal.cell_count();
    }

    [[nodiscard]] auto internal() const -> const Internal& override
    {
        return m_internal;
    }

    [[nodiscard]] auto pool() const -> const NodePool& override
    {
        return m_pool;
    }

    auto internal() -> Internal& override
    {
        return m_internal;
    }

    auto pool() -> NodePool& override
    {
        return m_pool;
    }

    [[nodiscard]] static auto open(Parameters) -> Result<std::unique_ptr<ITree>>;
    [[nodiscard]] auto insert(BytesView, BytesView) -> Result<bool> override;
    [[nodiscard]] auto erase(Cursor) -> Result<bool> override;
    [[nodiscard]] auto root(bool) -> Result<page::Node> override;
    auto save_header(page::FileHeader&) const -> void override;
    auto load_header(const page::FileHeader&) -> void override;
    auto find_exact(BytesView) -> Cursor override;
    auto find(BytesView key) -> Cursor override;
    auto find_minimum() -> Cursor override;
    auto find_maximum() -> Cursor override;

private:
    explicit Tree(Parameters);
    auto find_aux(BytesView, bool&) -> Cursor;

    NodePool m_pool;
    Internal m_internal;
    std::shared_ptr<spdlog::logger> m_logger;
};

} // calico

#endif // CCO_TREE_TREE_H
