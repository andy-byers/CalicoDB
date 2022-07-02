
#ifndef CALICO_TREE_TREE_H
#define CALICO_TREE_TREE_H

#include <spdlog/spdlog.h>
#include "interface.h"
#include "internal.h"

namespace calico {

class Cell;
class Cursor;
class IBufferPool;
class Node;

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

    explicit Tree(Parameters);
    auto save_header(FileHeader&) const -> void override;
    auto load_header(const FileHeader&) -> void override;
    auto insert(BytesView, BytesView) -> bool override;
    auto remove(Cursor) -> bool override;
    auto find(BytesView, bool) -> Cursor override;
    auto find_minimum() -> Cursor override;
    auto find_maximum() -> Cursor override;
    auto root(bool) -> Node override;

private:
    NodePool m_pool;
    Internal m_internal;
    std::shared_ptr<spdlog::logger> m_logger;
};

} // calico

#endif // CALICO_TREE_TREE_H
