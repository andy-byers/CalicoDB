
#ifndef CALICO_TREE_NODE_POOL_H
#define CALICO_TREE_NODE_POOL_H

#include <spdlog/spdlog.h>
#include "free_list.h"
#include "interface.h"
#include "utils/scratch.h"

namespace calico {

class Cell;
class IBufferPool;
class Node;

class NodePool final {
public:
    struct Parameters {
        IBufferPool *buffer_pool {};
        PID free_start {};
        Size free_count {};
        Size node_count {};
    };

    explicit NodePool(Parameters);
    ~NodePool() = default;

    [[nodiscard]] auto page_size() const -> Size;
    auto save_header(FileHeader&) -> void;
    auto load_header(const FileHeader&) -> void;
    auto allocate(PageType) -> Node;
    auto acquire(PID, bool) -> Node;
    auto destroy(Node) -> void;
    auto allocate_overflow_chain(BytesView) -> PID;
    auto destroy_overflow_chain(PID, Size) -> void;
    auto collect_overflow_chain(PID, Bytes) const -> void;

    [[nodiscard]] auto node_count() const -> Size
    {
        return m_node_count;
    }

private:
    FreeList m_free_list;
    IBufferPool *m_pool {};
    Size m_node_count {};
};

} // calico

#endif // CALICO_TREE_NODE_POOL_H
