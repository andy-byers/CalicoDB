
#ifndef CCO_TREE_NODE_POOL_H
#define CCO_TREE_NODE_POOL_H

#include "free_list.h"
#include "interface.h"
#include "utils/scratch.h"
#include <spdlog/spdlog.h>

namespace cco {

class IBufferPool;

class NodePool final {
public:
    struct Parameters {
        IBufferPool *buffer_pool {};
        PID free_start;
        Size free_count {};
        Size node_count {};
    };

    explicit NodePool(Parameters);
    ~NodePool() = default;

    [[nodiscard]] auto page_size() const -> Size;
    [[nodiscard]] auto allocate(PageType) -> Result<Node>;
    [[nodiscard]] auto acquire(PID, bool) -> Result<Node>;
    [[nodiscard]] auto release(Node) -> Result<void>;
    [[nodiscard]] auto destroy(Node) -> Result<void>;
    [[nodiscard]] auto allocate_chain(BytesView) -> Result<PID>;
    [[nodiscard]] auto destroy_chain(PID, Size) -> Result<void>;
    [[nodiscard]] auto collect_chain(PID, Bytes) const -> Result<void>;
    auto save_header(FileHeaderWriter &) -> void;
    auto load_header(const FileHeaderReader &) -> void;

    [[nodiscard]] auto node_count() const -> Size
    {
        return m_node_count;
    }

private:
    FreeList m_free_list;
    IBufferPool *m_pool {};
    Size m_node_count {};
};

} // namespace cco

#endif // CCO_TREE_NODE_POOL_H
