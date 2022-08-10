
#ifndef CCO_TREE_NODE_POOL_H
#define CCO_TREE_NODE_POOL_H

#include "free_list.h"
#include "interface.h"
#include "utils/scratch.h"
#include <spdlog/spdlog.h>

namespace cco {

class BufferPool;

class NodePool final {
public:
    struct Parameters {
        BufferPool *buffer_pool {};
        PageId free_start;
    };

    explicit NodePool(Parameters);
    ~NodePool() = default;

    [[nodiscard]] auto page_size() const -> Size;
    [[nodiscard]] auto allocate(PageType) -> Result<Node>;
    [[nodiscard]] auto acquire(PageId, bool) -> Result<Node>;
    [[nodiscard]] auto release(Node) -> Result<void>;
    [[nodiscard]] auto destroy(Node) -> Result<void>;
    [[nodiscard]] auto allocate_chain(BytesView) -> Result<PageId>;
    [[nodiscard]] auto destroy_chain(PageId, Size) -> Result<void>;
    [[nodiscard]] auto collect_chain(PageId, Bytes) const -> Result<void>;
    auto save_header(FileHeaderWriter &) -> void;
    auto load_header(const FileHeaderReader &) -> void;

private:
    FreeList m_free_list;
    std::string m_scratch;
    BufferPool *m_pool {};
};

} // namespace cco

#endif // CCO_TREE_NODE_POOL_H
