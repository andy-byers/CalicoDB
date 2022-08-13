
#ifndef CALICO_TREE_NODE_POOL_H
#define CALICO_TREE_NODE_POOL_H

#include "free_list.h"
#include "tree.h"
#include "utils/scratch.h"
#include <spdlog/spdlog.h>

namespace calico {

class Pager;

class NodePool final {
public:

    NodePool(Pager&, Size);
    ~NodePool() = default;

    [[nodiscard]] auto page_size() const -> Size;
    [[nodiscard]] auto allocate(PageType) -> Result<Node>;
    [[nodiscard]] auto acquire(PageId, bool) -> Result<Node>;
    [[nodiscard]] auto release(Node) -> Result<void>;
    [[nodiscard]] auto destroy(Node) -> Result<void>;
    [[nodiscard]] auto allocate_chain(BytesView) -> Result<PageId>;
    [[nodiscard]] auto destroy_chain(PageId, Size) -> Result<void>;
    [[nodiscard]] auto collect_chain(PageId, Bytes) const -> Result<void>;
    auto save_state(FileHeader &header) -> void;
    auto load_state(const FileHeader &header) -> void;

private:
    FreeList m_free_list;
    std::string m_scratch;
    Pager *m_pager {};
};

} // namespace cco

#endif // CALICO_TREE_NODE_POOL_H
