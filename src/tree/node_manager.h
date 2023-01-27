
#ifndef CALICO_TREE_NODE_POOL_H
#define CALICO_TREE_NODE_POOL_H

#include "free_list.h"
#include "spdlog/spdlog.h"
#include "tree.h"
#include "utils/scratch.h"

namespace Calico {

class Pager;
class System;

class NodeManager final {
public:
    NodeManager(Pager &, System &system, Size page_size);
    ~NodeManager() = default;

    [[nodiscard]] auto page_size() const -> Size;
    [[nodiscard]] auto page_count() const -> Size;
    [[nodiscard]] auto allocate(PageType type) -> tl::expected<Node__, Status>;
    [[nodiscard]] auto acquire(Id pid, bool is_writable) -> tl::expected<Node__, Status>;
    [[nodiscard]] auto release(Node__ node) -> tl::expected<void, Status>;
    [[nodiscard]] auto destroy(Node__ node) -> tl::expected<void, Status>;
    [[nodiscard]] auto allocate_chain(Slice value) -> tl::expected<Id, Status>;
    [[nodiscard]] auto destroy_chain(Id pid, Size size) -> tl::expected<void, Status>;
    [[nodiscard]] auto collect_chain(Id pid, Span out) const -> tl::expected<void, Status>;
    auto save_state(FileHeader__ &header) -> void;
    auto load_state(const FileHeader__ &header) -> void;

private:
    FreeList__ m_free_list;
    std::string m_scratch;
    Pager *m_pager {};
    System *m_system {};
};

} // namespace Calico

#endif // CALICO_TREE_NODE_POOL_H
