
#ifndef CALICO_TREE_BPLUS_TREE_H
#define CALICO_TREE_BPLUS_TREE_H

#include "internal.h"
#include "tree.h"
#include <spdlog/spdlog.h>

namespace calico {

class Cursor;
class Pager;

class BPlusTree : public Tree {
public:
    ~BPlusTree() override;

    [[nodiscard]]
    auto record_count() const -> Size override
    {
        return m_internal.cell_count();
    }

    [[nodiscard]] static auto open(Pager &, spdlog::sink_ptr, Size) -> Result<std::unique_ptr<BPlusTree>>;
    [[nodiscard]] auto insert(BytesView, BytesView) -> Status override;
    [[nodiscard]] auto erase(Cursor) -> Status override;
    [[nodiscard]] auto root(bool) -> Result<Node> override;
    [[nodiscard]] auto find_exact(BytesView) -> Cursor override;
    [[nodiscard]] auto find(BytesView key) -> Cursor override;
    [[nodiscard]] auto find_minimum() -> Cursor override;
    [[nodiscard]] auto find_maximum() -> Cursor override;
    auto save_state(FileHeader &header) const -> void override;
    auto load_state(const FileHeader &header) -> void override;
    auto TEST_validate_node(PageId) -> void override;

private:
    struct SearchResult {
        Node node;
        Size index {};
        bool was_found {};
    };
    BPlusTree(Pager &, spdlog::sink_ptr, Size);
    auto find_aux(BytesView) -> Result<SearchResult>;

    NodePool m_pool;
    Internal m_internal;
    std::shared_ptr<spdlog::logger> m_logger;
};

} // namespace calico

#endif // CALICO_TREE_BPLUS_TREE_H
