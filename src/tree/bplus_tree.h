#ifndef CALICO_TREE_BPLUS_TREE_H
#define CALICO_TREE_BPLUS_TREE_H

#include "cursor_internal.h"
#include "internal.h"
#include "spdlog/spdlog.h"
#include "tree.h"
#include "utils/system.h"

namespace Calico {

class Cursor;
class Pager;

class BPlusTree : public Tree {
public:
    ~BPlusTree() override = default;

    [[nodiscard]]
    auto record_count() const -> Size override
    {
        return m_internal.cell_count();
    }

    [[nodiscard]] static auto open(Pager &pager, System &state, Size page_size) -> tl::expected<Tree::Ptr, Status>;
    [[nodiscard]] auto insert(Slice key, Slice value) -> Status override;
    [[nodiscard]] auto erase(Cursor cursor) -> Status override;
    [[nodiscard]] auto root(bool is_writable) -> tl::expected<Node, Status> override;
    [[nodiscard]] auto find_exact(Slice key) -> Cursor override;
    [[nodiscard]] auto find(Slice key) -> Cursor override;
    [[nodiscard]] auto find_minimum() -> Cursor override;
    [[nodiscard]] auto find_maximum() -> Cursor override;
    auto save_state(FileHeader &header) const -> void override;
    auto load_state(const FileHeader &header) -> void override;

#if not NDEBUG
    auto TEST_to_string(bool integer_keys) -> std::string override;
    auto TEST_validate_nodes() -> void override;
    auto TEST_validate_order() -> void override;
    auto TEST_validate_links() -> void override;
#endif // not NDEBUG

private:
    struct SearchResult {
        Node node;
        Size index {};
        bool was_found {};
    };
    BPlusTree(Pager &pager, System &state, Size page_size);
    [[nodiscard]] auto find_aux(Slice key) -> tl::expected<SearchResult, Status>;
    [[nodiscard]] auto check_key(Slice key, const char *primary) -> Status;

    CursorActions m_actions;
    NodeManager m_pool;
    Internal m_internal;
    LogPtr m_log;
    System *m_system {};
};

} // namespace Calico

#endif // CALICO_TREE_BPLUS_TREE_H