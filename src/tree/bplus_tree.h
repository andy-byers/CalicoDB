#ifndef CALICO_TREE_BPLUS_TREE_H__
#define CALICO_TREE_BPLUS_TREE_H__

#include "cursor_internal.h"
#include "internal.h"
#include "spdlog/spdlog.h"
#include "tree.h"
#include "utils/system.h"

namespace Calico {

class Cursor;
class Pager;

class BPlusTree__ : public Tree {
public:
    ~BPlusTree__() override = default;

    [[nodiscard]]
    auto record_count() const -> Size override
    {
        return m_internal.cell_count();
    }

    [[nodiscard]] static auto open(Pager &pager, System &state, Size page_size) -> tl::expected<Tree::Ptr, Status>;
    [[nodiscard]] auto insert(const Slice &key, const Slice &value) -> Status override;
    [[nodiscard]] auto erase(Cursor cursor) -> Status override;
    [[nodiscard]] auto root(bool is_writable) -> tl::expected<Node__, Status> override;
    [[nodiscard]] auto find_exact(const Slice &key) -> Cursor override;
    [[nodiscard]] auto find(const Slice &key) -> Cursor override;
    [[nodiscard]] auto find_minimum() -> Cursor override;
    [[nodiscard]] auto find_maximum() -> Cursor override;
    auto save_state(FileHeader__ &header) const -> void override;
    auto load_state(const FileHeader__ &header) -> void override;

#if not NDEBUG
    auto TEST_to_string(bool integer_keys) -> std::string override;
    auto TEST_validate_nodes() -> void override;
    auto TEST_validate_order() -> void override;
    auto TEST_validate_links() -> void override;
#endif // not NDEBUG

private:
    struct SearchResult {
        Node__ node;
        Size index {};
        bool was_found {};
    };
    BPlusTree__(Pager &pager, System &state, Size page_size);
    [[nodiscard]] auto find_aux(const Slice &key) -> tl::expected<SearchResult, Status>;
    [[nodiscard]] auto check_key(const Slice &key, const char *primary) -> Status;

    CursorActions m_actions;
    NodeManager m_pool;
    Internal m_internal;
    LogPtr m_log;
    System *m_system {};
};

class BPlusTree : public Tree_ {
public:
    ~BPlusTree() override = default;
    [[nodiscard]] static auto open(Pager &pager, System &system) -> Tree_::Ptr;
    [[nodiscard]] auto insert(const Slice &key, const Slice &value) -> bool override;
    [[nodiscard]] auto erase(const Slice &key) -> bool override;
    [[nodiscard]] auto find(const Slice &key) const -> FindResult override;
    auto save_state(FileHeader__ &header) const -> void override;
    auto load_state(const FileHeader__ &header) -> void override;

private:
    BPlusTree(Pager &pager, System &system);

    Size m_maximum_key_size {};
    std::array<StaticScratch, 3> m_scratch;
    Pager *m_pager {};
    System *m_system {};
};

} // namespace Calico

#endif // CALICO_TREE_BPLUS_TREE_H__