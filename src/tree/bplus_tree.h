#ifndef CALICO_TREE_BPLUS_TREE_H
#define CALICO_TREE_BPLUS_TREE_H

#include "cursor_internal.h"
#include "free_list.h"
#include "node.h"
#include "utils/expected.hpp"

namespace Calico {

struct FileHeader;
class Pager;

class BPlusTree {
    /*
     * m_scratch[0]: Overflow cell scratch
     * m_scratch[1]: Extra overflow cell scratch
     * m_scratch[2]: Root fixing routine scratch
     * m_scratch[3]: Defragmentation scratch
     */
    std::array<std::string, 4> m_scratch;

    NodeMeta m_external_meta;
    NodeMeta m_internal_meta;
    CursorActions m_actions;
    FreeList m_free_list;

    Pager *m_pager {};

public:
    friend class BPlusTreeInternal;
    friend class CursorInternal;

    struct SearchResult {
        Node node;
        Size index {};
        bool exact {};
    };

    explicit BPlusTree(Pager &pager);
    [[nodiscard]] auto setup() -> tl::expected<Node, Status>;
    [[nodiscard]] auto lowest() -> tl::expected<Node, Status>;
    [[nodiscard]] auto highest() -> tl::expected<Node, Status>;
    [[nodiscard]] auto collect(Node node, Size index) -> tl::expected<std::string, Status>;
    [[nodiscard]] auto search(const Slice &key) -> tl::expected<SearchResult, Status>;
    [[nodiscard]] auto insert(const Slice &key, const Slice &value) -> tl::expected<bool, Status>;
    [[nodiscard]] auto erase(const Slice &key) -> tl::expected<void, Status>;

    auto save_state(FileHeader &header) const -> void;
    auto load_state(const FileHeader &header) -> void;

    auto TEST_to_string() -> std::string;
    auto TEST_check_order() -> void;
    auto TEST_check_links() -> void;
    auto TEST_check_nodes() -> void;
};

} // namespace Calico

#endif // CALICO_TREE_BPLUS_TREE_H
