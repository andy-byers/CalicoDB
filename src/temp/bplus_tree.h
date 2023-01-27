#ifndef CALICO_TREE_BPLUS_TREE_H
#define CALICO_TREE_BPLUS_TREE_H

#include "free_list.h"
#include "node.h"
#include "utils/expected.hpp"

namespace Calico {

struct FileHeader;
class Pager;

class BPlusTree {
    FreeList m_free_list;
    NodeMeta m_external_meta;
    NodeMeta m_internal_meta;

    /*
     * m_scratch[0]: Overflow cell scratch
     * m_scratch[1]: Extra overflow cell scratch
     * m_scratch[2]: Root fixing routine scratch
     * m_scratch[3]: Defragmentation scratch
     */
    std::array<std::string, 4> m_scratch;

    Pager *m_pager {};

public:
    friend class BPlusTreeInternal;

    struct FindResult {
        Node node;
        Size index {};
        bool exact {};
    };

    explicit BPlusTree(Pager &pager);
    [[nodiscard]] auto insert(const Slice &key, const Slice &value) -> tl::expected<bool, Status>;
    [[nodiscard]] auto erase(const Slice &key) -> tl::expected<bool, Status>;
    [[nodiscard]] auto find(const Slice &key) -> tl::expected<FindResult, Status>;

    auto save_state(FileHeader &header) const -> void;
    auto load_state(const FileHeader &header) -> void;

    auto TEST_to_string() -> std::string;
    auto TEST_check_order() -> void;
    auto TEST_check_links() -> void;
    auto TEST_check_nodes() -> void;
};

} // namespace Calico

#endif // CALICO_TREE_BPLUS_TREE_H
