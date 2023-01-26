#ifndef CALICO_TREE_BPLUS_TREE_H
#define CALICO_TREE_BPLUS_TREE_H

#include "node.h"
#include "utils/expected.hpp"

namespace Calico {

struct FileHeader;
class Pager;

class BPlusTree_ {
    Pager *m_pager {};
    NodeMeta m_external_meta;
    NodeMeta m_internal_meta;

    /*
     * m_scratch[0]: Overflow cell scratch
     * m_scratch[1]:
     * m_scratch[2]:
     * m_scratch[3]: Defragmentation scratch
     */
    std::array<std::string, 4> m_scratch;

public:
    friend class BPlusTreeImpl;

    struct FindResult {
        Node_ node;
        Size index {};
        bool exact {};
    };

    explicit BPlusTree_(Pager &pager);
    [[nodiscard]] auto insert(const Slice &key, const Slice &value) -> tl::expected<bool, Status>;
    [[nodiscard]] auto erase(const Slice &key) -> tl::expected<bool, Status>;
    [[nodiscard]] auto find(const Slice &key) -> tl::expected<FindResult, Status>;

    auto save_state(FileHeader_ &header) const -> void;
    auto load_state(const FileHeader_ &header) -> void;

    auto TEST_to_string() -> std::string;
    auto TEST_check_order() -> void;
    auto TEST_check_links() -> void;
};

} // namespace Calico

#endif // CALICO_TREE_BPLUS_TREE_H
