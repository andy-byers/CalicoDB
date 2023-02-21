#ifndef CALICO_TREE_H
#define CALICO_TREE_H

#include <array>
#include "memory.h"
#include "node.h"

namespace Calico {

struct FileHeader;
class BPlusTree;

struct SearchResult {
    Node node;
    Size index {};
    bool exact {};
};

// TODO: This implementation takes a shortcut and reads fragmented keys into a temporary buffer.
//       This isn't necessary: we could iterate through, page by page, and compare bytes as we encounter
//       them. It's just a bit more complicated.
class NodeIterator {
    OverflowList *m_overflow {};
    std::string *m_lhs_key {};
    std::string *m_rhs_key {};
    Node *m_node {};
    Size m_index {};

    [[nodiscard]] auto fetch_key(std::string &buffer, const Cell &cell) const -> tl::expected<Slice, Status>;

public:
    struct Parameters {
        OverflowList *overflow {};
        std::string *lhs_key {};
        std::string *rhs_key {};
    };
    explicit NodeIterator(Node &node, const Parameters &param);
    [[nodiscard]] auto is_valid() const -> bool;
    [[nodiscard]] auto index() const -> Size;
    [[nodiscard]] auto seek(const Slice &key) -> tl::expected<bool, Status>;
    [[nodiscard]] auto seek(const Cell &cell) -> tl::expected<bool, Status>;
};

class PayloadManager {
    const NodeMeta *m_meta {};
    OverflowList *m_overflow {};

public:
    explicit PayloadManager(const NodeMeta &meta, OverflowList &overflow);

    /* Create a cell in an external node. If the node overflows, the cell will be created in scratch
     * memory and set as the node's overflow cell. May allocate an overflow chain with its back pointer
     * pointing to "node".
     */
    [[nodiscard]] auto emplace(Byte *scratch, Node &node, const Slice &key, const Slice &value, Size index) -> tl::expected<void, Status>;

    /* Prepare a cell read from an external node to be posted into its parent as a separator. Will
     * allocate a new overflow chain for overflowing keys, pointing back to "parent_id".
     */
    [[nodiscard]] auto promote(Byte *scratch, Cell &cell, Id parent_id) -> tl::expected<void, Status>;

    [[nodiscard]] auto collect_key(std::string &scratch, const Cell &cell) const -> tl::expected<Slice, Status>;
    [[nodiscard]] auto collect_value(std::string &scratch, const Cell &cell) const -> tl::expected<Slice, Status>;
};

class BPlusTree {
    friend class BPlusTreeInternal;
    friend class BPlusTreeValidator;
    friend class CursorInternal;
    friend class CursorImpl;

    mutable std::array<std::string, 4> m_scratch;
    mutable std::string m_lhs_key;
    mutable std::string m_rhs_key;
    mutable std::string m_anchor;

    NodeMeta m_external_meta;
    NodeMeta m_internal_meta;
    PointerMap m_pointers;
    FreeList m_freelist;
    OverflowList m_overflow;
    PayloadManager m_payloads;

    Pager *m_pager {};

    [[nodiscard]] auto vacuum_step(Page &head, Id last_id) -> tl::expected<void, Status>;
    [[nodiscard]] auto make_existing_node(Page page) const -> Node;
    [[nodiscard]] auto make_fresh_node(Page page, bool is_external) const -> Node;
    [[nodiscard]] auto scratch(Size index) -> Byte *;
    [[nodiscard]] auto allocate(bool is_external) -> tl::expected<Node, Status>;
    [[nodiscard]] auto acquire(Id pid, bool upgrade = false) const -> tl::expected<Node, Status>;
    [[nodiscard]] auto lowest() const -> tl::expected<Node, Status>;
    [[nodiscard]] auto highest() const -> tl::expected<Node, Status>;
    [[nodiscard]] auto destroy(Node node) -> tl::expected<void, Status>;
    auto release(Node node) const -> void;

public:
    explicit BPlusTree(Pager &pager);

    [[nodiscard]] auto setup() -> tl::expected<Node, Status>;
    [[nodiscard]] auto collect_key(std::string &scratch, const Cell &cell) const -> tl::expected<Slice, Status>;
    [[nodiscard]] auto collect_value(std::string &scratch, const Cell &cell) const -> tl::expected<Slice, Status>;
    [[nodiscard]] auto search(const Slice &key) -> tl::expected<SearchResult, Status>;
    [[nodiscard]] auto insert(const Slice &key, const Slice &value) -> tl::expected<bool, Status>;
    [[nodiscard]] auto erase(const Slice &key) -> tl::expected<void, Status>;
    [[nodiscard]] auto vacuum_one(Id target) -> tl::expected<bool, Status>;

    auto save_state(FileHeader &header) const -> void;
    auto load_state(const FileHeader &header) -> void;

    struct Components {
        FreeList *freelist {};
        OverflowList *overflow {};
        PointerMap *pointers {};
    };

    auto TEST_to_string() -> std::string;
    auto TEST_components() -> Components;
    auto TEST_check_order() -> void;
    auto TEST_check_links() -> void;
    auto TEST_check_nodes() -> void;
};

} // namespace Calico

#endif // CALICO_TREE_H
