#ifndef CALICO_TREE_H
#define CALICO_TREE_H

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

    [[nodiscard]] auto fetch_key(std::string &buffer, const Cell &cell, Slice &out) const -> Status;

public:
    struct Parameters {
        OverflowList *overflow {};
        std::string *lhs_key {};
        std::string *rhs_key {};
    };
    explicit NodeIterator(Node &node, const Parameters &param);
    [[nodiscard]] auto index() const -> Size;
    [[nodiscard]] auto seek(const Slice &key, bool *found = nullptr) -> Status;
    [[nodiscard]] auto seek(const Cell &cell, bool *found = nullptr) -> Status;
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
    [[nodiscard]] auto emplace(Byte *scratch, Node &node, const Slice &key, const Slice &value, Size index) -> Status;

    /* Prepare a cell read from an external node to be posted into its parent as a separator. Will
     * allocate a new overflow chain for overflowing keys, pointing back to "parent_id".
     */
    [[nodiscard]] auto promote(Byte *scratch, Cell &cell, Id parent_id) -> Status;

    [[nodiscard]] auto collect_key(std::string &scratch, const Cell &cell, Slice &out) const -> Status;
    [[nodiscard]] auto collect_value(std::string &scratch, const Cell &cell, Slice &out) const -> Status;
};

class BPlusTree {
    friend class BPlusTreeInternal;
    friend class BPlusTreeValidator;
    friend class CursorInternal;
    friend class CursorImpl;

    mutable std::string m_lhs_key;
    mutable std::string m_rhs_key;
    mutable std::string m_anchor;
    mutable std::string m_node_scratch;
    mutable std::string m_cell_scratch;

    NodeMeta m_external_meta;
    NodeMeta m_internal_meta;
    PointerMap m_pointers;
    FreeList m_freelist;
    OverflowList m_overflow;
    PayloadManager m_payloads;

    Pager *m_pager {};

    [[nodiscard]] auto vacuum_step(Page &head, Id last_id) -> Status;
    [[nodiscard]] auto cell_scratch() -> Byte *;
    [[nodiscard]] auto lowest(Node &out) -> Status;
    [[nodiscard]] auto highest(Node &out) -> Status;

public:
    explicit BPlusTree(Pager &pager);

    [[nodiscard]] auto collect_key(std::string &scratch, const Cell &cell, Slice &out) const -> Status;
    [[nodiscard]] auto collect_value(std::string &scratch, const Cell &cell, Slice &out) const -> Status;
    [[nodiscard]] auto search(const Slice &key, SearchResult &out) -> Status;
    [[nodiscard]] auto insert(const Slice &key, const Slice &value, bool &exists) -> Status;
    [[nodiscard]] auto erase(const Slice &key) -> Status;
    [[nodiscard]] auto vacuum_one(Id target, bool &vacuumed) -> Status;

    auto save_state(FileHeader &header) const -> void;
    auto load_state(const FileHeader &header) -> void;

    auto TEST_to_string() -> std::string;
    auto TEST_check_order() -> void;
    auto TEST_check_links() -> void;
    auto TEST_check_nodes() -> void;
};

class BPlusTreeInternal {
public:
    explicit BPlusTreeInternal(BPlusTree &tree);

    [[nodiscard]] auto pointers() -> PointerMap *
    {
        return m_pointers;
    }

    [[nodiscard]] auto overflow() -> OverflowList *
    {
        return m_overflow;
    }

    [[nodiscard]] auto payloads() -> PayloadManager *
    {
        return m_payloads;
    }

    [[nodiscard]] auto freelist() -> FreeList *
    {
        return m_freelist;
    }

    [[nodiscard]] auto node_iterator(Node &node) const -> NodeIterator;
    [[nodiscard]] auto is_pointer_map(Id pid) const -> bool;
    [[nodiscard]] auto find_external_slot(const Slice &key, SearchResult &out) const -> Status;
    [[nodiscard]] auto find_parent_id(Id pid, Id &out) const -> Status;
    [[nodiscard]] auto fix_parent_id(Id pid, Id parent_id, PointerMap::Type type = PointerMap::NODE) -> Status;
    [[nodiscard]] auto maybe_fix_overflow_chain(const Cell &cell, Id parent_id) -> Status;
    [[nodiscard]] auto insert_cell(Node &node, Size index, const Cell &cell) -> Status;
    [[nodiscard]] auto remove_cell(Node &node, Size index) -> Status;
    [[nodiscard]] auto fix_links(Node &node) -> Status;

    auto make_existing_node(Node &out) const -> void;
    auto make_fresh_node(Node &out, bool is_external) const -> void;

    [[nodiscard]] auto allocate_root(Node &out) -> Status;
    [[nodiscard]] auto allocate(Node &out, bool is_external) -> Status;
    [[nodiscard]] auto acquire(Node &out, Id pid, bool upgrade = false) const -> Status;
    [[nodiscard]] auto destroy(Node node) -> Status;
    auto upgrade(Node &node) const -> void;
    auto release(Node node) const -> void;

    [[nodiscard]] auto resolve_overflow(Node node) -> Status;
    [[nodiscard]] auto split_root(Node root, Node &out) -> Status;
    [[nodiscard]] auto split_non_root(Node node, Node &out) -> Status;
    [[nodiscard]] auto resolve_underflow(Node node, const Slice &anchor) -> Status;
    [[nodiscard]] auto fix_root(Node root) -> Status;
    [[nodiscard]] auto fix_non_root(Node node, Node &parent, Size index) -> Status;
    [[nodiscard]] auto merge_left(Node &left, Node right, Node &parent, Size index) -> Status;
    [[nodiscard]] auto merge_right(Node &left, Node right, Node &parent, Size index) -> Status;
    [[nodiscard]] auto rotate_left(Node &parent, Node &left, Node &right, Size index) -> Status;
    [[nodiscard]] auto rotate_right(Node &parent, Node &left, Node &right, Size index) -> Status;

private:
    [[nodiscard]] auto split_non_root_fast(Node parent, Node left, Node right, const Cell &overflow, Node &out) -> Status;
    [[nodiscard]] auto find_external_slot(const Slice &key, Node node, SearchResult &out) const -> Status;
    [[nodiscard]] auto transfer_left(Node &left, Node &right) -> Status;
    [[nodiscard]] auto internal_merge_left(Node &left, Node &right, Node &parent, Size index) -> Status;
    [[nodiscard]] auto external_merge_left(Node &left, Node &right, Node &parent, Size index) -> Status;
    [[nodiscard]] auto internal_merge_right(Node &left, Node &right, Node &parent, Size index) -> Status;
    [[nodiscard]] auto external_merge_right(Node &left, Node &right, Node &parent, Size index) -> Status;
    [[nodiscard]] auto external_rotate_left(Node &parent, Node &left, Node &right, Size index) -> Status;
    [[nodiscard]] auto internal_rotate_left(Node &parent, Node &left, Node &right, Size index) -> Status;
    [[nodiscard]] auto external_rotate_right(Node &parent, Node &left, Node &right, Size index) -> Status;
    [[nodiscard]] auto internal_rotate_right(Node &parent, Node &left, Node &right, Size index) -> Status;

    BPlusTree *m_tree {};
    PointerMap *m_pointers {};
    OverflowList *m_overflow {};
    PayloadManager *m_payloads {};
    FreeList *m_freelist {};
    Pager *m_pager {};
};

} // namespace Calico

#endif // CALICO_TREE_H
