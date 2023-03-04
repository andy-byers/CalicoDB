#ifndef CALICODB_TREE_H
#define CALICODB_TREE_H

#include "page.h"
#include <optional>

namespace calicodb
{

class Pager;
struct Node;

static constexpr std::size_t kMaxCellHeaderSize =
    sizeof(std::uint64_t) + // Value size  (varint)
    sizeof(std::uint64_t) + // Key size    (varint)
    sizeof(Id);             // Overflow ID (8 B)

inline constexpr auto compute_min_local(std::size_t page_size) -> std::size_t
{
    CDB_EXPECT_TRUE(is_power_of_two(page_size));
    // NOTE: This computation was adapted from a similar one in SQLite3.
    return (page_size - NodeHeader::kSize) * 32 / 256 -
           kMaxCellHeaderSize - sizeof(PageSize);
}

inline constexpr auto compute_max_local(std::size_t page_size) -> std::size_t
{
    CDB_EXPECT_TRUE(is_power_of_two(page_size));
    // NOTE: This computation was adapted from a similar one in SQLite3.
    return (page_size - NodeHeader::kSize) * 64 / 256 -
           kMaxCellHeaderSize - sizeof(PageSize);
}

inline constexpr auto compute_local_size(std::size_t key_size, std::size_t value_size, std::size_t min_local, std::size_t max_local) -> std::size_t
{
    if (key_size + value_size <= max_local) {
        return key_size + value_size;
    } else if (key_size > max_local) {
        return max_local;
    }
    // Try to prevent the key from being split.
    return std::max(min_local, key_size);
}

/* Internal Cell Format:
 *     std::size_t    Name
 *    -----------------------
 *     8       child_id
 *     varint  key_size
 *     n       key
 *     8       [overflow_id]
 *
 * External Cell Format:
 *     std::size_t    Name
 *    -----------------------
 *     varint  value_size
 *     varint  key_size
 *     n       key
 *     m       value
 *     8       [overflow_id]
 */
struct Cell {
    char *ptr {};
    char *key {};
    std::size_t local_size {};
    std::size_t key_size {};
    std::size_t size {};
    bool is_free {};
    bool has_remote {};
};

struct NodeMeta {
    using CellSize = std::size_t (*)(const NodeMeta &, const char *);
    using ParseCell = Cell (*)(const NodeMeta &, char *);

    CellSize cell_size {};
    ParseCell parse_cell {};
    std::size_t min_local {};
    std::size_t max_local {};
};

struct Node {
    Node() = default;
    [[nodiscard]] auto take() && -> Page;

    Node(Node &&rhs) noexcept = default;
    auto operator=(Node &&) noexcept -> Node & = default;

    [[nodiscard]] auto get_slot(std::size_t index) const -> std::size_t;
    auto set_slot(std::size_t index, std::size_t pointer) -> void;
    auto insert_slot(std::size_t index, std::size_t pointer) -> void;
    auto remove_slot(std::size_t index) -> void;

    auto TEST_validate() -> void;

    Page page;
    char *scratch {};
    const NodeMeta *meta {};
    NodeHeader header;
    std::optional<Cell> overflow;
    PageSize overflow_index {};
    PageSize slots_offset {};
    PageSize gap_size {};
};

struct NodeFactory {
    [[nodiscard]] static auto make_new(Page page, char *scratch, bool is_external) -> Node;
    [[nodiscard]] static auto make_existing(Page page, char *scratch) -> Node;
};

/*
 * Determine the amount of usable space remaining in the node.
 */
[[nodiscard]] auto usable_space(const Node &node) -> std::size_t;

/*
 * Read a cell from the node at the specified index or offset. The node must remain alive for as long as the cell.
 */
[[nodiscard]] auto read_cell_at(Node &node, std::size_t offset) -> Cell;
[[nodiscard]] auto read_cell(Node &node, std::size_t index) -> Cell;

/*
 * Write a cell to the node at the specified index. May defragment the node. The cell must be of the same
 * type as the node, or if the node is internal, promote_cell() must have been called on the cell.
 */
auto write_cell(Node &node, std::size_t index, const Cell &cell) -> std::size_t;

/*
 * Erase a cell from the node at the specified index.
 */
auto erase_cell(Node &node, std::size_t index) -> void;
auto erase_cell(Node &node, std::size_t index, std::size_t size_hint) -> void;

/*
 * Manually defragment the node. Collects all cells at the end of the page with no room in-between (adds the
 * intra-node freelist and fragments back to the "gap").
 */
auto manual_defragment(Node &node) -> void;

/*
 * Helpers for constructing a cell in an external node. emplace_cell() will write an overflow ID if one is provided. It is
 * up to the caller to determine if one is needed, allocate it, and provide its value here.
 */
[[nodiscard]] auto allocate_block(Node &node, std::uint16_t index, std::uint16_t size) -> std::size_t;
auto emplace_cell(char *out, std::size_t key_size, std::size_t value_size, const Slice &local_key, const Slice &local_value, Id overflow_id = Id::null()) -> char *;

/*
 * Write the cell into backing memory and update its pointers.
 */
auto detach_cell(Cell &cell, char *backing) -> void;

[[nodiscard]] auto read_child_id_at(const Node &node, std::size_t offset) -> Id;
auto write_child_id_at(Node &node, std::size_t offset, Id child_id) -> void;

[[nodiscard]] auto read_child_id(const Node &node, std::size_t index) -> Id;
[[nodiscard]] auto read_child_id(const Cell &cell) -> Id;
[[nodiscard]] auto read_overflow_id(const Cell &cell) -> Id;
auto write_child_id(Node &node, std::size_t index, Id child_id) -> void;
auto write_child_id(Cell &cell, Id child_id) -> void;
auto write_overflow_id(Cell &cell, Id overflow_id) -> void;

auto merge_root(Node &root, Node &child) -> void;


/* Pointer map management. Most pages in the database have a parent page. For node pages, the parent is clear:
 * it is the page that contains a child reference to the current page. For non-node pages, i.e. overflow links and
 * freelist links, the parent is the link that came before it. For overflow links, the parent of the first link
 * is the node page that the chain originated in. The only 2 pages that don't have a parent are the root page and
 * the head of the freelist.
 *
 * Special care must be taken to ensure that the pointer maps stay correct. Pointer map entries must be updated in
 * the following situations:
 *     (1) A parent-child tree connection is changed
 *     (2) A cell with an overflow chain is moved between nodes
 *     (3) During all freelist and some overflow chain operations
 *
 * NOTE: The purpose of this data structure is to make the vacuum operation possible. It lets us swap any 2 pages,
 *       and easily update the pages that reference them. This lets us swap freelist pages with pages from the end
 *       of the file, after which the file can be truncated.
 */
class PointerMap
{
    Pager *m_pager {};

public:
    enum Type : char {
        kNode = 1,
        kOverflowHead,
        kOverflowLink,
        kFreelistLink,
    };

    struct Entry {
        Id back_ptr;
        Type type {};
    };

    explicit PointerMap(Pager &pager)
        : m_pager {&pager}
    {
    }

    // Find the page ID of the pointer map that holds the back pointer for page "pid".
    [[nodiscard]] auto lookup(Id pid) const -> Id;

    // Read an entry from a pointer map.
    [[nodiscard]] auto read_entry(Id pid, Entry &entry) const -> Status;

    // Write an entry to a pointer map.
    [[nodiscard]] auto write_entry(Id pid, Entry entry) -> Status;
};

/* Freelist management. The freelist is essentially a linked list that is threaded through the database. Each freelist
 * link page contains a pointer to the next freelist link page, or to Id::null() if it is the last link. Pages that are
 * no longer needed by the tree are placed at the front of the freelist. When more pages are needed, the freelist is
 * checked first. Only if it is empty do we allocate a page from the end of the file.
 */
class Freelist
{
    friend class BPlusTree;

    Pager *m_pager {};
    PointerMap *m_pointers {};
    Id m_head;

public:
    explicit Freelist(Pager &pager, PointerMap &pointers)
        : m_pager {&pager},
          m_pointers {&pointers}
    {
    }

    [[nodiscard]] auto is_empty() const -> bool
    {
        return m_head.is_null();
    }

    [[nodiscard]] auto pop(Page &page) -> Status;
    [[nodiscard]] auto push(Page page) -> Status;
};

/* Overflow chain management. The tree engine attempts to store all data in external node pages. If the user inserts
 * a record that is too large, part of the payload key and/or value will be transferred to one or more overflow
 * chain pages. Like the freelist, an overflow chain forms a singly-linked list of pages. The difference is that
 * each overflow chain page contains both a pointer and payload data, while each freelist page only contains a
 * "next" pointer.
 */
class OverflowList
{
    Pager *m_pager {};
    Freelist *m_freelist {};
    PointerMap *m_pointers {};

    std::string m_scratch; // TODO: Only needed for copy_chain(). Not actually necessary, it just makes it easier. Should fix that at some point.

public:
    explicit OverflowList(Pager &pager, Freelist &freelist, PointerMap &pointers)
        : m_pager {&pager},
          m_freelist {&freelist},
          m_pointers {&pointers}
    {
    }

    [[nodiscard]] auto read_chain(Span out, Id pid, std::size_t offset = 0) const -> Status;
    [[nodiscard]] auto write_chain(Id &out, Id pid, Slice first, Slice second = {}) -> Status;
    [[nodiscard]] auto copy_chain(Id &out, Id pid, Id overflow_id, std::size_t size) -> Status;
    [[nodiscard]] auto erase_chain(Id pid) -> Status;
};

[[nodiscard]] auto read_next_id(const Page &page) -> Id;
auto write_next_id(Page &page, Id) -> void;

struct FileHeader;
class BPlusTree;

struct SearchResult {
    Node node;
    std::size_t index {};
    bool exact {};
};

// TODO: This implementation takes a shortcut and reads fragmented keys into a temporary buffer.
//       This isn't necessary: we could iterate through, page by page, and compare bytes as we encounter
//       them. It's just a bit more complicated.
class NodeIterator
{
    OverflowList *m_overflow {};
    std::string *m_lhs_key {};
    std::string *m_rhs_key {};
    Node *m_node {};
    std::size_t m_index {};

    [[nodiscard]] auto fetch_key(std::string &buffer, const Cell &cell, Slice &out) const -> Status;

public:
    struct Parameters {
        OverflowList *overflow {};
        std::string *lhs_key {};
        std::string *rhs_key {};
    };
    explicit NodeIterator(Node &node, const Parameters &param);
    [[nodiscard]] auto index() const -> std::size_t;
    [[nodiscard]] auto seek(const Slice &key, bool *found = nullptr) -> Status;
    [[nodiscard]] auto seek(const Cell &cell, bool *found = nullptr) -> Status;
};

class PayloadManager
{
    const NodeMeta *m_meta {};
    OverflowList *m_overflow {};

public:
    explicit PayloadManager(const NodeMeta &meta, OverflowList &overflow);

    /* Create a cell in an external node. If the node overflows, the cell will be created in scratch
     * memory and set as the node's overflow cell. May allocate an overflow chain with its back pointer
     * pointing to "node".
     */
    [[nodiscard]] auto emplace(char *scratch, Node &node, const Slice &key, const Slice &value, std::size_t index) -> Status;

    /* Prepare a cell read from an external node to be posted into its parent as a separator. Will
     * allocate a new overflow chain for overflowing keys, pointing back to "parent_id".
     */
    [[nodiscard]] auto promote(char *scratch, Cell &cell, Id parent_id) -> Status;

    [[nodiscard]] auto collect_key(std::string &scratch, const Cell &cell, Slice &out) const -> Status;
    [[nodiscard]] auto collect_value(std::string &scratch, const Cell &cell, Slice &out) const -> Status;
};

class BPlusTree
{
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
    Freelist m_freelist;
    OverflowList m_overflow;
    PayloadManager m_payloads;

    Pager *m_pager {};

    [[nodiscard]] auto vacuum_step(Page &head, Id last_id) -> Status;
    [[nodiscard]] auto cell_scratch() -> char *;
    [[nodiscard]] auto lowest(Node &out) -> Status;
    [[nodiscard]] auto highest(Node &out) -> Status;

public:
    explicit BPlusTree(Pager &pager);

    [[nodiscard]] auto collect_key(std::string &scratch, const Cell &cell, Slice &out) const -> Status;
    [[nodiscard]] auto collect_value(std::string &scratch, const Cell &cell, Slice &out) const -> Status;
    [[nodiscard]] auto search(const Slice &key, SearchResult &out) -> Status;
    [[nodiscard]] auto insert(const Slice &key, const Slice &value, bool *exists = nullptr) -> Status;
    [[nodiscard]] auto erase(const Slice &key) -> Status;
    [[nodiscard]] auto vacuum_one(Id target, bool &vacuumed) -> Status;

    auto save_state(FileHeader &header) const -> void;
    auto load_state(const FileHeader &header) -> void;

    auto TEST_to_string() -> std::string;
    auto TEST_check_order() -> void;
    auto TEST_check_links() -> void;
    auto TEST_check_nodes() -> void;
};

class BPlusTreeInternal
{
public:
    explicit BPlusTreeInternal(BPlusTree &tree);

    [[nodiscard]] auto pager() -> Pager *
    {
        return m_pager;
    }

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

    [[nodiscard]] auto freelist() -> Freelist *
    {
        return m_freelist;
    }

    [[nodiscard]] auto node_iterator(Node &node) const -> NodeIterator;
    [[nodiscard]] auto is_pointer_map(Id pid) const -> bool;
    [[nodiscard]] auto find_external_slot(const Slice &key, SearchResult &out) const -> Status;
    [[nodiscard]] auto find_parent_id(Id pid, Id &out) const -> Status;
    [[nodiscard]] auto fix_parent_id(Id pid, Id parent_id, PointerMap::Type type = PointerMap::kNode) -> Status;
    [[nodiscard]] auto maybe_fix_overflow_chain(const Cell &cell, Id parent_id) -> Status;
    [[nodiscard]] auto insert_cell(Node &node, std::size_t index, const Cell &cell) -> Status;
    [[nodiscard]] auto remove_cell(Node &node, std::size_t index) -> Status;
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
    [[nodiscard]] auto fix_non_root(Node node, Node &parent, std::size_t index) -> Status;
    [[nodiscard]] auto merge_left(Node &left, Node right, Node &parent, std::size_t index) -> Status;
    [[nodiscard]] auto merge_right(Node &left, Node right, Node &parent, std::size_t index) -> Status;
    [[nodiscard]] auto rotate_left(Node &parent, Node &left, Node &right, std::size_t index) -> Status;
    [[nodiscard]] auto rotate_right(Node &parent, Node &left, Node &right, std::size_t index) -> Status;

private:
    [[nodiscard]] auto split_non_root_fast(Node parent, Node left, Node right, const Cell &overflow, Node &out) -> Status;
    [[nodiscard]] auto find_external_slot(const Slice &key, Node node, SearchResult &out) const -> Status;
    [[nodiscard]] auto transfer_left(Node &left, Node &right) -> Status;
    [[nodiscard]] auto internal_merge_left(Node &left, Node &right, Node &parent, std::size_t index) -> Status;
    [[nodiscard]] auto external_merge_left(Node &left, Node &right, Node &parent, std::size_t index) -> Status;
    [[nodiscard]] auto internal_merge_right(Node &left, Node &right, Node &parent, std::size_t index) -> Status;
    [[nodiscard]] auto external_merge_right(Node &left, Node &right, Node &parent, std::size_t index) -> Status;
    [[nodiscard]] auto external_rotate_left(Node &parent, Node &left, Node &right, std::size_t index) -> Status;
    [[nodiscard]] auto internal_rotate_left(Node &parent, Node &left, Node &right, std::size_t index) -> Status;
    [[nodiscard]] auto external_rotate_right(Node &parent, Node &left, Node &right, std::size_t index) -> Status;
    [[nodiscard]] auto internal_rotate_right(Node &parent, Node &left, Node &right, std::size_t index) -> Status;

    BPlusTree *m_tree {};
    PointerMap *m_pointers {};
    OverflowList *m_overflow {};
    PayloadManager *m_payloads {};
    Freelist *m_freelist {};
    Pager *m_pager {};
};

} // namespace calicodb

namespace calicodb {

struct NodeManager
{
    [[nodiscard]] static auto allocate(Pager &pager, Freelist &freelist, Node *node, bool is_external) -> Status;
    [[nodiscard]] static auto acquire(Pager &pager, Node *node, bool upgrade) -> Status;
    [[nodiscard]] static auto release(Pager &pager, Node node) -> Status;
    [[nodiscard]] static auto destroy(Freelist &freelist, Node node) -> Status;
};

struct OverflowList_
{
    [[nodiscard]] static auto read(Pager &pager, std::string *out, Id head_id, std::size_t offset = 0) -> Status;
    [[nodiscard]] static auto write(Pager &pager, Id *out, Id parent_id, const Slice &first, const Slice &second = {}) -> Status;
    [[nodiscard]] static auto copy(Pager &pager, Id *out, Id parent_id, Id overflow_id, std::size_t size) -> Status;
    [[nodiscard]] static auto erase(Pager &pager, Id head_id) -> Status;
};

class PayloadManager_
{
    [[nodiscard]] static auto emplace(Pager &pager, std::string &scratch, Node &node, const Slice &key, const Slice &value, std::size_t index) -> Status;
    [[nodiscard]] static auto promote(Pager &pager, std::string &scratch, Cell &cell, Id parent_id) -> Status;
    [[nodiscard]] static auto collect_key(Pager &pager, std::string &scratch, const Cell &cell, const Slice *key) -> Status;
    [[nodiscard]] static auto collect_value(Pager &pager, std::string &scratch, const Cell &cell, const Slice *value) -> Status;
};

class Tree {
public:
    explicit Tree(Id root, Pager &pager, Freelist &freelist);

    [[nodiscard]] auto find_external(const Slice &key, SearchResult *out) const -> Status;
    [[nodiscard]] auto resolve_overflow(Node node) -> Status;
    [[nodiscard]] auto resolve_underflow(Node node, const Slice &anchor) -> Status;

private:
    [[nodiscard]] auto split_root(Node root, Node &out) -> Status;
    [[nodiscard]] auto split_non_root(Node node, Node &out) -> Status;
    [[nodiscard]] auto fix_root(Node root) -> Status;
    [[nodiscard]] auto fix_non_root(Node node, Node &parent, std::size_t index) -> Status;
    [[nodiscard]] auto merge_left(Node &left, Node right, Node &parent, std::size_t index) -> Status;
    [[nodiscard]] auto merge_right(Node &left, Node right, Node &parent, std::size_t index) -> Status;
    [[nodiscard]] auto rotate_left(Node &parent, Node &left, Node &right, std::size_t index) -> Status;
    [[nodiscard]] auto rotate_right(Node &parent, Node &left, Node &right, std::size_t index) -> Status;
    [[nodiscard]] auto split_non_root_fast(Node parent, Node left, Node right, const Cell &overflow, Node &out) -> Status;
    [[nodiscard]] auto find_external_slot(const Slice &key, Node node, SearchResult &out) const -> Status;
    [[nodiscard]] auto transfer_left(Node &left, Node &right) -> Status;
    [[nodiscard]] auto internal_merge_left(Node &left, Node &right, Node &parent, std::size_t index) -> Status;
    [[nodiscard]] auto external_merge_left(Node &left, Node &right, Node &parent, std::size_t index) -> Status;
    [[nodiscard]] auto internal_merge_right(Node &left, Node &right, Node &parent, std::size_t index) -> Status;
    [[nodiscard]] auto external_merge_right(Node &left, Node &right, Node &parent, std::size_t index) -> Status;
    [[nodiscard]] auto external_rotate_left(Node &parent, Node &left, Node &right, std::size_t index) -> Status;
    [[nodiscard]] auto internal_rotate_left(Node &parent, Node &left, Node &right, std::size_t index) -> Status;
    [[nodiscard]] auto external_rotate_right(Node &parent, Node &left, Node &right, std::size_t index) -> Status;
    [[nodiscard]] auto internal_rotate_right(Node &parent, Node &left, Node &right, std::size_t index) -> Status;

    Freelist *m_freelist {};
    Pager *m_pager {};
    Id m_root;
};

} // namespace calicodb

#endif // CALICODB_TREE_H
