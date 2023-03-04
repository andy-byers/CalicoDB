#ifndef CALICODB_TREE_H
#define CALICODB_TREE_H

#include "page.h"
#include <optional>

namespace calicodb
{

class Pager;

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
    bool is_root {};
};

/*
 * Read a cell from the node at the specified index or offset. The node must remain alive for as long as the cell.
 */
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

/* Freelist management. The freelist is essentially a linked list that is threaded through the database. Each freelist
 * link page contains a pointer to the next freelist link page, or to Id::null() if it is the last link. Pages that are
 * no longer needed by the tree are placed at the front of the freelist. When more pages are needed, the freelist is
 * checked first. Only if it is empty do we allocate a page from the end of the file.
 */
class Freelist
{
    friend class Tree;

    Pager *m_pager {};
    Id *m_head {};

public:
    explicit Freelist(Pager &pager, Id *head)
        : m_pager {&pager},
          m_head {head}
    {
    }

    [[nodiscard]] auto is_empty() const -> bool
    {
        return m_head->is_null();
    }

    [[nodiscard]] auto pop(Page &page) -> Status;
    [[nodiscard]] auto push(Page page) -> Status;
};

// TODO: This implementation takes a shortcut and reads fragmented keys into a temporary buffer.
//       This isn't necessary: we could iterate through, page by page, and compare bytes as we encounter
//       them. It's just a bit more complicated.
class NodeIterator
{
    Pager *m_pager {};
    std::string *m_lhs_key {};
    std::string *m_rhs_key {};
    Node *m_node {};
    std::size_t m_index {};

    [[nodiscard]] auto fetch_key(std::string &buffer, const Cell &cell, Slice &out) const -> Status;

public:
    struct Parameters {
        Pager *pager {};
        std::string *lhs_key {};
        std::string *rhs_key {};
    };
    explicit NodeIterator(Node &node, const Parameters &param);
    [[nodiscard]] auto index() const -> std::size_t;
    [[nodiscard]] auto seek(const Slice &key, bool *found = nullptr) -> Status;
    [[nodiscard]] auto seek(const Cell &cell, bool *found = nullptr) -> Status;
};

struct PointerMap {
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

    // Find the page ID of the pointer map that holds the back pointer for page "pid".
    [[nodiscard]] static auto lookup(const Pager &pager, Id pid) -> Id;

    // Read an entry from a pointer map.
    [[nodiscard]] static auto read_entry(Pager &pager, Id pid, Entry *entry) -> Status;

    // Write an entry to a pointer map.
    [[nodiscard]] static auto write_entry(Pager &pager, Id pid, Entry entry) -> Status;
};

struct NodeManager
{
    [[nodiscard]] static auto allocate(Pager &pager, Freelist &freelist, Node *out, char *scratch, bool is_external) -> Status;
    [[nodiscard]] static auto acquire(Pager &pager, Id pid, Node *out, char *scratch, bool upgrade) -> Status;
    [[nodiscard]] static auto destroy(Freelist &freelist, Node node) -> Status;
    static auto upgrade(Pager &pager, Node &node) -> void;
    static auto release(Pager &pager, Node node) -> void;
};

struct OverflowList
{
    [[nodiscard]] static auto read(Pager &pager, Span out, Id head_id, std::size_t offset = 0) -> Status;
    [[nodiscard]] static auto write(Pager &pager, Freelist &freelist, Id *out, const Slice &first, const Slice &second = {}) -> Status;
    [[nodiscard]] static auto copy(Pager &pager, Freelist &freelist, Id *out, Id overflow_id, std::size_t size) -> Status;
    [[nodiscard]] static auto erase(Pager &pager, Freelist &freelist, Id head_id) -> Status;
};

struct PayloadManager
{
    [[nodiscard]] static auto emplace(Pager &pager, Freelist &freelist, std::string &scratch, Node &node, const Slice &key, const Slice &value, std::size_t index) -> Status;
    [[nodiscard]] static auto promote(Pager &pager, Freelist &freelist, char *scratch, Cell &cell, Id parent_id) -> Status;
    [[nodiscard]] static auto collect_key(Pager &pager, std::string &scratch, const Cell &cell, Slice *key) -> Status;
    [[nodiscard]] static auto collect_value(Pager &pager, std::string &scratch, const Cell &cell, Slice *value) -> Status;
};

struct SearchResult {
    Node node;
    std::size_t index {};
    bool exact {};
};

class Tree {
public:
    [[nodiscard]] static auto allocate_tree(Pager &pager, Id *root) -> Status;
    explicit Tree(Pager &pager, Id root, Id *freelist_head);
    [[nodiscard]] auto find(const Slice &key, SearchResult *out) const -> Status;
    [[nodiscard]] auto put_back(Node node) -> Status;
    auto load_state(const FileHeader &header) -> void;

    auto TEST_to_string() -> void;
    auto TEST_validate() -> void;

private:
    [[nodiscard]] auto resolve_overflow(Node node) -> Status;
    [[nodiscard]] auto resolve_underflow(Node node, const Slice &anchor) -> Status;
    [[nodiscard]] auto split_root(Node root, Node &out) -> Status;
    [[nodiscard]] auto split_non_root(Node node, Node &out) -> Status;
    [[nodiscard]] auto split_non_root_fast(Node parent, Node left, Node right, const Cell &overflow, Node &out) -> Status;
    [[nodiscard]] auto fix_root(Node root) -> Status;
    [[nodiscard]] auto fix_non_root(Node node, Node &parent, std::size_t index) -> Status;
    [[nodiscard]] auto merge_left(Node &left, Node right, Node &parent, std::size_t index) -> Status;
    [[nodiscard]] auto merge_right(Node &left, Node right, Node &parent, std::size_t index) -> Status;
    [[nodiscard]] auto rotate_left(Node &parent, Node &left, Node &right, std::size_t index) -> Status;
    [[nodiscard]] auto rotate_right(Node &parent, Node &left, Node &right, std::size_t index) -> Status;
    [[nodiscard]] auto transfer_left(Node &left, Node &right) -> Status;
    [[nodiscard]] auto internal_merge_left(Node &left, Node &right, Node &parent, std::size_t index) -> Status;
    [[nodiscard]] auto external_merge_left(Node &left, Node &right, Node &parent, std::size_t index) -> Status;
    [[nodiscard]] auto internal_merge_right(Node &left, Node &right, Node &parent, std::size_t index) -> Status;
    [[nodiscard]] auto external_merge_right(Node &left, Node &right, Node &parent, std::size_t index) -> Status;
    [[nodiscard]] auto external_rotate_left(Node &parent, Node &left, Node &right, std::size_t index) -> Status;
    [[nodiscard]] auto internal_rotate_left(Node &parent, Node &left, Node &right, std::size_t index) -> Status;
    [[nodiscard]] auto external_rotate_right(Node &parent, Node &left, Node &right, std::size_t index) -> Status;
    [[nodiscard]] auto internal_rotate_right(Node &parent, Node &left, Node &right, std::size_t index) -> Status;

    [[nodiscard]] auto find(const Slice &key, Node node, SearchResult *out) const -> Status;
    [[nodiscard]] auto node_iterator(Node &node) const -> NodeIterator;
    [[nodiscard]] auto insert_cell(Node &node, std::size_t index, const Cell &cell) -> Status;
    [[nodiscard]] auto remove_cell(Node &node, std::size_t index) -> Status;
    [[nodiscard]] auto is_pointer_map(Id pid) const -> bool;
    [[nodiscard]] auto find_parent_id(Id pid, Id *out) const -> Status;
    [[nodiscard]] auto fix_parent_id(Id pid, Id parent_id, PointerMap::Type type) -> Status;
    [[nodiscard]] auto maybe_fix_overflow_chain(const Cell &cell, Id parent_id) -> Status;
    [[nodiscard]] auto fix_links(Node &node) -> Status;
    [[nodiscard]] auto cell_scratch() -> char *;

    [[nodiscard]] auto allocate(Node *out, bool is_external) -> Status;
    [[nodiscard]] auto acquire(Node *out, Id pid, bool upgrade) const -> Status;
    [[nodiscard]] auto destroy(Node node) -> Status;
    auto upgrade(Node &node) const -> void;
    auto release(Node node) const -> void;

    mutable std::string m_key_scratch[2];
    mutable std::string m_node_scratch;
    mutable std::string m_cell_scratch;
    Freelist m_freelist;
    Pager *m_pager {};
    Id m_root;
};

} // namespace calicodb

#endif // CALICODB_TREE_H
