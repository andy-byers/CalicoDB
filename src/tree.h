// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#ifndef CALICODB_TREE_H
#define CALICODB_TREE_H

#include "calicodb/cursor.h"
#include "header.h"
#include "page.h"
#include "pager.h"
#include <optional>

namespace calicodb
{

class CursorImpl;
class Schema;
struct Node;

// Maintains a list of available memory regions within a node, called the free
// block list, as well as the "gap" space between the cell with the lowest offset
// and the end of the cell pointer array.
//
// Free blocks take the form (offset, size), where "offset" and "size" are 16-bit
// unsigned integers. The entries are kept sorted by the "offset" field, and
// adjacent regions are merged if possible. Free blocks must be at least 4 bytes.
// If a region smaller than 4 bytes is released, it is considered a fragment, and
// its size is added to the "frag_count" node header field. Luckily, it is possible
// to clean up some of these fragments as regions are merged (see release()).
class BlockAllocator
{
public:
    static auto accumulate_free_bytes(const Node &node) -> std::size_t;

    // Allocate "needed_size" bytes of contiguous memory in "node" and return the
    // offset of the first byte, relative to the start of the page.
    static auto allocate(Node &node, std::size_t needed_size) -> std::size_t;

    // Free "block_size" bytes of contiguous memory in "node" starting at
    // "block_start".
    static auto release(Node &node, std::size_t block_start, std::size_t block_size) -> void;

    // Merge all free blocks and fragment bytes into the gap space.
    static auto defragment(Node &node, int skip = -1) -> void;
};

// Internal Cell Format:
//     Size    Name
//    -----------------------
//     4       child_id
//     varint  key_size
//     n       key
//     4       [overflow_id]
//
// External Cell Format:
//     Size    Name
//    -----------------------
//     varint  value_size
//     varint  key_size
//     n       key
//     m       value
//     4       [overflow_id]
//
struct Cell {
    char *ptr = nullptr;
    char *key = nullptr;
    std::size_t local_size = 0;
    std::size_t key_size = 0;
    std::size_t size = 0;
    bool is_free = false;
    bool has_remote = false;
};

struct NodeMeta {
    using ParseCell = Cell (*)(char *, const char *);

    ParseCell parse_cell = nullptr;
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
    char *scratch = nullptr;
    const NodeMeta *meta = nullptr;
    NodeHeader header;
    std::optional<Cell> overflow;
    unsigned overflow_index = 0;
    unsigned slots_offset = 0;
    unsigned gap_size = 0;
};

// Read a cell from the node at the specified index or offset. The node must remain alive for as long as the cell.
[[nodiscard]] auto read_cell(Node &node, std::size_t index) -> Cell;

// Write a cell to the node at the specified index. May defragment the node. The cell must be of the same
// type as the node, or if the node is internal, promote_cell() must have been called on the cell.
auto write_cell(Node &node, std::size_t index, const Cell &cell) -> std::size_t;

// Erase a cell from the node at the specified index.
auto erase_cell(Node &node, std::size_t index) -> void;

class BufferLocalizer final
{
    Page m_page;
    Status m_status;
    Pager *m_pager;

    // True if this object has ownership of a database page in `m_page`,
    // false otherwise. If this variable is false, the contents of `m_page`
    // are unspecified.
    bool m_has_page;

public:
    explicit BufferLocalizer(Pager &pager, Id remote);
    ~BufferLocalizer();
    [[nodiscard]] auto is_valid() const -> bool;
    [[nodiscard]] auto status() const -> Status;
    [[nodiscard]] auto value() const -> Slice;
    auto next() -> Id;
};

// TODO: This implementation takes a shortcut and reads fragmented keys into a temporary buffer.
//       This isn't necessary: we could iterate through, page by page, and compare bytes as we encounter
//       them. It's just a bit more complicated.
class NodeIterator
{
    Pager *m_pager = nullptr;
    std::string *m_lhs_key = nullptr;
    std::string *m_rhs_key = nullptr;
    Node *m_node = nullptr;
    std::size_t m_index = 0;

    [[nodiscard]] auto fetch_key(std::string &buffer, const Cell &cell, Slice &out) const -> Status;

public:
    struct Parameters {
        Pager *pager = nullptr;
        std::string *lhs_key = nullptr;
        std::string *rhs_key = nullptr;
    };
    explicit NodeIterator(Node &node, const Parameters &param);
    [[nodiscard]] auto index() const -> std::size_t;
    [[nodiscard]] auto seek(const Slice &key, bool *found = nullptr) -> Status;
    [[nodiscard]] auto seek(const Cell &cell, bool *found = nullptr) -> Status;
};

struct NodeManager {
    [[nodiscard]] static auto allocate(Pager &pager, Node &out, std::string &scratch, bool is_external) -> Status;
    [[nodiscard]] static auto acquire(Pager &pager, Id page_id, Node &out, std::string &scratch, bool upgrade) -> Status;
    [[nodiscard]] static auto destroy(Pager &pager, Node node) -> Status;
    static auto upgrade(Pager &pager, Node &node) -> void;
    static auto release(Pager &pager, Node node) -> void;
};

struct OverflowList {
    [[nodiscard]] static auto read(Pager &pager, Id head_id, std::size_t offset, std::size_t size, char *scratch) -> Status;
    [[nodiscard]] static auto write(Pager &pager, Id &out, const Slice &key, const Slice &value = {}) -> Status;
    [[nodiscard]] static auto copy(Pager &pager, Id overflow_id, std::size_t size, Id &out) -> Status;
    [[nodiscard]] static auto erase(Pager &pager, Id head_id) -> Status;
};

struct PayloadManager {
    [[nodiscard]] static auto emplace(Pager &pager, char *scratch, Node &node, const Slice &key, const Slice &value, std::size_t index) -> Status;
    [[nodiscard]] static auto promote(Pager &pager, char *scratch, Cell &cell, Id parent_id) -> Status;
    [[nodiscard]] static auto collect_key(Pager &pager, std::string &scratch, const Cell &cell, Slice *key) -> Status;
    [[nodiscard]] static auto collect_value(Pager &pager, std::string &scratch, const Cell &cell, Slice *value) -> Status;
};

struct TreeStatistics {
    std::size_t smo_count = 0;
    std::size_t bytes_read = 0;
    std::size_t bytes_written = 0;
};

class Tree final
{
public:
    explicit Tree(Pager &pager, const Id *root_id);
    ~Tree();
    [[nodiscard]] static auto create(Pager &pager, bool is_root, Id *out) -> Status;
    [[nodiscard]] static auto destroy(Tree &tree) -> Status;
    [[nodiscard]] auto put(const Slice &key, const Slice &value, bool *exists = nullptr) -> Status;
    [[nodiscard]] auto get(const Slice &key, std::string *value) const -> Status;
    [[nodiscard]] auto erase(const Slice &key) -> Status;
    [[nodiscard]] auto vacuum_one(Id target, Schema &schema, bool *success = nullptr) -> Status;
    [[nodiscard]] auto allocate(bool is_external, Node &out) -> Status;
    [[nodiscard]] auto acquire(Id page_id, bool upgrade, Node &out) const -> Status;
    [[nodiscard]] auto free(Node node) -> Status;
    auto upgrade(Node &node) const -> void;
    auto release(Node node) const -> void;

    [[nodiscard]] auto TEST_to_string() const -> std::string;
    auto TEST_validate() const -> void;

    [[nodiscard]] auto root() const -> Id
    {
        return m_root_id ? *m_root_id : Id::root();
    }

    [[nodiscard]] auto statistics() const -> const TreeStatistics &
    {
        return m_stats;
    }

private:
    friend class Schema;

    struct SearchResult {
        Node node;
        std::size_t index = 0;
        bool exact = false;
    };

    [[nodiscard]] auto destroy_impl(Node node) -> Status;
    [[nodiscard]] auto vacuum_step(Page &free, Schema &schema, Id last_id) -> Status;
    [[nodiscard]] auto resolve_overflow(Node node) -> Status;
    [[nodiscard]] auto resolve_underflow(Node node, const Slice &anchor) -> Status;
    [[nodiscard]] auto split_root(Node root, Node &out) -> Status;
    [[nodiscard]] auto split_nonroot(Node node, Node &out) -> Status;
    [[nodiscard]] auto split_nonroot_fast(Node parent, Node left, Node right, const Cell &overflow, Node &out) -> Status;
    [[nodiscard]] auto fix_root(Node root) -> Status;
    [[nodiscard]] auto fix_nonroot(Node node, Node &parent, std::size_t index) -> Status;
    [[nodiscard]] auto merge_left(Node &left, Node right, Node &parent, std::size_t index) -> Status;
    [[nodiscard]] auto merge_right(Node &left, Node right, Node &parent, std::size_t index) -> Status;
    [[nodiscard]] auto rotate_left(Node &parent, Node &left, Node &right, std::size_t index) -> Status;
    [[nodiscard]] auto rotate_right(Node &parent, Node &left, Node &right, std::size_t index) -> Status;
    [[nodiscard]] auto transfer_left(Node &left, Node &right) -> Status;

    [[nodiscard]] auto find_highest(Node &node) const -> Status;
    [[nodiscard]] auto find_lowest(Node &node) const -> Status;
    [[nodiscard]] auto find_external(const Slice &key, Node node, SearchResult &out) const -> Status;
    [[nodiscard]] auto find_external(const Slice &key, SearchResult &out) const -> Status;
    [[nodiscard]] auto node_iterator(Node &node) const -> NodeIterator;
    [[nodiscard]] auto insert_cell(Node &node, std::size_t index, const Cell &cell) -> Status;
    [[nodiscard]] auto remove_cell(Node &node, std::size_t index) -> Status;
    [[nodiscard]] auto find_parent_id(Id page_id, Id &out) const -> Status;
    [[nodiscard]] auto fix_parent_id(Id page_id, Id parent_id, PointerMap::Type type) -> Status;
    [[nodiscard]] auto maybe_fix_overflow_chain(const Cell &cell, Id parent_id) -> Status;
    [[nodiscard]] auto fix_links(Node &node) -> Status;
    [[nodiscard]] auto cell_scratch() -> char *;

    enum ReportType {
        kBytesRead,
        kBytesWritten,
        kSMOCount,
    };

    auto report_stats(ReportType type, std::size_t increment) const -> void;

    friend class DBImpl;
    friend class CursorInternal;
    friend class CursorImpl;
    friend class TreeValidator;

    // List of active cursors.
    CursorImpl *m_cursors = nullptr;

    mutable TreeStatistics m_stats;
    mutable std::string m_key_scratch[2];
    mutable std::string m_node_scratch;
    mutable std::string m_cell_scratch;
    mutable std::string m_anchor;
    Pager *m_pager = nullptr;
    const Id *m_root_id = nullptr;
};

class CursorImpl : public Cursor
{
    struct Location {
        Id page_id;
        unsigned index = 0;
        unsigned count = 0;
    };
    mutable Status m_status;
    std::string m_key;
    std::string m_value;
    std::size_t m_key_size = 0;
    std::size_t m_value_size = 0;
    Tree *m_tree = nullptr;
    Location m_loc;

    CursorImpl *m_prev = nullptr;
    CursorImpl *m_next = nullptr;

    bool m_needs_reposition = false;

    [[nodiscard]] auto reposition() -> std::string;

    auto seek_to(Node node, std::size_t index) -> void;
    auto fetch_payload() -> Status;

public:
    friend class CursorInternal;
    friend class Tree;

    explicit CursorImpl(Tree &tree)
        : m_tree(&tree)
    {
    }

    ~CursorImpl() override;

    [[nodiscard]] auto is_valid() const -> bool override;
    [[nodiscard]] auto status() const -> Status override;
    [[nodiscard]] auto key() const -> Slice override;
    [[nodiscard]] auto value() const -> Slice override;

    auto seek(const Slice &key) -> void override;
    auto seek_first() -> void override;
    auto seek_last() -> void override;
    auto next() -> void override;
    auto previous() -> void override;
};

class CursorInternal
{
public:
    [[nodiscard]] static auto make_cursor(Tree &tree) -> Cursor *;
    static auto invalidate(const Cursor &cursor, Status error) -> void;
};

} // namespace calicodb

#endif // CALICODB_TREE_H
