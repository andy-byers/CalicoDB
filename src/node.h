// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#ifndef CALICODB_NODE_H
#define CALICODB_NODE_H

#include "header.h"
#include "page.h"

namespace calicodb
{

struct Node;

struct BlockAllocator {
    [[nodiscard]] static auto allocate(Node &node, U32 needed_size) -> int;
    [[nodiscard]] static auto release(Node &node, U32 block_start, U32 block_size) -> int;
    [[nodiscard]] static auto defragment(Node &node, int skip = -1) -> int;
};

// NOTE: Cell headers are padded out to kMinCellHeaderSize, which corresponds to the size of a free block
//       header.
static constexpr U32 kMinCellHeaderSize =
    sizeof(U16) +
    sizeof(U16);
static constexpr U32 kMaxCellHeaderSize =
    kVarintMaxLength + // Value size  (5 B)
    kVarintMaxLength + // Key size    (5 B)
    sizeof(U32);       // Overflow ID (4 B)

// Determine how many bytes of payload can be stored locally (not on an overflow chain)
[[nodiscard]] static constexpr auto compute_local_pl_size(std::size_t key_size, std::size_t value_size) -> U32
{
    // SQLite's computation for min and max local payload sizes. If kMaxLocal is exceeded, then 1 or more
    // overflow chain pages will be required to store this payload.
    constexpr const U32 kMinLocal =
        (kPageSize - NodeHdr::kSize) * 32 / 256 - kMaxCellHeaderSize - sizeof(U16);
    constexpr const U32 kMaxLocal =
        (kPageSize - NodeHdr::kSize) * 64 / 256 - kMaxCellHeaderSize - sizeof(U16);
    if (key_size + value_size <= kMaxLocal) {
        // The whole payload can be stored locally.
        return static_cast<U32>(key_size + value_size);
    } else if (key_size > kMaxLocal) {
        // The first part of the key will occupy the entire local payload.
        return kMaxLocal;
    }
    // Try to prevent the key from being split.
    return std::max(kMinLocal, static_cast<U32>(key_size));
}

// Internal cell format:
//     Size    Name
//    -----------------------
//     4       child_id
//     varint  key_size
//     n       key
//    [4       overflow_id]
//
// External cell format:
//     Size    Name
//    -----------------------
//     varint  value_size
//     varint  key_size
//     n       key
//     m       value
//    [4       overflow_id]
//
struct Cell {
    // Pointer to the start of the cell.
    char *ptr;

    // Pointer to the start of the key.
    char *key;

    // Number of bytes contained in the key.
    U32 key_size;

    // Number of bytes contained in both the key and the value.
    U32 total_pl_size;

    // Number of payload bytes stored locally (embedded in a node).
    U32 local_pl_size;

    // Number of bytes occupied by this cell when embedded.
    U32 footprint;
};

// Simple construct representing a tree node
struct Node final {
    static constexpr U32 kMaxFragCount = 0x80;

    PageRef *ref;
    int (*parser)(char *, const char *, Cell *);
    char *scratch;
    U32 usable_space;
    U32 gap_size;

    [[nodiscard]] static auto from_existing_page(PageRef &page, char *scratch, Node &node_out) -> int;
    [[nodiscard]] static auto from_new_page(PageRef &page, char *scratch, bool is_leaf) -> Node;

    explicit Node()
        : ref(nullptr)
    {
    }

    ~Node() = default;

    Node(const Node &) = delete;
    auto operator=(const Node &) -> Node & = delete;

    Node(Node &&rhs) noexcept
        : ref(rhs.ref),
          parser(rhs.parser),
          scratch(rhs.scratch),
          usable_space(rhs.usable_space),
          gap_size(rhs.gap_size)
    {
        rhs.ref = nullptr;
    }

    auto operator=(Node &&rhs) noexcept -> Node &
    {
        if (this != &rhs) {
            ref = rhs.ref;
            parser = rhs.parser;
            scratch = rhs.scratch;
            usable_space = rhs.usable_space;
            gap_size = rhs.gap_size;

            rhs.ref = nullptr;
        }
        return *this;
    }

    [[nodiscard]] auto hdr() const -> char *
    {
        return ref->page + page_offset(ref->page_id);
    }

    [[nodiscard]] auto is_leaf() const -> bool
    {
        return hdr()[NodeHdr::kTypeOffset] == NodeHdr::kExternal;
    }

    [[nodiscard]] auto read_child_id(U32 index) const -> Id;
    auto write_child_id(U32 index, Id child_id) -> void;

    [[nodiscard]] auto defrag() -> int;
    [[nodiscard]] auto alloc(U32 index, U32 size) -> int;
    [[nodiscard]] auto write(U32 index, const Cell &cell) -> int;
    [[nodiscard]] auto read(U32 index, Cell &cell_out) const -> int;
    auto erase(U32 index, U32 cell_size) -> int;

    [[nodiscard]] auto check_freelist() const -> int;
    [[nodiscard]] auto check_state() const -> int;
    [[nodiscard]] auto assert_state() const -> bool;
};

} // namespace calicodb

#endif // CALICODB_NODE_H
