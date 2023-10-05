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
    // Count the total number of bytes in the intra-node freelist
    // Returns a nonnegative number on success and -1 on failure. This routine should
    // fail if the freelist is not valid. The following properties are checked:
    // (1) All free blocks are entirely contained within the cell content area
    // (2) Free blocks are sorted by offset, and no 2 free blocks overlap
    // (3) Any 2 adjacent free blocks are separated by at least 4 bytes (otherwise,
    //     there is a fragment between the two blocks that should have been consumed
    //     by release()).
    [[nodiscard]] static auto freelist_size(const Node &node, uint32_t total_space) -> int;

    // Release unused memory back to the node
    // Returns 0 on success and -1 on failure. The freelist (and gap) was already
    // validated when the node was created, so this routine must ensure that those
    // invariants are maintained.
    [[nodiscard]] static auto release(Node &node, uint32_t block_start, uint32_t block_size) -> int;

    // Allocate memory for a cell
    // Returns the offset of the allocated region on success. Returns 0 if the node
    // doesn't have `needed_size` contiguous bytes available. This routine should
    // never encounter corruption.
    [[nodiscard]] static auto allocate(Node &node, uint32_t needed_size) -> uint32_t;

    // Get rid of the fragmentation present in a `node`
    // Returns 0 on success and -1 on failure. If `skip` is set to the index of a
    // particular cell, that cell will be skipped during processing.
    [[nodiscard]] static auto defragment(Node &node, int skip = -1) -> int;
};

// NOTE: Cell headers are padded out to kMinCellHeaderSize, which corresponds to the size
//       of a free block header.
static constexpr size_t kMinCellHeaderSize =
    sizeof(uint16_t) +
    sizeof(uint16_t);
static constexpr size_t kMaxCellHeaderSize =
    kVarintMaxLength + // Value size  (5 B)
    kVarintMaxLength + // Key size    (5 B)
    sizeof(uint32_t);  // Overflow ID (4 B)

struct LocalBounds {
    uint32_t min;
    uint32_t max;
};

// Determine how many bytes of payload can be stored locally (not on an overflow chain)
// Uses SQLite's computation for min and max local payload sizes. If "max local" is exceeded, then 1 or more
// overflow chain pages will be required to store this payload.
[[nodiscard]] inline auto compute_local_pl_size(size_t key_size, size_t value_size, uint32_t total_space) -> uint32_t
{
    const auto max_local = static_cast<uint32_t>((total_space - NodeHdr::kSize) * 64 / 256 -
                                                 kMaxCellHeaderSize - sizeof(uint16_t));
    if (key_size + value_size <= max_local) {
        // The whole payload can be stored locally.
        return static_cast<uint32_t>(key_size + value_size);
    } else if (key_size > max_local) {
        // The first part of the key will occupy the entire local payload.
        return max_local;
    }
    const auto min_local = static_cast<uint32_t>((total_space - NodeHdr::kSize) * 32 / 256 -
                                                 kMaxCellHeaderSize - sizeof(uint16_t));
    // Try to prevent the key from being split.
    return maxval(min_local, static_cast<uint32_t>(key_size));
}

// Internal cell format:
//     Size   | Name
//    --------|---------------
//     4      | child_id
//     varint | key_size
//     n      | key
//     4      | overflow_id*
//
// External cell format:
//     Size   | Name
//    --------|---------------
//     varint | value_size
//     varint | key_size
//     n      | key
//     m      | value
//     4      | overflow_id*
//
// * overflow_id field is only present when the cell payload is unable to
//   fit entirely within the node page. In this case, it holds the page ID
//   of the first overflow chain page that the payload has spilled onto.
struct Cell {
    // Pointer to the start of the cell.
    char *ptr;

    // Pointer to the start of the key.
    char *key;

    // Number of bytes contained in the key.
    uint32_t key_size;

    // Number of bytes contained in both the key and the value.
    uint32_t total_pl_size;

    // Number of payload bytes stored locally (embedded in a node).
    uint32_t local_pl_size;

    // Number of bytes occupied by this cell when embedded.
    uint32_t footprint;
};

// Simple construct representing a tree node
struct Node final {
    using ParseCell = int (*)(char *, const char *, uint32_t, Cell &);
    static constexpr uint32_t kMaxFragCount = 0x80;

    PageRef *ref;
    ParseCell parser;
    char *scratch;
    uint32_t total_space;
    uint32_t usable_space;
    uint32_t gap_size;

    [[nodiscard]] static auto from_existing_page(PageRef &page, uint32_t total_space, char *scratch, Node &node_out) -> int;
    [[nodiscard]] static auto from_new_page(PageRef &page, uint32_t total_space, char *scratch, bool is_leaf) -> Node;

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
          total_space(rhs.total_space),
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
            total_space = rhs.total_space;
            usable_space = rhs.usable_space;
            gap_size = rhs.gap_size;

            rhs.ref = nullptr;
        }
        return *this;
    }

    [[nodiscard]] auto hdr() const -> char *
    {
        return ref->data + page_offset(ref->page_id);
    }

    [[nodiscard]] auto is_leaf() const -> bool
    {
        return hdr()[NodeHdr::kTypeOffset] == NodeHdr::kExternal;
    }

    [[nodiscard]] auto read_child_id(uint32_t index) const -> Id;
    auto write_child_id(uint32_t index, Id child_id) -> void;

    [[nodiscard]] auto defrag() -> int;
    [[nodiscard]] auto alloc(uint32_t index, uint32_t size) -> int;
    [[nodiscard]] auto insert(uint32_t index, const Cell &cell) -> int;
    [[nodiscard]] auto read(uint32_t index, Cell &cell_out) const -> int;
    auto erase(uint32_t index, uint32_t cell_size) -> int;

    [[nodiscard]] auto check_integrity() const -> Status;
    [[nodiscard]] auto assert_integrity() const -> bool;
};

} // namespace calicodb

#endif // CALICODB_NODE_H
