// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#include "node.h"

namespace calicodb
{

static constexpr U16 kPageMask = kPageSize - 1;
static constexpr U32 kSlotWidth = sizeof(U16);
static constexpr U32 kMinBlockSize = 2 * kSlotWidth;

[[nodiscard]] static auto node_header_offset(const Node &node)
{
    return page_offset(node.ref->page_id);
}

[[nodiscard]] static auto cell_slots_offset(const Node &node) -> U32
{
    return node_header_offset(node) + NodeHdr::kSize;
}

[[nodiscard]] static auto cell_area_offset(const Node &node) -> U32
{
    return cell_slots_offset(node) + NodeHdr::get_cell_count(node.hdr()) * kSlotWidth;
}

[[nodiscard]] static auto get_ivec_slot(const Node &node, U32 index) -> U32
{
    CALICODB_EXPECT_LT(index, NodeHdr::get_cell_count(node.hdr()));
    return kPageMask & get_u16(node.ref->page + cell_slots_offset(node) + index * kSlotWidth);
}

static auto put_ivec_slot(Node &node, U32 index, U32 slot)
{
    CALICODB_EXPECT_LT(index, NodeHdr::get_cell_count(node.hdr()));
    return put_u16(node.ref->page + cell_slots_offset(node) + index * kSlotWidth, static_cast<U16>(slot));
}

static auto insert_ivec_slot(Node &node, U32 index, U32 slot)
{
    CALICODB_EXPECT_GE(node.gap_size, kSlotWidth);
    const auto count = NodeHdr::get_cell_count(node.hdr());
    CALICODB_EXPECT_LE(index, count);
    const auto offset = cell_slots_offset(node) + index * kSlotWidth;
    const auto size = (count - index) * kSlotWidth;
    auto *data = node.ref->page + offset;

    std::memmove(data + kSlotWidth, data, size);
    put_u16(data, static_cast<U16>(slot));

    node.gap_size -= kSlotWidth;
    NodeHdr::put_cell_count(node.hdr(), count + 1);
}

static auto remove_ivec_slot(Node &node, U32 index)
{
    const auto count = NodeHdr::get_cell_count(node.hdr());
    CALICODB_EXPECT_LT(index, count);
    const auto offset = cell_slots_offset(node) + index * kSlotWidth;
    const auto size = (count - index) * kSlotWidth;
    auto *data = node.ref->page + offset;

    std::memmove(data, data + kSlotWidth, size);

    node.gap_size += kSlotWidth;
    NodeHdr::put_cell_count(node.hdr(), count - 1);
}

[[nodiscard]] static auto external_parse_cell(char *data, const char *limit, Cell *cell_out)
{
    U32 key_size, value_size;
    const auto *ptr = data;
    if (!(ptr = decode_varint(ptr, limit, value_size))) {
        return -1;
    }
    if (!(ptr = decode_varint(ptr, limit, key_size))) {
        return -1;
    }
    const auto hdr_size = static_cast<std::uintptr_t>(ptr - data);
    const auto pad_size = hdr_size > kMinCellHeaderSize ? 0 : kMinCellHeaderSize - hdr_size;
    const auto local_pl_size = compute_local_pl_size(key_size, value_size);
    const auto has_remote = local_pl_size < key_size + value_size;
    const auto footprint = hdr_size + pad_size + local_pl_size + has_remote * sizeof(U32);

    if (data + footprint <= limit) {
        if (cell_out) {
            cell_out->ptr = data;
            cell_out->key = data + hdr_size + pad_size;
            cell_out->key_size = key_size;
            cell_out->total_pl_size = key_size + value_size;
            cell_out->local_pl_size = local_pl_size;
            cell_out->footprint = static_cast<U32>(footprint);
        }
        return 0;
    }
    return -1;
}
[[nodiscard]] static auto internal_parse_cell(char *data, const char *limit, Cell *cell_out)
{
    U32 key_size;
    if (const auto *ptr = decode_varint(data + sizeof(U32), limit, key_size)) {
        const auto hdr_size = static_cast<std::uintptr_t>(ptr - data);
        const auto local_pl_size = compute_local_pl_size(key_size, 0);
        const auto has_remote = local_pl_size < key_size;
        const auto footprint = hdr_size + local_pl_size + has_remote * sizeof(U32);
        if (data + footprint <= limit) {
            if (cell_out) {
                cell_out->ptr = data;
                cell_out->key = data + hdr_size;
                cell_out->key_size = key_size;
                cell_out->total_pl_size = key_size;
                cell_out->local_pl_size = local_pl_size;
                cell_out->footprint = static_cast<U32>(footprint);
            }
            return 0;
        }
    }
    return -1;
}

auto Node::alloc(U32 index, U32 size) -> int
{
    CALICODB_EXPECT_LE(index, NodeHdr::get_cell_count(hdr()));

    if (size + kSlotWidth > usable_space) {
        return 0;
    }

    // We don't have room to insert the cell pointer.
    if (gap_size < kSlotWidth) {
        if (defrag()) {
            return -1;
        }
    }
    // Insert a dummy cell pointer to save the slot.
    insert_ivec_slot(*this, index, kPageSize - 1);

    // Attempt to allocate `size` contiguous bytes within `node`.
    int offset = 0;
    if (NodeHdr::get_frag_count(hdr()) + kMinBlockSize - 1 <= kMaxFragCount) {
        offset = BlockAllocator::allocate(*this, size);
    }
    if (offset == 0) {
        // There is enough space in `node`, it just isn't contiguous. Defragment and try again. Note that
        // we pass `index` so that defragment() skips the cell we haven't filled in yet.
        if (BlockAllocator::defragment(*this, static_cast<int>(index))) {
            return -1;
        }
        offset = BlockAllocator::allocate(*this, size);
    }
    // We already made sure we had enough room to fulfill the request. If we had to defragment, the call
    // to allocate() following defragmentation should succeed. If offset < 0 at this point, then corruption
    // was detected in the `node`.
    CALICODB_EXPECT_NE(offset, 0);
    if (offset > 0) {
        put_ivec_slot(*this, index, static_cast<U32>(offset));
        usable_space -= size + kSlotWidth;
    }
    return offset;
}

[[nodiscard]] static auto get_next_pointer(const Node &node, U32 offset) -> U32
{
    return get_u16(node.ref->page + offset);
}

[[nodiscard]] static auto get_block_size(const Node &node, U32 offset) -> U32
{
    return get_u16(node.ref->page + offset + kSlotWidth);
}

static auto set_next_pointer(Node &node, U32 offset, U32 value) -> void
{
    CALICODB_EXPECT_LT(value, kPageSize);
    return put_u16(node.ref->page + offset, static_cast<U16>(value));
}

static auto set_block_size(Node &node, U32 offset, U32 value) -> void
{
    CALICODB_EXPECT_GE(value, kMinBlockSize);
    CALICODB_EXPECT_LT(value, kPageSize);
    return put_u16(node.ref->page + offset + kSlotWidth, static_cast<U16>(value));
}

static auto take_free_space(Node &node, U32 ptr0, U32 ptr1, U32 needed_size) -> int
{
    CALICODB_EXPECT_LT(ptr0, kPageSize);
    CALICODB_EXPECT_LT(ptr1, kPageSize);
    CALICODB_EXPECT_LT(needed_size, kPageSize);

    const auto ptr2 = get_next_pointer(node, ptr1);
    const auto free_size = get_block_size(node, ptr1);
    auto total_free = NodeHdr::get_free_total(node.hdr()) - needed_size;

    CALICODB_EXPECT_GE(free_size, needed_size);
    const auto diff = free_size - needed_size;

    if (diff < kMinBlockSize) {
        total_free -= diff; // Rest of the free block becomes a fragment.
        const auto frag_count = NodeHdr::get_frag_count(node.hdr());
        NodeHdr::put_frag_count(node.hdr(), frag_count + diff);
        if (ptr0 == 0) {
            NodeHdr::put_free_start(node.hdr(), ptr2);
        } else {
            set_next_pointer(node, ptr0, ptr2);
        }
    } else {
        set_block_size(node, ptr1, diff);
    }
    NodeHdr::put_free_total(node.hdr(), total_free);
    return static_cast<int>(ptr1 + diff);
}

static auto allocate_from_freelist(Node &node, U32 needed_size) -> int
{
    if (needed_size > NodeHdr::get_free_total(node.hdr())) {
        return 0;
    }
    auto block_ofs = NodeHdr::get_free_start(node.hdr());
    auto prev_len = cell_area_offset(node) + node.gap_size - kMinBlockSize;
    U32 prev_ofs = 0;

    while (block_ofs) {
        if (prev_ofs + prev_len + kMinBlockSize > block_ofs) {
            return -1;
        }
        const auto block_len = get_block_size(node, block_ofs);
        if (needed_size <= block_len) {
            return take_free_space(node, prev_ofs, block_ofs, needed_size);
        }
        prev_ofs = block_ofs;
        prev_len = block_len;
        block_ofs = get_next_pointer(node, block_ofs);
    }
    return 0;
}

static auto allocate_from_gap(Node &node, U32 needed_size) -> int
{
    if (node.gap_size >= needed_size) {
        node.gap_size -= needed_size;
        const auto offset = NodeHdr::get_cell_start(node.hdr()) - needed_size;
        NodeHdr::put_cell_start(node.hdr(), offset);
        return static_cast<int>(offset);
    }
    return 0;
}

auto BlockAllocator::allocate(Node &node, U32 needed_size) -> int
{
    CALICODB_EXPECT_LT(needed_size, kPageSize);
    if (const auto offset = allocate_from_gap(node, needed_size)) {
        return offset;
    }
    return allocate_from_freelist(node, needed_size);
}

auto BlockAllocator::release(Node &node, U32 block_start, U32 block_size) -> int
{
    // Largest possible fragment that can be reclaimed in this process. All cell headers
    // are padded out to 4 bytes, so anything smaller must be a fragment.
    static constexpr U32 kFragmentCutoff = 3;
    const auto free_total = NodeHdr::get_free_total(node.hdr()) + block_size;
    const auto frag_count = NodeHdr::get_frag_count(node.hdr());
    auto free_start = NodeHdr::get_free_start(node.hdr());
    U32 frag_diff = 0;
    CALICODB_EXPECT_NE(block_size, 0);

    // Blocks of less than kMinBlockSize bytes are too small to hold the free block header,
    // so they must become fragment bytes.
    if (block_size < kMinBlockSize) {
        if (frag_count + block_size <= Node::kMaxFragCount) {
            NodeHdr::put_frag_count(node.hdr(), frag_count + block_size);
            return 0;
        }
        return defragment(node);
    }
    // The free block list is sorted by start position. Find where the
    // new block should go.
    U32 prev = 0;
    auto next = free_start;
    while (next > 0 && next < block_start) {
        prev = next;
        next = get_next_pointer(node, next);
    }

    if (prev != 0) {
        // Merge with the predecessor block.
        const auto before_end = prev + get_block_size(node, prev);
        if (before_end + kFragmentCutoff >= block_start) {
            const auto diff = block_start - before_end;
            block_start = prev;
            block_size += get_block_size(node, prev) + diff;
            frag_diff += diff;
        }
    }
    if (block_start != prev) {
        // There was no left merge. Point the "before" pointer to where the new free
        // block will be inserted.
        if (prev == 0) {
            free_start = block_start;
        } else {
            set_next_pointer(node, prev, block_start);
        }
    }

    if (next != 0) {
        // Merge with the successor block.
        const auto current_end = block_start + block_size;
        if (current_end + kFragmentCutoff >= next) {
            const auto diff = next - current_end;
            block_size += get_block_size(node, next) + diff;
            next = get_next_pointer(node, next);
            frag_diff += diff;
        }
    }
    // If there was a left merge, this will set the next pointer and block size of
    // the free block at "prev".
    set_next_pointer(node, block_start, next);
    set_block_size(node, block_start, block_size);
    NodeHdr::put_frag_count(node.hdr(), frag_count - frag_diff);
    NodeHdr::put_free_start(node.hdr(), free_start);
    // Adjust the free block byte total stored in the header.
    NodeHdr::put_free_total(node.hdr(), free_total + frag_diff);
    return 0;
}

auto BlockAllocator::defragment(Node &node, int skip) -> int
{
    const auto n = NodeHdr::get_cell_count(node.hdr());
    const auto cell_start = NodeHdr::get_cell_start(node.hdr());
    const auto to_skip = skip >= 0 ? static_cast<U32>(skip) : n;
    auto *ptr = node.ref->page;
    U32 end = kPageSize;

    // Copy everything before the indirection vector.
    std::memcpy(node.scratch, ptr, cell_slots_offset(node));
    for (U32 index = 0; index < n; ++index) {
        if (index != to_skip) {
            // Pack cells at the end of the scratch page and write the indirection
            // vector.
            Cell cell;
            if (node.read(index, cell) || end < cell_start + cell.footprint) {
                return -1;
            }
            end -= cell.footprint;
            std::memcpy(node.scratch + end, cell.ptr, cell.footprint);
            put_u16(node.scratch + cell_slots_offset(node) + index * kSlotWidth,
                    static_cast<U16>(end));
        }
    }
    std::memcpy(ptr, node.scratch, kPageSize);

    NodeHdr::put_free_total(node.hdr(), 0);
    NodeHdr::put_free_start(node.hdr(), 0);
    NodeHdr::put_frag_count(node.hdr(), 0);
    NodeHdr::put_cell_start(node.hdr(), end);
    node.gap_size = end - cell_area_offset(node);

    const auto gap_adjust = skip < 0 ? 0 : kSlotWidth;
    if (node.gap_size + gap_adjust != node.usable_space) {
        return -1;
    }
    return 0;
}

static constexpr int (*kParsers[2])(char *, const char *, Cell *) = {
    internal_parse_cell,
    external_parse_cell,
};

static constexpr U32 kMaxCellCount = (kPageSize - NodeHdr::kSize) /
                                     (kMinCellHeaderSize + kSlotWidth);

auto Node::from_existing_page(PageRef &page, char *scratch, Node &node_out) -> int
{
    const auto hdr_offset = page_offset(page.page_id);
    const auto *hdr = page.page + hdr_offset;
    const auto type = NodeHdr::get_type(hdr);
    if (type == NodeHdr::kInvalid) {
        return -1;
    }
    const auto ncells = NodeHdr::get_cell_count(hdr);
    if (ncells > kMaxCellCount) {
        return -1;
    }
    const auto gap_upper = NodeHdr::get_cell_start(hdr);
    const auto gap_lower = hdr_offset + NodeHdr::kSize + ncells * kSlotWidth;
    if (gap_upper < gap_lower) {
        return -1;
    }
    node_out.ref = &page;
    node_out.scratch = scratch;
    node_out.parser = kParsers[type - NodeHdr::kInternal];
    node_out.gap_size = gap_upper - gap_lower;
    node_out.usable_space = node_out.gap_size +
                            NodeHdr::get_free_total(hdr) +
                            NodeHdr::get_frag_count(hdr);
    return 0;
}

auto Node::from_new_page(PageRef &page, char *scratch, bool is_leaf) -> Node
{
    Node node;
    node.ref = &page;
    node.scratch = scratch;
    node.parser = kParsers[is_leaf];

    const auto total_space = static_cast<U32>(
        kPageSize - cell_slots_offset(node));
    node.gap_size = total_space;
    node.usable_space = total_space;

    std::memset(node.hdr(), 0, NodeHdr::kSize);
    NodeHdr::put_cell_start(node.hdr(), kPageSize);
    NodeHdr::put_type(node.hdr(), is_leaf);
    return node;
}

auto Node::read_child_id(U32 index) const -> Id
{
    CALICODB_EXPECT_FALSE(is_leaf());
    const auto cell_count = NodeHdr::get_cell_count(hdr());
    CALICODB_EXPECT_LE(index, cell_count);
    if (index == cell_count) {
        return NodeHdr::get_next_id(hdr());
    }
    return Id(get_u32(ref->page + get_ivec_slot(*this, index)));
}

auto Node::write_child_id(U32 index, Id child_id) -> void
{
    CALICODB_EXPECT_FALSE(is_leaf());
    CALICODB_EXPECT_LE(index, NodeHdr::get_cell_count(hdr()));
    if (index == NodeHdr::get_cell_count(hdr())) {
        NodeHdr::put_next_id(hdr(), child_id);
    } else {
        put_u32(ref->page + get_ivec_slot(*this, index), child_id.value);
    }
}

auto Node::read(U32 index, Cell &cell_out) const -> int
{
    return parser(
        ref->page + get_ivec_slot(*this, index),
        ref->page + kPageSize,
        &cell_out);
}

auto Node::write(U32 index, const Cell &cell) -> int
{
    const auto offset = alloc(index, cell.footprint);
    if (offset > 0) {
        std::memcpy(ref->page + offset, cell.ptr, cell.footprint);
    }
    return offset;
}

auto Node::erase(U32 index, U32 cell_size) -> int
{
    const auto rc = BlockAllocator::release(
        *this,
        get_ivec_slot(*this, index),
        cell_size);
    if (rc == 0) {
        remove_ivec_slot(*this, index);
        usable_space += cell_size + kSlotWidth;
    }
    return rc;
}

auto Node::defrag() -> int
{
    if (BlockAllocator::defragment(*this)) {
        return -1;
    }
    CALICODB_EXPECT_EQ(usable_space, gap_size);
    return 0;
}

auto Node::check_freelist() const -> int
{
    if (kMaxFragCount < NodeHdr::get_frag_count(hdr())) {
        return -1;
    }

    const auto min_ofs = cell_area_offset(*this) + gap_size;
    auto total_len = NodeHdr::get_free_total(hdr());
    auto block_ofs = NodeHdr::get_free_start(hdr());
    const char *ptr = ref->page;

    while (block_ofs && total_len) {
        if (block_ofs < min_ofs || block_ofs + kMinBlockSize > kPageSize) {
            return -1;
        }
        const auto block_len = get_u16(ptr + block_ofs + sizeof(U16));
        if (block_ofs + block_len > kPageSize) {
            return -1;
        }
        const auto next_ofs = get_u16(ptr + block_ofs);
        if (next_ofs && block_ofs + block_len + kMinBlockSize > next_ofs) {
            return -1;
        }
        block_ofs = next_ofs;
        total_len -= block_len;
    }
    if (total_len) {
        return -1;
    }
    return 0;
}

auto Node::check_state() const -> int
{
    const auto account = [this](auto from, auto size) {
        if (from > kPageSize || from + size > kPageSize) {
            return -1;
        }
        for (auto *ptr = scratch + from; ptr != scratch + from + size; ++ptr) {
            if (*ptr) {
                return -1;
            } else {
                *ptr = 1;
            }
        }
        return 0;
    };
    std::memset(scratch, 0, kPageSize);

    // Header(s) and cell pointers.
    if (account(0U, cell_area_offset(*this))) {
        return -1;
    }
    // Make sure the header fields are not obviously wrong.
    if (Node::kMaxFragCount < NodeHdr::get_frag_count(hdr()) ||
        kPageSize / 2 < NodeHdr::get_cell_count(hdr()) ||
        kPageSize < NodeHdr::get_free_start(hdr())) {
        return -1;
    }

    // Gap space.
    if (account(cell_area_offset(*this), gap_size)) {
        return -1;
    }

    // Free list blocks.
    auto i = NodeHdr::get_free_start(hdr());
    const char *data = ref->page;
    while (i) {
        if (account(i, get_u16(data + i + kSlotWidth))) {
            return -1;
        }
        i = get_u16(data + i);
    }

    if (check_freelist()) {
        return -1;
    }

    // Cell bodies. Also makes sure the cells are in order where possible.
    for (i = 0; i < NodeHdr::get_cell_count(hdr()); ++i) {
        Cell left_cell;
        if (U32 ivec_slot; read(i, left_cell) ||
                           kPageSize <= (ivec_slot = get_ivec_slot(*this, i)) ||
                           account(ivec_slot, left_cell.footprint)) {
            return -1;
        }

        if (i + 1 < NodeHdr::get_cell_count(hdr())) {
            Cell right_cell;
            if (read(i + 1, right_cell)) {
                return -1;
            }
            const Slice left_local(left_cell.key, std::min(left_cell.key_size, left_cell.local_pl_size));
            const Slice right_local(right_cell.key, std::min(right_cell.key_size, right_cell.local_pl_size));
            const auto right_has_ovfl = right_cell.key_size > right_cell.local_pl_size;
            if (right_local < left_local || (left_local == right_local && !right_has_ovfl)) {
                return -1;
            }
        }
    }

    // Every byte should be accounted for, except for fragments.
    auto total_bytes = NodeHdr::get_frag_count(hdr());
    for (i = 0; i < kPageSize; ++i) {
        total_bytes += scratch[i] == '\x01';
    }
    return -(total_bytes != kPageSize);
}

auto Node::assert_state() const -> bool
{
    CALICODB_EXPECT_EQ(0, check_state());
    return true;
}

} // namespace calicodb