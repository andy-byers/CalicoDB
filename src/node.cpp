// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#include "node.h"
#include <algorithm>
#include <vector>

namespace calicodb
{

static constexpr U32 kSlotWidth = sizeof(U16);

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
    return cell_slots_offset(node) + static_cast<U32>(NodeHdr::get_cell_count(node.hdr()) * kSlotWidth);
}

[[nodiscard]] static auto get_ivec_slot(const Node &node, U32 index)
{
    CALICODB_EXPECT_LT(index, NodeHdr::get_cell_count(node.hdr()));
    return get_u16(node.ref->page + cell_slots_offset(node) + index * kSlotWidth);
}

static auto put_ivec_slot(Node &node, U32 index, U32 pointer)
{
    CALICODB_EXPECT_LT(index, NodeHdr::get_cell_count(node.hdr()));
    return put_u16(node.ref->page + cell_slots_offset(node) + index * kSlotWidth, static_cast<U16>(pointer));
}

static auto insert_ivec_slot(Node &node, U32 index, U32 pointer)
{
    CALICODB_EXPECT_GE(node.gap_size, kSlotWidth);
    const auto count = NodeHdr::get_cell_count(node.hdr());
    CALICODB_EXPECT_LE(index, count);
    const auto offset = cell_slots_offset(node) + index * kSlotWidth;
    const auto size = (count - index) * kSlotWidth;
    auto *data = node.ref->page + offset;

    std::memmove(data + kSlotWidth, data, size);
    put_u16(data, static_cast<U16>(pointer));

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

auto Node::alloc(U32 index, U32 size, char *scratch) -> int
{
    CALICODB_EXPECT_LE(index, NodeHdr::get_cell_count(hdr()));

    if (size + kSlotWidth > usable_space) {
        return 0;
    }

    // We don't have room to insert the cell pointer.
    if (gap_size < kSlotWidth) {
        if (defrag(scratch)) {
            return -1;
        }
    }
    // Insert a dummy cell pointer to save the slot.
    insert_ivec_slot(*this, index, kPageSize - 1);

    // Attempt to allocate `size` contiguous bytes within `node`.
    auto offset = BlockAllocator::allocate(*this, size);
    if (offset == 0) {
        // There is enough space in `node`, it just isn't contiguous. Defragment and try again. Note that
        // we pass `index` so that defragment() skips the cell we haven't filled in yet.
        if (BlockAllocator::defragment(*this, scratch, static_cast<int>(index))) {
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

static constexpr U32 kMinBlockSize = 2 * kSlotWidth;

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

    CALICODB_EXPECT_GE(free_size, needed_size);
    const auto diff = free_size - needed_size;

    if (diff < kMinBlockSize) {
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
    return static_cast<int>(ptr1 + diff);
}

static auto allocate_from_freelist(Node &node, U32 needed_size) -> int
{
    U32 prev_ptr = 0;
    auto curr_ptr = NodeHdr::get_free_start(node.hdr());

    while (curr_ptr) {
        if (needed_size <= get_block_size(node, curr_ptr)) {
            return take_free_space(node, prev_ptr, curr_ptr, needed_size);
        }
        prev_ptr = curr_ptr;
        curr_ptr = get_next_pointer(node, curr_ptr);
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
    auto frag_count = NodeHdr::get_frag_count(node.hdr());
    auto free_start = NodeHdr::get_free_start(node.hdr());
    CALICODB_EXPECT_NE(block_size, 0);

    // Blocks of less than kMinBlockSize bytes are too small to hold the free block header,
    // so they must become fragment bytes.
    if (block_size < kMinBlockSize) {
        NodeHdr::put_frag_count(node.hdr(), frag_count + block_size);
        return 0;
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
            frag_count -= diff;
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
            frag_count -= diff;
            next = get_next_pointer(node, next);
        }
    }
    // If there was a left merge, this will set the next pointer and block size of
    // the free block at "prev".
    set_next_pointer(node, block_start, next);
    set_block_size(node, block_start, block_size);
    NodeHdr::put_frag_count(node.hdr(), frag_count);
    NodeHdr::put_free_start(node.hdr(), free_start);
    return 0;
}

auto BlockAllocator::defragment(Node &node, char *scratch, int skip) -> int
{
    const auto n = NodeHdr::get_cell_count(node.hdr());
    const auto to_skip = skip >= 0 ? static_cast<U32>(skip) : n;
    auto *ptr = node.ref->page;
    U32 end = kPageSize;

    // Copy everything before the indirection vector.
    std::memcpy(scratch, ptr, cell_slots_offset(node));
    for (U32 index = 0; index < n; ++index) {
        if (index != to_skip) {
            // Pack cells at the end of the scratch page and write the indirection
            // vector.
            Cell cell;
            if (node.read(index, cell)) {
                return -1;
            }
            end -= cell.footprint;
            std::memcpy(scratch + end, cell.ptr, cell.footprint);
            put_u16(scratch + cell_slots_offset(node) + index * kSlotWidth,
                    static_cast<U16>(end));
        }
    }
    std::memcpy(ptr, scratch, kPageSize);

    NodeHdr::put_free_start(node.hdr(), 0);
    NodeHdr::put_frag_count(node.hdr(), 0);
    NodeHdr::put_cell_start(node.hdr(), end);
    node.gap_size = end - cell_area_offset(node);
    return 0;
}

auto BlockAllocator::freelist_size(const Node &node) -> int
{
    U32 size = 0;
    for (auto ptr = NodeHdr::get_free_start(node.hdr()); ptr;) {
        if (ptr >= kPageSize - kMinBlockSize + 1) {
            return -1;
        }
        size += get_block_size(node, ptr);
        ptr = get_next_pointer(node, ptr);
    }
    return static_cast<int>(size);
}

static constexpr int (*kParsers[2])(char *, const char *, Cell *) = {
    internal_parse_cell,
    external_parse_cell,
};

auto Node::from_existing_page(PageRef &page, Node &node_out) -> int
{
    node_out.ref = &page;
    node_out.parser = kParsers[node_out.is_leaf()];

    const auto gap_upper = NodeHdr::get_cell_start(node_out.hdr());
    const auto gap_lower = cell_area_offset(node_out);
    if (gap_upper < gap_lower) {
        return -1;
    }
    const auto free_size = BlockAllocator::freelist_size(node_out);
    if (free_size < 0) {
        return -1;
    }
    node_out.gap_size = gap_upper - gap_lower;
    node_out.usable_space = node_out.gap_size +
                            NodeHdr::get_frag_count(node_out.hdr()) +
                            static_cast<U32>(free_size);
    return 0;
}

auto Node::from_new_page(PageRef &page, bool is_leaf) -> Node
{
    const auto total_space = static_cast<U32>(
        kPageSize - page_offset(page.page_id) - NodeHdr::kSize);

    Node node;
    node.ref = &page;
    node.parser = kParsers[is_leaf];
    node.gap_size = total_space;
    node.usable_space = total_space;

    std::memset(node.hdr(), 0, NodeHdr::kSize);
    NodeHdr::put_cell_start(node.hdr(), kPageSize);
    NodeHdr::put_type(node.hdr(), is_leaf ? NodeHdr::kExternal : NodeHdr::kInternal);
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

auto Node::write(U32 index, const Cell &cell, char *scratch) -> int
{
    const auto offset = alloc(index, cell.footprint, scratch);
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

auto Node::defrag(char *scratch) -> int
{
    if (BlockAllocator::defragment(*this, scratch)) {
        return -1;
    }
    CALICODB_EXPECT_EQ(usable_space, gap_size);
    return 0;
}

auto Node::validate(char *scratch) const -> int
{
    const auto account = [&scratch](auto from, auto size) {
        auto lower = scratch + long(from);
        auto upper = scratch + long(from) + long(size);
        if (std::any_of(lower, upper, [](auto u) { return u; })) {
            return -1;
        }
        std::fill(lower, upper, true);
        return 0;
    };
    std::memset(scratch, 0, kPageSize);

    // Header(s) and cell pointers.
    if (account(0, cell_area_offset(*this))) {
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
    std::vector<U32> offsets;
    auto i = NodeHdr::get_free_start(hdr());
    const char *data = ref->page;
    while (i) {
        if (account(i, get_u16(data + i + kSlotWidth))) {
            return -1;
        }
        offsets.emplace_back(i);
        i = get_u16(data + i);
    }
    const auto offsets_copy = offsets;
    std::sort(begin(offsets), end(offsets));
    if (offsets != offsets_copy) {
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
        total_bytes += scratch[i] != '\x00';
    }
    return -(total_bytes != kPageSize);
}

auto Node::assert_state() const -> bool
{
    char scratch[kPageSize];
    CALICODB_EXPECT_EQ(0, validate(scratch));
    return true;
}

} // namespace calicodb