// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#include "node.h"

namespace calicodb
{

namespace
{

constexpr uint32_t kSlotWidth = sizeof(uint16_t);
constexpr uint32_t kMinBlockSize = 2 * kSlotWidth;

[[nodiscard]] auto node_header_offset(const Node &node)
{
    return page_offset(node.ref->page_id);
}

[[nodiscard]] auto cell_slots_offset(const Node &node) -> uint32_t
{
    return node_header_offset(node) + NodeHdr::kSize;
}

[[nodiscard]] auto cell_area_offset(const Node &node) -> uint32_t
{
    return cell_slots_offset(node) + NodeHdr::get_cell_count(node.hdr()) * kSlotWidth;
}

[[nodiscard]] auto get_ivec_slot(const Node &node, uint32_t index) -> uint32_t
{
    // mask is used to make sure the offset returned by this function does not go off the end of a page due
    // to corruption. Callers must make sure that calls to std::memcpy() don't run off the page. put_u*() is
    // fine, however, since we keep some spillover space to account for it (e.g. put_u64(ptr, val) can be
    // called, where ptr points to the last byte on the page, without running into undefined behavior).
    const auto mask = static_cast<uint16_t>(node.total_space - 1);
    CALICODB_EXPECT_LT(index, NodeHdr::get_cell_count(node.hdr()));
    return mask & get_u16(node.ref->data + cell_slots_offset(node) + index * kSlotWidth);
}

auto put_ivec_slot(Node &node, uint32_t index, uint32_t slot)
{
    CALICODB_EXPECT_LT(index, NodeHdr::get_cell_count(node.hdr()));
    return put_u16(node.ref->data + cell_slots_offset(node) + index * kSlotWidth, static_cast<uint16_t>(slot));
}

auto insert_ivec_slot(Node &node, uint32_t index, uint32_t slot)
{
    CALICODB_EXPECT_GE(node.gap_size, kSlotWidth);
    const auto count = NodeHdr::get_cell_count(node.hdr());
    CALICODB_EXPECT_LE(index, count);
    const auto offset = cell_slots_offset(node) + index * kSlotWidth;
    const auto size = (count - index) * kSlotWidth;
    auto *data = node.ref->data + offset;

    std::memmove(data + kSlotWidth, data, size);
    put_u16(data, static_cast<uint16_t>(slot));

    node.gap_size -= kSlotWidth;
    NodeHdr::put_cell_count(node.hdr(), count + 1);
}

auto remove_ivec_slot(Node &node, uint32_t index)
{
    const auto count = NodeHdr::get_cell_count(node.hdr());
    CALICODB_EXPECT_LT(index, count);
    const auto offset = cell_slots_offset(node) + index * kSlotWidth;
    const auto size = (count - index) * kSlotWidth;
    auto *data = node.ref->data + offset;

    std::memmove(data, data + kSlotWidth, size);

    node.gap_size += kSlotWidth;
    NodeHdr::put_cell_count(node.hdr(), count - 1);
}

[[nodiscard]] auto external_parse_cell(char *data, const char *limit, uint32_t total_space, Cell &cell_out)
{
    uint32_t key_size, value_size;
    const auto *ptr = data;
    if (!(ptr = decode_varint(ptr, limit, value_size))) {
        return -1;
    }
    if (!(ptr = decode_varint(ptr, limit, key_size))) {
        return -1;
    }
    const auto hdr_size = static_cast<uintptr_t>(ptr - data);
    const auto pad_size = hdr_size > kMinCellHeaderSize ? 0 : kMinCellHeaderSize - hdr_size;
    const auto local_pl_size = compute_local_pl_size(key_size, value_size, total_space);
    const auto has_remote = local_pl_size < key_size + value_size;
    const auto footprint = hdr_size + pad_size + local_pl_size + has_remote * sizeof(uint32_t);

    if (data + footprint <= limit) {
        cell_out.ptr = data;
        cell_out.key = data + hdr_size + pad_size;
        cell_out.key_size = key_size;
        cell_out.total_pl_size = key_size + value_size;
        cell_out.local_pl_size = local_pl_size;
        cell_out.footprint = static_cast<uint32_t>(footprint);
        return 0;
    }
    return -1;
}

[[nodiscard]] auto internal_parse_cell(char *data, const char *limit, uint32_t total_space, Cell &cell_out)
{
    uint32_t key_size;
    if (const auto *ptr = decode_varint(data + sizeof(uint32_t), limit, key_size)) {
        const auto hdr_size = static_cast<uintptr_t>(ptr - data);
        const auto local_pl_size = compute_local_pl_size(key_size, 0, total_space);
        const auto has_remote = local_pl_size < key_size;
        const auto footprint = hdr_size + local_pl_size + has_remote * sizeof(uint32_t);
        if (data + footprint <= limit) {
            cell_out.ptr = data;
            cell_out.key = data + hdr_size;
            cell_out.key_size = key_size;
            cell_out.total_pl_size = key_size;
            cell_out.local_pl_size = local_pl_size;
            cell_out.footprint = static_cast<uint32_t>(footprint);
            return 0;
        }
    }
    return -1;
}

constexpr Node::ParseCell kParsers[2] = {
    internal_parse_cell,
    external_parse_cell,
};

[[nodiscard]] auto get_next_pointer(const Node &node, uint32_t offset) -> uint32_t
{
    return get_u16(node.ref->data + offset);
}

[[nodiscard]] auto get_block_size(const Node &node, uint32_t offset) -> uint32_t
{
    return get_u16(node.ref->data + offset + kSlotWidth);
}

auto set_next_pointer(Node &node, uint32_t offset, uint32_t value) -> void
{
    CALICODB_EXPECT_LT(value, node.total_space);
    return put_u16(node.ref->data + offset, static_cast<uint16_t>(value));
}

auto set_block_size(Node &node, uint32_t offset, uint32_t value) -> void
{
    CALICODB_EXPECT_GE(value, kMinBlockSize);
    CALICODB_EXPECT_LT(value, node.total_space);
    return put_u16(node.ref->data + offset + kSlotWidth, static_cast<uint16_t>(value));
}

auto take_free_space(Node &node, uint32_t ptr0, uint32_t ptr1, uint32_t needed_size) -> uint32_t
{
    CALICODB_EXPECT_LE(ptr1 + kMinBlockSize, node.total_space);
    CALICODB_EXPECT_LT(ptr0 + kMinBlockSize, ptr1);
    CALICODB_EXPECT_LE(needed_size, node.usable_space);

    const auto ptr2 = get_next_pointer(node, ptr1);
    const auto block_len = get_block_size(node, ptr1);

    CALICODB_EXPECT_GE(block_len, needed_size);
    const auto leftover_len = block_len - needed_size;

    if (leftover_len < kMinBlockSize) {
        const auto frag_count = NodeHdr::get_frag_count(node.hdr());
        NodeHdr::put_frag_count(node.hdr(), frag_count + leftover_len);
        if (ptr0 == 0) {
            NodeHdr::put_free_start(node.hdr(), ptr2);
        } else {
            set_next_pointer(node, ptr0, ptr2);
        }
    } else {
        set_block_size(node, ptr1, leftover_len);
    }
    return ptr1 + leftover_len;
}

auto allocate_from_freelist(Node &node, uint32_t needed_size) -> uint32_t
{
    auto block_ofs = NodeHdr::get_free_start(node.hdr());
    uint32_t prev_ofs = 0;

    while (block_ofs) {
        // Ensured by BlockAllocator::freelist_size(), which is called each time a node is
        // created from a page.
        CALICODB_EXPECT_LE(prev_ofs + kMinBlockSize, block_ofs);
        const auto block_len = get_block_size(node, block_ofs);
        if (needed_size <= block_len) {
            return take_free_space(node, prev_ofs, block_ofs, needed_size);
        }
        prev_ofs = block_ofs;
        block_ofs = get_next_pointer(node, block_ofs);
    }
    return 0;
}

auto allocate_from_gap(Node &node, uint32_t needed_size) -> uint32_t
{
    if (node.gap_size >= needed_size) {
        node.gap_size -= needed_size;
        const auto offset = NodeHdr::get_cell_start(node.hdr()) - needed_size;
        NodeHdr::put_cell_start(node.hdr(), offset);
        return offset;
    }
    return 0U;
}

} // namespace

auto Node::alloc(uint32_t index, uint32_t size) -> int
{
    CALICODB_EXPECT_LE(index, NodeHdr::get_cell_count(hdr()));

    if (size + kSlotWidth > usable_space) {
        return 0;
    }

    if (gap_size < kSlotWidth) {
        // We don't have room in the gap to insert the cell pointer.
        if (defrag()) {
            return -1;
        }
    }
    // Insert a dummy cell pointer to save the slot.
    insert_ivec_slot(*this, index, total_space - 1);

    // Attempt to allocate `size` contiguous bytes within `node`.
    uint32_t offset = 0;
    if (NodeHdr::get_frag_count(hdr()) + kMinBlockSize - 1 <= kMaxFragCount) {
        offset = BlockAllocator::allocate(*this, size);
    }
    if (offset == 0) {
        // There is enough space in `node`, it just isn't contiguous. Either that, or there
        // is a chance the fragment count might overflow. Defragment and try again. Note that
        // we pass `index` so that defragment() skips the cell we haven't filled in yet.
        if (BlockAllocator::defragment(*this, static_cast<int>(index))) {
            return -1;
        }
        offset = BlockAllocator::allocate(*this, size);
    }
    CALICODB_EXPECT_LE(offset + size, total_space);
    // We already made sure we had enough room to fulfill the request. If we had to defragment,
    // the call to allocate() should succeed.
    CALICODB_EXPECT_GT(offset, 0);
    put_ivec_slot(*this, index, static_cast<uint32_t>(offset));
    usable_space -= size + kSlotWidth;
    return static_cast<int>(offset);
}

auto BlockAllocator::allocate(Node &node, uint32_t needed_size) -> uint32_t
{
    CALICODB_EXPECT_LT(needed_size, node.total_space);
    if (const auto offset = allocate_from_gap(node, needed_size)) {
        return offset;
    }
    return allocate_from_freelist(node, needed_size);
}

auto BlockAllocator::release(Node &node, uint32_t block_ofs, uint32_t block_len) -> int
{
    // Largest possible fragment that can be reclaimed in this process. All cell headers
    // are padded out to 4 bytes, so anything smaller must be a fragment.
    static constexpr uint32_t kFragmentCutoff = 3;
    const auto frag_count = NodeHdr::get_frag_count(node.hdr());
    auto free_start = NodeHdr::get_free_start(node.hdr());
    uint32_t frag_diff = 0;

    CALICODB_EXPECT_GE(block_ofs, NodeHdr::get_cell_start(node.hdr()));
    CALICODB_EXPECT_LE(block_ofs + block_len, node.total_space);
    CALICODB_EXPECT_NE(block_len, 0);

    // Blocks of less than kMinBlockSize bytes are too small to hold the free block header,
    // so they must become fragment bytes.
    if (block_len < kMinBlockSize) {
        if (frag_count + block_len <= Node::kMaxFragCount) {
            NodeHdr::put_frag_count(node.hdr(), frag_count + block_len);
            return 0;
        }
        return defragment(node);
    }
    // The free block list is sorted by start position. Find where the
    // new block should go.
    uint32_t prev = 0;
    auto next = free_start;
    while (next && next < block_ofs) {
        prev = next;
        next = get_next_pointer(node, next);
        // This condition is ensured by freelist_size() and the logic below.
        CALICODB_EXPECT_TRUE(!next || prev + kMinBlockSize <= next);
    }

    if (prev != 0) {
        // Merge with the predecessor block.
        const auto before_end = prev + get_block_size(node, prev);
        if (before_end > block_ofs) {
            return -1;
        } else if (before_end + kFragmentCutoff >= block_ofs) {
            const auto diff = block_ofs - before_end;
            block_ofs = prev;
            block_len += get_block_size(node, prev) + diff;
            frag_diff += diff;
        }
    }
    if (block_ofs != prev) {
        // There was no left merge. Point the "before" pointer to where the new free
        // block will be inserted.
        if (prev == 0) {
            free_start = block_ofs;
        } else {
            set_next_pointer(node, prev, block_ofs);
        }
    }

    if (next != 0) {
        // Merge with the successor block.
        const auto current_end = block_ofs + block_len;
        if (current_end > next) {
            return -1;
        } else if (current_end + kFragmentCutoff >= next) {
            const auto diff = next - current_end;
            block_len += get_block_size(node, next) + diff;
            next = get_next_pointer(node, next);
            frag_diff += diff;
        }
    }
    // If there was a left merge, this will set the next pointer and block size of
    // the free block at "prev".
    set_next_pointer(node, block_ofs, next);
    set_block_size(node, block_ofs, block_len);
    NodeHdr::put_frag_count(node.hdr(), frag_count - frag_diff);
    NodeHdr::put_free_start(node.hdr(), free_start);
    return 0;
}

auto BlockAllocator::freelist_size(const Node &node, uint32_t total_space) -> int
{
    uint32_t total_len = 0;
    auto prev_end = NodeHdr::get_cell_start(node.hdr()) - kMinBlockSize;
    for (auto block_ofs = NodeHdr::get_free_start(node.hdr()); block_ofs;) {
        if (block_ofs + kMinBlockSize > total_space || // Free block header is out of bounds
            prev_end + kMinBlockSize > block_ofs) {    // Out-of-order blocks or missed fragment
            return -1;
        }
        const auto block_len = get_block_size(node, block_ofs);
        const auto next_ofs = get_next_pointer(node, block_ofs);
        if (block_ofs + block_len > total_space) { // Free block body is out of bounds
            return -1;
        }
        prev_end = block_ofs + block_len;
        block_ofs = next_ofs;
        total_len += block_len;
    }
    return static_cast<int>(total_len);
}

auto BlockAllocator::defragment(Node &node, int skip) -> int
{
    const auto n = NodeHdr::get_cell_count(node.hdr());
    const auto cell_start = NodeHdr::get_cell_start(node.hdr());
    const auto to_skip = skip >= 0 ? static_cast<uint32_t>(skip) : n;
    auto *ptr = node.ref->data;
    uint32_t end = node.total_space;

    // Copy everything before the indirection vector.
    std::memcpy(node.scratch, ptr, cell_slots_offset(node));
    for (uint32_t index = 0; index < n; ++index) {
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
                    static_cast<uint16_t>(end));
        }
    }
    std::memcpy(ptr, node.scratch, node.total_space);

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

auto Node::from_existing_page(PageRef &page, uint32_t total_space, char *scratch, Node &node_out) -> int
{
    const auto max_cell_count = (total_space - NodeHdr::kSize) /
                                (kMinCellHeaderSize + kSlotWidth);
    const auto hdr_offset = page_offset(page.page_id);
    const auto *hdr = page.data + hdr_offset;
    const auto type = NodeHdr::get_type(hdr);
    if (type == NodeHdr::kInvalid) {
        return -1;
    }
    const auto ncells = NodeHdr::get_cell_count(hdr);
    if (ncells > max_cell_count) {
        return -1;
    }
    const auto gap_upper = NodeHdr::get_cell_start(hdr);
    const auto gap_lower = hdr_offset + NodeHdr::kSize + ncells * kSlotWidth;
    if (gap_upper < gap_lower || gap_upper > total_space) {
        return -1;
    }
    Node node;
    node.ref = &page;

    const auto total_freelist_bytes = BlockAllocator::freelist_size(node, total_space);
    if (total_freelist_bytes < 0) {
        return -1;
    }
    node.scratch = scratch;
    node.total_space = total_space;
    node.parser = kParsers[type - NodeHdr::kInternal];
    node.gap_size = gap_upper - gap_lower;
    node.usable_space = node.gap_size +
                        static_cast<uint32_t>(total_freelist_bytes) +
                        NodeHdr::get_frag_count(hdr);
    if (node.usable_space > total_space) {
        return -1;
    }
    node_out = move(node);
    return 0;
}

auto Node::from_new_page(PageRef &page, uint32_t total_space, char *scratch, bool is_leaf) -> Node
{
    Node node;
    node.ref = &page;
    node.scratch = scratch;
    node.total_space = total_space;
    node.parser = kParsers[is_leaf];

    const auto usable_space = static_cast<uint32_t>(
        total_space - cell_slots_offset(node));
    node.gap_size = usable_space;
    node.usable_space = usable_space;

    std::memset(node.hdr(), 0, NodeHdr::kSize);
    NodeHdr::put_cell_start(node.hdr(), total_space);
    NodeHdr::put_type(node.hdr(), is_leaf);
    return node;
}

auto Node::read_child_id(uint32_t idx) const -> Id
{
    CALICODB_EXPECT_FALSE(is_leaf());
    if (idx >= NodeHdr::get_cell_count(hdr())) {
        return NodeHdr::get_next_id(hdr());
    }
    return Id(get_u32(ref->data + get_ivec_slot(*this, idx)));
}

auto Node::write_child_id(uint32_t idx, Id child_id) -> void
{
    CALICODB_EXPECT_FALSE(is_leaf());
    if (idx >= NodeHdr::get_cell_count(hdr())) {
        NodeHdr::put_next_id(hdr(), child_id);
    } else {
        put_u32(ref->data + get_ivec_slot(*this, idx), child_id.value);
    }
}

auto Node::read(uint32_t index, Cell &cell_out) const -> int
{
    const auto offset = get_ivec_slot(*this, index);
    if (offset < NodeHdr::get_cell_start(hdr())) {
        // NOTE: parser() checks the upper boundary.
        return -1;
    }
    return parser(
        ref->data + offset,
        ref->data + total_space,
        total_space,
        cell_out);
}

auto Node::write(uint32_t index, const Cell &cell) -> int
{
    const auto offset = alloc(index, cell.footprint);
    if (offset > 0) {
        std::memcpy(ref->data + offset, cell.ptr, cell.footprint);
    }
    return offset;
}

auto Node::erase(uint32_t index, uint32_t cell_size) -> int
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

auto Node::check_state() const -> int
{
    const auto account = [this](auto from, auto size) {
        if (from > total_space || from + size > total_space) {
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
    std::memset(scratch, 0, total_space);

    // Header(s) and cell pointers.
    if (account(0U, cell_area_offset(*this))) {
        return -1;
    }
    // Make sure the header fields are not obviously wrong.
    if (Node::kMaxFragCount < NodeHdr::get_frag_count(hdr()) ||
        total_space / 2 < NodeHdr::get_cell_count(hdr()) ||
        total_space < NodeHdr::get_free_start(hdr())) {
        return -1;
    }

    // Gap space.
    if (account(cell_area_offset(*this), gap_size)) {
        return -1;
    }

    // Free list blocks.
    auto i = NodeHdr::get_free_start(hdr());
    const char *data = ref->data;
    while (i) {
        if (account(i, get_u16(data + i + kSlotWidth))) {
            return -1;
        }
        i = get_u16(data + i);
    }

    if (kMaxFragCount < NodeHdr::get_frag_count(hdr())) {
        return -1;
    }

    // Cell bodies. Also makes sure the cells are in order where possible.
    for (i = 0; i < NodeHdr::get_cell_count(hdr()); ++i) {
        Cell left_cell;
        if (auto ivec_slot = get_ivec_slot(*this, i);
            read(i, left_cell) ||
            account(ivec_slot, left_cell.footprint)) {
            return -1;
        }

        if (i + 1 < NodeHdr::get_cell_count(hdr())) {
            Cell right_cell;
            if (read(i + 1, right_cell)) {
                return -1;
            }
            const Slice left_local(left_cell.key, minval(left_cell.key_size, left_cell.local_pl_size));
            const Slice right_local(right_cell.key, minval(right_cell.key_size, right_cell.local_pl_size));
            const auto right_has_ovfl = right_cell.key_size > right_cell.local_pl_size;
            if (right_local < left_local || (left_local == right_local && !right_has_ovfl)) {
                return -1;
            }
        }
    }

    // Every byte should be accounted for, except for fragments.
    auto total_bytes = NodeHdr::get_frag_count(hdr());
    for (i = 0; i < total_space; ++i) {
        total_bytes += scratch[i] == '\x01';
    }
    return -(total_bytes != total_space);
}

auto Node::assert_state() const -> bool
{
    CALICODB_EXPECT_EQ(0, check_state());
    return true;
}

} // namespace calicodb