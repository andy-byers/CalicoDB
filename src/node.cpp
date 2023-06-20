// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#include "node.h"

namespace calicodb
{

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
    return cell_slots_offset(node) + NodeHdr::get_cell_count(node.hdr()) * sizeof(U16);
}

[[nodiscard]] static auto get_ivec_slot(const Node &node, std::size_t index)
{
    CALICODB_EXPECT_LT(index, NodeHdr::get_cell_count(node.hdr()));
    return get_u16(node.ref->page + cell_slots_offset(node) + index * sizeof(U16));
}

static auto put_ivec_slot(Node &node, std::size_t index, std::size_t pointer)
{
    CALICODB_EXPECT_LT(index, NodeHdr::get_cell_count(node.hdr()));
    return put_u16(node.ref->page + cell_slots_offset(node) + index * sizeof(U16), static_cast<U16>(pointer));
}

static auto insert_ivec_slot(Node &node, std::size_t index, std::size_t pointer)
{
    CALICODB_EXPECT_GE(node.gap_size, sizeof(U16));
    const auto count = NodeHdr::get_cell_count(node.hdr());
    CALICODB_EXPECT_LE(index, count);
    const auto offset = cell_slots_offset(node) + index * sizeof(U16);
    const auto size = (count - index) * sizeof(U16);
    auto *data = node.ref->page + offset;

    std::memmove(data + sizeof(U16), data, size);
    put_u16(data, static_cast<U16>(pointer));

    node.gap_size -= sizeof(U16);
    NodeHdr::put_cell_count(node.hdr(), count + 1);
}

static auto remove_ivec_slot(Node &node, std::size_t index)
{
    const auto count = NodeHdr::get_cell_count(node.hdr());
    CALICODB_EXPECT_LT(index, count);
    const auto offset = cell_slots_offset(node) + index * sizeof(U16);
    const auto size = (count - index) * sizeof(U16);
    auto *data = node.ref->page + offset;

    std::memmove(data, data + sizeof(U16), size);

    node.gap_size += sizeof(U16);
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
    const auto header_size = static_cast<std::uintptr_t>(ptr - data);
    const auto local_pl_size = compute_local_pl_size(key_size, value_size);
    const auto has_remote = local_pl_size < key_size + value_size;
    const auto footprint = header_size + local_pl_size + has_remote * sizeof(U32);

    if (data + footprint <= limit) {
        if (cell_out) {
            cell_out->ptr = data;
            cell_out->key = data + header_size;
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
        const auto header_size = static_cast<std::uintptr_t>(ptr - data);
        const auto local_pl_size = compute_local_pl_size(key_size, 0);
        const auto has_remote = local_pl_size < key_size;
        const auto footprint = header_size + local_pl_size + has_remote * sizeof(U32);
        if (data + footprint <= limit) {
            if (cell_out) {
                cell_out->ptr = data;
                cell_out->key = data + header_size;
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

    if (size + sizeof(U16) > usable_space) {
        return 0;
    }

    // We don't have room to insert the cell pointer.
    if (gap_size < sizeof(U16)) {
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
        put_ivec_slot(*this, index, offset);
    }
    return offset;
}

[[nodiscard]] static auto get_next_pointer(const Node &node, std::size_t offset) -> U32
{
    return get_u16(node.ref->page + offset);
}

[[nodiscard]] static auto get_block_size(const Node &node, std::size_t offset) -> U32
{
    return get_u16(node.ref->page + offset + sizeof(U16));
}

static auto set_next_pointer(Node &node, std::size_t offset, std::size_t value) -> void
{
    CALICODB_EXPECT_LT(value, kPageSize);
    return put_u16(node.ref->page + offset, static_cast<U16>(value));
}

static constexpr U32 kMinBlockSize = 2 * sizeof(U16);

static auto set_block_size(Node &node, std::size_t offset, std::size_t value) -> void
{
    CALICODB_EXPECT_GE(value, kMinBlockSize);
    CALICODB_EXPECT_LT(value, kPageSize);
    return put_u16(node.ref->page + offset + sizeof(U16), static_cast<U16>(value));
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
    // Largest possible fragment that can be reclaimed in this process. Based on
    // the fact that external cells must be at least 3 bytes. Internal cells are
    // always larger than fragments.
    const U32 fragment_cutoff = 2 + !node.is_leaf();
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
        if (before_end + fragment_cutoff >= block_start) {
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
        if (current_end + fragment_cutoff >= next) {
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
    for (std::size_t index = 0; index < n; ++index) {
        if (index != to_skip) {
            // Pack cells at the end of the scratch page and write the indirection
            // vector.
            Cell cell;
            if (node.read(index, cell)) {
                return -1;
            }
            end -= cell.footprint;
            std::memcpy(scratch + end, cell.ptr, cell.footprint);
            put_u16(scratch + cell_slots_offset(node) + index * sizeof(U16),
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
        usable_space -= cell.footprint + sizeof(U16);
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
        usable_space += cell_size + sizeof(U16);
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

auto Node::assert_state() -> bool
{
    bool used[kPageSize] = {};
    const auto account = [&used](auto from, auto size) {
        auto lower = used + long(from);
        auto upper = used + long(from) + long(size);
        CALICODB_EXPECT_TRUE(!std::any_of(lower, upper, [](auto u) {
            return u;
        }));
        std::fill(lower, upper, true);
    };
    // Header(s) and cell pointers.
    account(0, cell_area_offset(*this));
    // Make sure the header fields are not obviously wrong.
    CALICODB_EXPECT_TRUE(NodeHdr::get_frag_count(hdr()) < static_cast<U8>(-1) * 2);
    CALICODB_EXPECT_TRUE(NodeHdr::get_cell_count(hdr()) < static_cast<U16>(-1));
    CALICODB_EXPECT_TRUE(NodeHdr::get_free_start(hdr()) < static_cast<U16>(-1));

    // Gap space.
    account(cell_area_offset(*this), gap_size);

    // Free list blocks.
    std::vector<U32> offsets;
    auto i = NodeHdr::get_free_start(hdr());
    const char *data = ref->page;
    while (i) {
        const auto size = get_u16(data + i + sizeof(U16));
        account(i, size);
        offsets.emplace_back(i);
        i = get_u16(data + i);
    }
    const auto offsets_copy = offsets;
    std::sort(begin(offsets), end(offsets));
    CALICODB_EXPECT_EQ(offsets, offsets_copy);

    // Cell bodies. Also makes sure the cells are in order where possible.
    for (i = 0; i < NodeHdr::get_cell_count(hdr()); ++i) {
        const auto lhs_ptr = get_ivec_slot(*this, i);
        Cell lhs_cell = {};
        CALICODB_EXPECT_EQ(0, read(i, lhs_cell));
        CALICODB_EXPECT_TRUE(lhs_cell.footprint >= 3);
        account(lhs_ptr, lhs_cell.footprint);

        if (i + 1 < NodeHdr::get_cell_count(hdr())) {
            Cell rhs_cell = {};
            CALICODB_EXPECT_EQ(0, read(i + 1, rhs_cell));
            if (lhs_cell.local_pl_size == lhs_cell.total_pl_size &&
                rhs_cell.local_pl_size == rhs_cell.total_pl_size) {
                const Slice lhs_key(lhs_cell.key, lhs_cell.key_size);
                const Slice rhs_key(rhs_cell.key, rhs_cell.key_size);
                CALICODB_EXPECT_TRUE(lhs_key < rhs_key);
            }
        }
    }

    // Every byte should be accounted for, except for fragments.
    U32 total_bytes = NodeHdr::get_frag_count(hdr());
    for (auto c : used) {
        total_bytes += c != '\x00';
    }
    CALICODB_EXPECT_EQ(kPageSize, total_bytes);
    (void)total_bytes;
    return true;
}

} // namespace calicodb