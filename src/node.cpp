// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#include "node.h"
#include "status_internal.h"

namespace calicodb
{

namespace
{

constexpr uint32_t kSlotWidth = sizeof(uint16_t);
constexpr uint32_t kMinBlockSize = 2 * kSlotWidth;

[[nodiscard]] auto node_header_offset(const Node &node)
{
    return page_offset(node.page_id());
}

[[nodiscard]] auto ivec_offset(const Node &node) -> uint32_t
{
    return node_header_offset(node) + NodeHdr::size(node.is_leaf());
}

[[nodiscard]] auto gap_offset(const Node &node) -> uint32_t
{
    return ivec_offset(node) + node.cell_count() * kSlotWidth;
}

[[nodiscard]] auto get_ivec_slot(const Node &node, uint32_t index) -> uint32_t
{
    // mask is used to make sure the offset returned by this function does not go off the end of a page due
    // to corruption. Callers must make sure that calls to std::memcpy() don't run off the page. put_u*() is
    // fine, however, since we keep some spillover space to account for it (e.g. put_u64(ptr, val) can be
    // called, where ptr points to the last byte on the page, without running into undefined behavior).
    const auto mask = static_cast<uint16_t>(node.total_space - 1);
    CALICODB_EXPECT_LT(index, node.cell_count());
    return mask & get_u16(node.ref->data + ivec_offset(node) + index * kSlotWidth);
}

auto put_ivec_slot(Node &node, uint32_t index, uint32_t slot)
{
    CALICODB_EXPECT_LT(index, node.cell_count());
    return put_u16(node.ref->data + ivec_offset(node) + index * kSlotWidth, static_cast<uint16_t>(slot));
}

auto insert_ivec_slot(Node &node, uint32_t index, uint32_t slot)
{
    CALICODB_EXPECT_GE(node.gap_size, kSlotWidth);
    const auto count = node.cell_count();
    CALICODB_EXPECT_LE(index, count);
    const auto offset = ivec_offset(node) + index * kSlotWidth;
    const auto size = (count - index) * kSlotWidth;
    auto *data = node.ref->data + offset;

    std::memmove(data + kSlotWidth, data, size);
    put_u16(data, static_cast<uint16_t>(slot));

    node.gap_size -= kSlotWidth;
    NodeHdr::put_cell_count(node.hdr(), count + 1);
}

auto remove_ivec_slot(Node &node, uint32_t index)
{
    const auto count = node.cell_count();
    CALICODB_EXPECT_LT(index, count);
    const auto offset = ivec_offset(node) + index * kSlotWidth;
    const auto size = (count - index) * kSlotWidth;
    auto *data = node.ref->data + offset;

    std::memmove(data, data + kSlotWidth, size);

    node.gap_size += kSlotWidth;
    NodeHdr::put_cell_count(node.hdr(), count - 1);
}

[[nodiscard]] auto external_parse_cell(char *data, const char *limit, uint32_t min_local, uint32_t max_local, Cell &cell_out)
{
    SizeWithFlag swf;
    const auto *ptr = decode_size_with_flag(data, limit, swf);
    if (!ptr) {
        return -1;
    }
    uint32_t key_size;
    uint32_t value_size = 0;
    if (swf.flag) {
        key_size = swf.size;
        ptr += sizeof(uint32_t);
    } else if ((ptr = decode_varint(ptr, limit, key_size))) {
        value_size = swf.size;
    } else {
        return -1;
    }
    const auto hdr_size = static_cast<uintptr_t>(ptr - data);
    const auto pad_size = hdr_size > kMinCellHeaderSize ? 0 : kMinCellHeaderSize - hdr_size;
    // Note that the root_id in a bucket cell is considered part of the header.
    const auto [k, v, o] = describe_leaf_payload(key_size, value_size, swf.flag,
                                                 min_local, max_local);
    const auto footprint = hdr_size + pad_size + k + v + o;

    if (data + footprint <= limit) {
        cell_out.ptr = data;
        cell_out.key = data + hdr_size + pad_size;
        cell_out.key_size = key_size;
        cell_out.total_size = key_size + value_size;
        cell_out.local_size = k + v;
        cell_out.footprint = static_cast<uint32_t>(footprint);
        cell_out.is_bucket = swf.flag;
        return 0;
    }
    return -1;
}

[[nodiscard]] auto internal_parse_cell(char *data, const char *limit, uint32_t min_local, uint32_t max_local, Cell &cell_out)
{
    uint32_t key_size;
    if (const auto *ptr = decode_varint(data + sizeof(uint32_t), limit, key_size)) {
        const auto hdr_size = static_cast<uintptr_t>(ptr - data);
        const auto [k, _, o] = describe_branch_payload(key_size, min_local, max_local);
        const auto footprint = hdr_size + k + o;
        if (data + footprint <= limit) {
            cell_out.ptr = data;
            cell_out.key = data + hdr_size;
            cell_out.key_size = key_size;
            cell_out.total_size = key_size;
            cell_out.local_size = k;
            cell_out.footprint = static_cast<uint32_t>(footprint);
            cell_out.is_bucket = false;
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

[[nodiscard]] constexpr auto min_local_payload_size(uint32_t total_space) -> uint32_t
{
    return static_cast<uint32_t>((total_space - NodeHdr::size(false)) * 32 / 256 -
                                 kMaxCellHeaderSize - kSlotWidth);
}

[[nodiscard]] constexpr auto max_local_payload_size(uint32_t total_space) -> uint32_t
{
    return static_cast<uint32_t>((total_space - NodeHdr::size(false)) * 64 / 256 -
                                 kMaxCellHeaderSize - kSlotWidth);
}

[[nodiscard]] constexpr auto min_leaf_payload_size(uint32_t total_space) -> uint32_t
{
    return min_local_payload_size(total_space);
}

[[nodiscard]] constexpr auto max_leaf_payload_size(uint32_t total_space) -> uint32_t
{
    return static_cast<uint32_t>((total_space - NodeHdr::size(true)) * 128 / 256 -
                                 kMaxCellHeaderSize - kSlotWidth);
}

} // namespace

auto encode_size_with_flag(const SizeWithFlag &swf, char *output) -> char *
{
    // Sizes stored with an extra flag bit must not use the most-significant bit.
    CALICODB_EXPECT_EQ(swf.size & 0x80000000U, 0);
    const auto value = swf.size << 1 | swf.flag;
    return encode_varint(output, value);
}

auto decode_size_with_flag(const char *input, const char *limit, SizeWithFlag &swf_out) -> const char *
{
    uint32_t value;
    input = decode_varint(input, limit, value);
    if (input) {
        swf_out.size = value >> 1;
        swf_out.flag = value & 1;
    }
    return input;
}

static_assert(kMaxAllocation < 0x80000000U);

auto read_bucket_root_id(const Cell &cell) -> Id
{
    CALICODB_EXPECT_TRUE(cell.is_bucket);
    return Id(get_u32(cell.key - sizeof(uint32_t)));
}

auto write_bucket_root_id(Cell &cell, Id root_id) -> void
{
    CALICODB_EXPECT_TRUE(cell.is_bucket);
    put_u32(cell.key - sizeof(uint32_t), root_id.value);
}

auto write_bucket_root_id(char *key, const Slice &root_id) -> void
{
    // root_id is already encoded. Caller must have called put_u32(ptr, val) at some
    // point, where ptr is equal to root_id.ptr(), and val is a 4-byte root ID.
    CALICODB_EXPECT_EQ(root_id.size(), sizeof(uint32_t));
    std::memcpy(key - sizeof(uint32_t), root_id.data(), sizeof(uint32_t));
}

auto encode_branch_record_cell_hdr(char *output, uint32_t key_size, Id child_id) -> char *
{
    put_u32(output, child_id.value);
    return encode_varint(output + sizeof(uint32_t), key_size);
}

auto encode_leaf_record_cell_hdr(char *output, uint32_t key_size, uint32_t value_size) -> char *
{
    const auto *begin = output;
    const SizeWithFlag swf = {value_size, false};
    output = encode_size_with_flag(swf, output);
    output = encode_varint(output, key_size);
    const auto hdr_size = static_cast<uintptr_t>(output - begin);
    const auto pad_size = hdr_size > kMinCellHeaderSize ? 0 : kMinCellHeaderSize - hdr_size;
    // External cell headers are padded out to 4 bytes.
    std::memset(output, 0, pad_size);
    return output + pad_size;
}

auto prepare_bucket_cell_hdr(char *output, uint32_t key_size) -> char *
{
    const SizeWithFlag swf = {key_size, true};
    output = encode_size_with_flag(swf, output);
    output += sizeof(uint32_t); // Fill in the root_id later
    // Cell headers are padded out to 4 bytes. The 4-byte root_id is considered part of the
    // header, so no additional padding is required.
    return output;
}

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
    const auto n = node.cell_count();
    const auto cell_start = NodeHdr::get_cell_start(node.hdr());
    const auto to_skip = skip >= 0 ? static_cast<uint32_t>(skip) : n;
    auto *ptr = node.ref->data;
    uint32_t end = node.total_space;

    // Copy everything before the indirection vector.
    std::memcpy(node.scratch, ptr, ivec_offset(node));
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
            put_u16(node.scratch + ivec_offset(node) + index * kSlotWidth,
                    static_cast<uint16_t>(end));
        }
    }
    std::memcpy(ptr, node.scratch, node.total_space);

    NodeHdr::put_free_start(node.hdr(), 0);
    NodeHdr::put_frag_count(node.hdr(), 0);
    NodeHdr::put_cell_start(node.hdr(), end);
    node.gap_size = end - gap_offset(node);

    const auto gap_adjust = skip < 0 ? 0 : kSlotWidth;
    if (node.gap_size + gap_adjust != node.usable_space) {
        return -1;
    }
    return 0;
}

Node::Options::Options(uint32_t page_size, char *scratch)
    : scratch(scratch),
      total_space(page_size),
      min_local(min_local_payload_size(total_space)),
      max_local(max_local_payload_size(total_space)),
      min_leaf(min_leaf_payload_size(total_space)),
      max_leaf(max_leaf_payload_size(total_space))
{
}

#define MAX_CELL_COUNT(total_space, is_external) (((total_space)-NodeHdr::size(is_external)) / \
                                                  (kMinCellHeaderSize + kSlotWidth))

auto Node::from_existing_page(const Options &options, PageRef &page, Node &node_out) -> int
{
    const auto hdr_offset = page_offset(page.page_id);
    const auto *hdr = page.data + hdr_offset;
    const auto type = NodeHdr::get_type(hdr);
    if (type == NodeHdr::kInvalid) {
        return -1;
    }
    const auto is_external = type == NodeHdr::kExternal;
    const auto max_cell_count = MAX_CELL_COUNT(options.total_space, is_external);
    const auto ncells = NodeHdr::get_cell_count(hdr);
    if (ncells > max_cell_count) {
        return -1;
    }
    const auto gap_upper = NodeHdr::get_cell_start(hdr);
    const auto gap_lower = hdr_offset + NodeHdr::size(is_external) + ncells * kSlotWidth;
    if (gap_upper < gap_lower || gap_upper > options.total_space) {
        return -1;
    }
    Node node;
    node.ref = &page;

    const auto total_freelist_bytes = BlockAllocator::freelist_size(node, options.total_space);
    if (total_freelist_bytes < 0) {
        return -1;
    }
    node.scratch = options.scratch;
    node.total_space = options.total_space;
    node.parser = kParsers[type - NodeHdr::kInternal];
    node.gap_size = gap_upper - gap_lower;
    node.min_local = type == NodeHdr::kExternal ? options.min_leaf : options.min_local;
    node.max_local = type == NodeHdr::kExternal ? options.max_leaf : options.max_local;
    node.usable_space = node.gap_size +
                        static_cast<uint32_t>(total_freelist_bytes) +
                        NodeHdr::get_frag_count(hdr);
    if (node.usable_space > options.total_space) {
        return -1;
    }
    node_out = move(node);
    return 0;
}

auto Node::from_new_page(const Options &options, PageRef &page, bool is_leaf) -> Node
{
    Node node;
    node.ref = &page;
    node.scratch = options.scratch;
    node.total_space = options.total_space;
    node.parser = kParsers[is_leaf];
    node.min_local = is_leaf ? options.min_leaf : options.min_local;
    node.max_local = is_leaf ? options.max_leaf : options.max_local;

    std::memset(node.hdr(), 0, NodeHdr::size(is_leaf));
    NodeHdr::put_cell_start(node.hdr(), options.total_space);
    NodeHdr::put_type(node.hdr(), is_leaf);

    const auto usable_space = static_cast<uint32_t>(
        options.total_space - ivec_offset(node));
    node.gap_size = usable_space;
    node.usable_space = usable_space;
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
    if (index >= NodeHdr::get_cell_count(hdr())) {
        return -1;
    }
    const auto offset = get_ivec_slot(*this, index);
    if (offset < NodeHdr::get_cell_start(hdr())) {
        // NOTE: parser() checks the upper boundary.
        return -1;
    }
    return parser(
        ref->data + offset,
        ref->data + total_space,
        min_local,
        max_local,
        cell_out);
}

auto Node::insert(uint32_t index, const Cell &cell) -> int
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

auto Node::check_integrity() const -> Status
{
#define CORRUPTED_NODE(fmt, ...)                                 \
    StatusBuilder::corruption("tree node %u is corrupted: " fmt, \
                              page_id().value, __VA_ARGS__)

    const auto account = [this](auto from, auto size, const auto *name, auto &s) {
        if (from > total_space || from + size > total_space) {
            s = CORRUPTED_NODE("\"%s\" region at [%u,%u) is out of bounds",
                               name, from, from + size);
            return -1;
        }
        for (auto *ptr = scratch + from; ptr != scratch + from + size; ++ptr) {
            if (*ptr) {
                s = CORRUPTED_NODE("\"%s\" region at [%u,%u) overlaps occupied byte at %zu",
                                   name, from, from + size, static_cast<uintptr_t>(ptr - scratch - from));
                return -1;
            } else {
                *ptr = 1;
            }
        }
        return 0;
    };
    std::memset(scratch, 0, total_space);

    Status s;
    if (account(0U, gap_offset(*this), "header/indirection vector", s)) {
        return s;
    }
    // Make sure the header fields are not obviously wrong.
    if (kMaxFragCount < NodeHdr::get_frag_count(hdr())) {
        return CORRUPTED_NODE("fragment count of %u exceeds maximum of %u",
                              NodeHdr::get_frag_count(hdr()), Node::kMaxFragCount);
    }
    if (MAX_CELL_COUNT(total_space, is_leaf()) < NodeHdr::get_cell_count(hdr())) {
        return CORRUPTED_NODE("cell count of %u exceeds maximum of %u",
                              NodeHdr::get_cell_count(hdr()), MAX_CELL_COUNT(total_space, is_leaf()));
    }
    // All free blocks are located in the cell content area, which must be between the end of the
    // indirection vector and the end of the page.
    const auto free_start = NodeHdr::get_free_start(hdr());
    const auto cell_start = NodeHdr::get_cell_start(hdr());
    if ((free_start && cell_start > free_start) || total_space < free_start) {
        return CORRUPTED_NODE("first free block at %u is out of bounds", free_start);
    }
    if (gap_offset(*this) > cell_start || total_space < cell_start) {
        return CORRUPTED_NODE("cell area offset at %u is out of bounds", cell_start);
    }

    if (account(gap_offset(*this), gap_size, "gap", s)) {
        return s;
    }

    auto i = NodeHdr::get_free_start(hdr());
    const char *data = ref->data;
    while (i) {
        if (account(i, get_u16(data + i + kSlotWidth), "free block", s)) {
            return s;
        }
        i = get_u16(data + i);
    }

    // Cell bodies. Also makes sure the cells are in order where possible.
    for (i = 0; i < NodeHdr::get_cell_count(hdr()); ++i) {
        Cell left_cell;
        auto ivec_slot = get_ivec_slot(*this, i);
        if (read(i, left_cell)) {
            return CORRUPTED_NODE("corruption detected in cell %u", i);
        }
        if (account(ivec_slot, left_cell.footprint, "cell", s)) {
            return s;
        }

        if (i + 1 < NodeHdr::get_cell_count(hdr())) {
            Cell right_cell;
            if (read(i + 1, right_cell)) {
                return CORRUPTED_NODE("corruption detected in cell %u", i + 1);
            }
            const Slice left_local(left_cell.key, minval(left_cell.key_size, left_cell.local_size));
            const Slice right_local(right_cell.key, minval(right_cell.key_size, right_cell.local_size));
            if (right_local < left_local) {
                return CORRUPTED_NODE("local keys for cells %u and %u are out of order", i, i + 1);
            }
        }
    }

    // Every byte should be accounted for, except for fragments.
    auto total_bytes = NodeHdr::get_frag_count(hdr());
    for (i = 0; i < total_space; ++i) {
        total_bytes += scratch[i] == '\x01';
    }
    if (total_bytes != total_space) {
        return CORRUPTED_NODE("unaccounted for bytes: found %u but expected %u",
                              total_bytes, total_space);
    }
    return Status::ok();
}

auto Node::assert_integrity() const -> bool
{
    const auto s = check_integrity();
    return s.is_ok();
}

} // namespace calicodb