// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#include "tree.h"
#include "db_impl.h"
#include "encoding.h"
#include "freelist.h"
#include "logging.h"
#include "pager.h"
#include "schema.h"
#include "scope_guard.h"
#include "utils.h"
#include <array>
#include <functional>
#include <numeric>

namespace calicodb
{

static constexpr auto kMaxCellHeaderSize =
    kVarintMaxLength + // Value size  (10 B)
    kVarintMaxLength + // Key size    (10 B)
    sizeof(U32);       // Overflow ID (4 B)

static constexpr U32 kPointerSize = sizeof(U16);

// Determine how many bytes of payload can be stored locally (not on an overflow chain)
[[nodiscard]] static constexpr auto compute_local_pl_size(std::size_t key_size, std::size_t value_size) -> U32
{
    // SQLite's computation for min and max local payload sizes. If kMaxLocal is exceeded, then 1 or more
    // overflow chain pages will be required to store this payload.
    constexpr const std::size_t kMinLocal =
        (kPageSize - NodeHdr::kSize) * 32 / 256 - kMaxCellHeaderSize - kPointerSize;
    constexpr const std::size_t kMaxLocal =
        (kPageSize - NodeHdr::kSize) * 64 / 256 - kMaxCellHeaderSize - kPointerSize;
    if (key_size + value_size <= kMaxLocal) {
        // The whole payload can be stored locally.
        return key_size + value_size;
    } else if (key_size > kMaxLocal) {
        // The first part of the key will occupy the entire local payload.
        return kMaxLocal;
    }
    // Try to prevent the key from being split.
    return std::max(kMinLocal, key_size);
}

auto Tree::corrupted_page(Id page_id) const -> Status
{
    std::string msg;
    append_fmt_string(msg, "corruption detected (root=%u, page=%u)", root().value, page_id.value);
    auto s = Status::corruption(msg);
    m_pager->set_status(s);
    return s;
}

[[nodiscard]] static auto page_offset(Id page_id) -> U32
{
    return FileHdr::kSize * page_id.is_root();
}

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
    return cell_slots_offset(node) + node.hdr.cell_count * kPointerSize;
}

[[nodiscard]] static auto read_next_id(const PageRef &page) -> Id
{
    return Id(get_u32(page.page + page_offset(page.page_id)));
}

static auto write_next_id(PageRef &page, Id next_id) -> void
{
    put_u32(page.page + page_offset(page.page_id), next_id.value);
}

// TODO: May want to cache the total number of free block bytes.
[[nodiscard]] static auto usable_space(const Node &node)
{
    // Number of bytes not occupied by cells or cell pointers.
    return node.gap_size + node.hdr.frag_count + BlockAllocator::accumulate_free_bytes(node);
}

[[nodiscard]] static auto node_get_slot(const Node &node, std::size_t index)
{
    CALICODB_EXPECT_LT(index, node.hdr.cell_count);
    return get_u16(node.ref->page + node.slots_offset + index * kPointerSize);
}

static auto node_set_slot(Node &node, std::size_t index, std::size_t pointer)
{
    CALICODB_EXPECT_LT(index, node.hdr.cell_count);
    return put_u16(node.ref->page + node.slots_offset + index * kPointerSize, static_cast<U16>(pointer));
}

static auto node_insert_slot(Node &node, std::size_t index, std::size_t pointer)
{
    CALICODB_EXPECT_LE(index, node.hdr.cell_count);
    CALICODB_EXPECT_GE(node.gap_size, kPointerSize);
    const auto offset = node.slots_offset + index * kPointerSize;
    const auto size = (node.hdr.cell_count - index) * kPointerSize;
    auto *data = node.ref->page + offset;

    std::memmove(data + kPointerSize, data, size);
    put_u16(data, static_cast<U16>(pointer));

    node.gap_size -= static_cast<unsigned>(kPointerSize);
    ++node.hdr.cell_count;
}

static auto node_remove_slot(Node &node, std::size_t index)
{
    CALICODB_EXPECT_LT(index, node.hdr.cell_count);
    const auto offset = node.slots_offset + index * kPointerSize;
    const auto size = (node.hdr.cell_count - index) * kPointerSize;
    auto *data = node.ref->page + offset;

    std::memmove(data, data + kPointerSize, size);

    node.gap_size += kPointerSize;
    --node.hdr.cell_count;
}

static auto detach_cell(Cell &cell, char *backing)
{
    if (backing && cell.ptr != backing) {
        std::memcpy(backing, cell.ptr, cell.footprint);
        const auto diff = cell.key - cell.ptr;
        cell.ptr = backing;
        cell.key = backing + diff;
    }
}

[[nodiscard]] static auto read_child_id_at(const Node &node, std::size_t offset)
{
    return Id(get_u32(node.ref->page + offset));
}

static auto write_child_id_at(Node &node, std::size_t offset, Id child_id)
{
    put_u32(node.ref->page + offset, child_id.value);
}

[[nodiscard]] static auto read_child_id(const Node &node, std::size_t index)
{
    const auto &header = node.hdr;
    CALICODB_EXPECT_LE(index, header.cell_count);
    CALICODB_EXPECT_FALSE(header.is_external);
    if (index == header.cell_count) {
        return header.next_id;
    }
    return read_child_id_at(node, node_get_slot(node, index));
}

[[nodiscard]] static auto read_child_id(const Cell &cell)
{
    return Id(get_u32(cell.ptr));
}

[[nodiscard]] static auto read_overflow_id(const Cell &cell)
{
    return Id(get_u32(cell.key + cell.local_pl_size));
}

static auto write_overflow_id(Cell &cell, Id overflow_id)
{
    put_u32(cell.key + cell.local_pl_size, overflow_id.value);
}

static auto write_child_id(Node &node, std::size_t index, Id child_id)
{
    auto &header = node.hdr;
    CALICODB_EXPECT_LE(index, header.cell_count);
    CALICODB_EXPECT_FALSE(header.is_external);
    if (index == header.cell_count) {
        header.next_id = child_id;
    } else {
        write_child_id_at(node, node_get_slot(node, index), child_id);
    }
}

static auto write_child_id(Cell &cell, Id child_id)
{
    put_u32(cell.ptr, child_id.value);
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

[[nodiscard]] static auto merge_root(Node &root, Node &child)
{
    CALICODB_EXPECT_EQ(root.hdr.next_id, child.ref->page_id);
    const auto &header = child.hdr;
    if (header.free_start != 0) {
        if (BlockAllocator::defragment(child)) {
            return -1;
        }
    }

    // Copy the cell content area.
    CALICODB_EXPECT_GE(header.cell_start, cell_slots_offset(root));
    auto memory_size = kPageSize - header.cell_start;
    auto *memory = root.ref->page + header.cell_start;
    std::memcpy(memory, child.ref->page + header.cell_start, memory_size);

    // Copy the header and cell pointers.
    memory_size = header.cell_count * kPointerSize;
    memory = root.ref->page + cell_slots_offset(root);
    std::memcpy(memory, child.ref->page + cell_slots_offset(child), memory_size);
    root.hdr = header;
    root.parser = child.parser;
    return 0;
}

[[nodiscard]] static auto is_overflowing(const Node &node)
{
    return node.overflow.has_value();
}

[[nodiscard]] static auto is_underflowing(const Node &node)
{
    return node.hdr.cell_count == 0;
}

static auto allocate_block(Node &node, unsigned index, unsigned size) -> int
{
    CALICODB_EXPECT_LE(index, node.hdr.cell_count);

    if (size + kPointerSize > usable_space(node)) {
        node.overflow_index = index;
        return 0;
    }

    // We don't have room to insert the cell pointer.
    if (node.gap_size < kPointerSize) {
        if (BlockAllocator::defragment(node)) {
            return -1;
        }
    }
    // Insert a dummy cell pointer to save the slot.
    node_insert_slot(node, index, kPageSize - 1);

    auto offset = BlockAllocator::allocate(node, size);
    if (offset == 0) {
        if (BlockAllocator::defragment(node, static_cast<int>(index))) {
            return -1;
        }
        offset = BlockAllocator::allocate(node, size);
    }
    // We already made sure we had enough room to fulfill the request. If we had to defragment, the call
    // to allocate() following defragmentation should succeed.
    CALICODB_EXPECT_NE(offset, 0);
    node_set_slot(node, index, offset);

    return int(offset); // TODO: Return an int from BlockAllocator::allocate() to report corruption? There are a few things we could detect...
}

static auto free_block(Node &node, unsigned index, unsigned size) -> void
{
    BlockAllocator::release(node, static_cast<unsigned>(node_get_slot(node, index)), size);
    node_remove_slot(node, index);
}

// Read a cell from a `node` at the specified `offset` or slot `index`
// The `node` must remain alive for as long as the cell. On success, stores the cell in `*cell_out` and returns
// 0. Returns a nonzero integer if the cell is determined to be corrupted.
[[nodiscard]] static auto read_cell_at(Node &node, std::size_t offset, Cell *cell_out)
{
    return node.parser(node.ref->page + offset, node.ref->page + kPageSize, cell_out);
}
[[nodiscard]] auto read_cell(Node &node, std::size_t index, Cell *cell_out) -> int
{
    return read_cell_at(node, node_get_slot(node, index), cell_out);
}

// Write a `cell` to a `node` at the specified `index`
// May defragment the `node`. The `cell` must be of the same type as the `node`, or if the `node` is internal,
// promote_cell() must have been called on the `cell`.
auto write_cell(Node &node, std::size_t index, const Cell &cell) -> std::size_t
{
    if (const auto offset = allocate_block(node, static_cast<unsigned>(index), static_cast<unsigned>(cell.footprint))) {
        std::memcpy(node.ref->page + offset, cell.ptr, cell.footprint);
        return std::size_t(offset); // TODO
    }
    node.overflow_index = static_cast<unsigned>(index);
    node.overflow = cell;
    return 0;
}

// Erase a cell from the `node` at the specified `index`
// The `cell_size` must be known beforehand (it may come from a parsed cell's "size" member).
auto erase_cell(Node &node, std::size_t index, std::size_t cell_size) -> void
{
    CALICODB_EXPECT_LT(index, node.hdr.cell_count);
    free_block(node, static_cast<unsigned>(index), static_cast<unsigned>(cell_size));
}

[[nodiscard]] static auto get_next_pointer(const Node &node, std::size_t offset) -> unsigned
{
    return get_u16(node.ref->page + offset);
}

[[nodiscard]] static auto get_block_size(const Node &node, std::size_t offset) -> unsigned
{
    return get_u16(node.ref->page + offset + kPointerSize);
}

static auto set_next_pointer(Node &node, std::size_t offset, std::size_t value) -> void
{
    CALICODB_EXPECT_LT(value, kPageSize);
    return put_u16(node.ref->page + offset, static_cast<U16>(value));
}

static auto set_block_size(Node &node, std::size_t offset, std::size_t value) -> void
{
    CALICODB_EXPECT_GE(value, 4);
    CALICODB_EXPECT_LT(value, kPageSize);
    return put_u16(node.ref->page + offset + kPointerSize, static_cast<U16>(value));
}

static auto take_free_space(Node &node, std::size_t ptr0, std::size_t ptr1, std::size_t needed_size) -> std::size_t
{
    CALICODB_EXPECT_LT(ptr0, kPageSize);
    CALICODB_EXPECT_LT(ptr1, kPageSize);
    CALICODB_EXPECT_LT(needed_size, kPageSize);

    const auto ptr2 = get_next_pointer(node, ptr1);
    const auto free_size = get_block_size(node, ptr1);
    auto &header = node.hdr;

    CALICODB_EXPECT_GE(free_size, needed_size);
    const auto diff = static_cast<unsigned>(free_size - needed_size);

    if (diff < 4) {
        header.frag_count = header.frag_count + diff;
        if (ptr0 == 0) {
            header.free_start = ptr2;
        } else {
            set_next_pointer(node, ptr0, ptr2);
        }
    } else {
        set_block_size(node, ptr1, diff);
    }
    return ptr1 + diff;
}

static auto allocate_from_freelist(Node &node, std::size_t needed_size) -> std::size_t
{
    unsigned prev_ptr = 0;
    auto curr_ptr = node.hdr.free_start;

    while (curr_ptr) {
        if (needed_size <= get_block_size(node, curr_ptr)) {
            return take_free_space(node, prev_ptr, curr_ptr, needed_size);
        }
        prev_ptr = curr_ptr;
        curr_ptr = get_next_pointer(node, curr_ptr);
    }
    return 0;
}

static auto allocate_from_gap(Node &node, std::size_t needed_size) -> std::size_t
{
    if (node.gap_size >= needed_size) {
        node.gap_size -= static_cast<unsigned>(needed_size);
        node.hdr.cell_start -= static_cast<unsigned>(needed_size);
        return node.hdr.cell_start;
    }
    return 0;
}

auto BlockAllocator::accumulate_free_bytes(const Node &node) -> std::size_t
{
    unsigned total = 0;
    for (auto ptr = node.hdr.free_start; ptr;) {
        total += get_block_size(node, ptr);
        ptr = get_next_pointer(node, ptr);
    }
    return total;
}

auto BlockAllocator::allocate(Node &node, std::size_t needed_size) -> std::size_t
{
    CALICODB_EXPECT_LT(needed_size, kPageSize);

    if (const auto offset = allocate_from_gap(node, needed_size)) {
        return offset;
    }
    return allocate_from_freelist(node, needed_size);
}

auto BlockAllocator::release(Node &node, std::size_t block_start, std::size_t block_size) -> void
{
    auto &header = node.hdr;

    // Largest possible fragment that can be reclaimed in this process. Based on
    // the fact that external cells must be at least 3 bytes. Internal cells are
    // always larger than fragments.
    const std::size_t fragment_cutoff = 2 + !header.is_external;
    CALICODB_EXPECT_NE(block_size, 0);

    // Blocks of less than 4 bytes are too small to hold the free block header,
    // so they must become fragment bytes.
    if (block_size < 4) {
        header.frag_count += static_cast<unsigned>(block_size);
        return;
    }
    // The free block list is sorted by start position. Find where the
    // new block should go.
    unsigned prev = 0;
    auto next = header.free_start;
    while (next && next < block_start) {
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
            header.frag_count -= static_cast<unsigned>(diff);
        }
    }
    if (block_start != prev) {
        // There was no left merge. Point the "before" pointer to where the new free
        // block will be inserted.
        if (prev == 0) {
            header.free_start = static_cast<unsigned>(block_start);
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
            header.frag_count -= static_cast<unsigned>(diff);
            next = get_next_pointer(node, next);
        }
    }
    // If there was a left merge, this will set the next pointer and block size of
    // the free block at "prev".
    set_next_pointer(node, block_start, next);
    set_block_size(node, block_start, block_size);
}

auto BlockAllocator::defragment(Node &node, int skip) -> int
{
    auto &header = node.hdr;
    const auto n = header.cell_count;
    const auto to_skip = skip >= 0 ? static_cast<std::size_t>(skip) : n;
    auto *ptr = node.ref->page;
    auto end = kPageSize;

    // Copy everything before the indirection vector.
    std::memcpy(node.scratch, ptr, node.slots_offset);
    for (std::size_t index = 0; index < n; ++index) {
        if (index != to_skip) {
            // Pack cells at the end of the scratch page and write the indirection
            // vector.
            Cell cell;
            if (read_cell(node, index, &cell)) {
                return -1;
            }
            end -= cell.footprint;
            std::memcpy(node.scratch + end, cell.ptr, cell.footprint);
            put_u16(node.scratch + node.slots_offset + index * kPointerSize,
                    static_cast<U16>(end));
        }
    }
    std::memcpy(ptr, node.scratch, kPageSize);

    header.frag_count = 0;
    header.free_start = 0;
    header.cell_start = static_cast<unsigned>(end);
    node.gap_size = static_cast<unsigned>(end - cell_area_offset(node));
    return 0;
}

static auto setup_node(Node &node) -> void
{
    node.parser = node.hdr.is_external ? external_parse_cell : internal_parse_cell;
    node.slots_offset = static_cast<unsigned>(cell_slots_offset(node));
    node.overflow_index = 0;
    node.overflow.reset();

    if (node.hdr.cell_start == 0) {
        node.hdr.cell_start = static_cast<unsigned>(kPageSize);
    }

    const auto bottom = cell_area_offset(node);
    const auto top = node.hdr.cell_start;

    CALICODB_EXPECT_GE(top, bottom);
    node.gap_size = static_cast<unsigned>(top - bottom);
}

static constexpr U32 kLinkContentOffset = sizeof(U32);
static constexpr U32 kLinkContentSize = kPageSize - kLinkContentOffset;

struct PayloadManager {
    static auto promote(Pager &pager, char *scratch, Cell &cell, Id parent_id) -> Status
    {
        detach_cell(cell, scratch);

        const auto header_size = sizeof(U32) + varint_length(cell.key_size);

        // The buffer that `scratch` points into should have enough room before `scratch` to write
        // the left child ID.
        cell.ptr = cell.key - header_size;
        cell.local_pl_size = compute_local_pl_size(cell.key_size, 0);
        cell.total_pl_size = cell.key_size;
        cell.footprint = static_cast<U32>(header_size + cell.local_pl_size);

        Status s;
        if (cell.key_size > cell.local_pl_size) {
            // Part of the key is on an overflow page. No value is stored locally in this case, so
            // the local size computation is still correct. Copy the overflow key, page-by-page,
            // to a new overflow chain.
            Id ovfl_id;
            auto rest = cell.key_size - cell.local_pl_size;
            auto pgno = read_overflow_id(cell);
            PageRef *prev = nullptr;
            auto dst_type = PointerMap::kOverflowHead;
            auto dst_bptr = parent_id;
            while (s.is_ok() && rest > 0) {
                PageRef *src, *dst;
                // Allocate a new overflow page.
                s = pager.allocate(dst);
                if (!s.is_ok()) {
                    break;
                }
                // Acquire the old overflow page.
                s = pager.acquire(pgno, src);
                if (!s.is_ok()) {
                    pager.release(dst);
                    break;
                }
                const auto copy_size = std::min(rest, kLinkContentSize);
                std::memcpy(dst->page + kLinkContentOffset,
                            src->page + kLinkContentOffset,
                            copy_size);

                if (prev) {
                    put_u32(prev->page, dst->page_id.value);
                    pager.release(prev, Pager::kNoCache);
                } else {
                    write_overflow_id(cell, dst->page_id);
                }
                rest -= copy_size;
                dst_type = PointerMap::kOverflowLink;
                dst_bptr = dst->page_id;
                prev = dst;
                pgno = read_next_id(*src);
                pager.release(src, Pager::kNoCache);

                s = PointerMap::write_entry(
                    pager, dst->page_id, {dst_bptr, dst_type});
            }
            if (s.is_ok()) {
                CALICODB_EXPECT_NE(nullptr, prev);
                put_u32(prev->page, 0);
                cell.footprint += sizeof(U32);
            }
            pager.release(prev, Pager::kNoCache);
        }
        return s;
    }

    static auto access(
        Pager &pager,
        const Cell &cell,   // The `cell` containing the payload being accessed
        std::size_t offset, // `offset` within the payload being accessed
        std::size_t length, // Number of bytes to access
        const char *in_buf, // Write buffer of size at least `length` bytes, or nullptr if not a write
        char *out_buf       // Read buffer of size at least `length` bytes, or nullptr if not a read
        ) -> Status
    {
        CALICODB_EXPECT_TRUE(in_buf || out_buf);
        if (offset <= cell.local_pl_size) {
            const auto n = std::min(length, cell.local_pl_size - offset);
            if (in_buf) {
                std::memcpy(cell.key + offset, in_buf, n);
                in_buf += n;
            } else {
                std::memcpy(out_buf, cell.key + offset, n);
                out_buf += n;
            }
            length -= n;
            offset = 0;
        } else {
            offset -= cell.local_pl_size;
        }

        Status s;
        if (length) {
            auto pgno = read_overflow_id(cell);
            while (!pgno.is_null()) {
                PageRef *ovfl;
                s = pager.acquire(pgno, ovfl);
                if (!s.is_ok()) {
                    break;
                }
                std::size_t len;
                if (offset >= kLinkContentSize) {
                    offset -= kLinkContentSize;
                    len = 0;
                } else {
                    len = std::min(length, kLinkContentSize - offset);
                    if (in_buf) {
                        std::memcpy(ovfl->page + kLinkContentOffset + offset, in_buf, len);
                        in_buf += len;
                    } else {
                        std::memcpy(out_buf, ovfl->page + kLinkContentOffset + offset, len);
                        out_buf += len;
                    }
                    offset = 0;
                }
                pgno = read_next_id(*ovfl);
                pager.release(ovfl, Pager::kNoCache);
                length -= len;
                if (length == 0) {
                    break;
                }
            }
        }
        return s;
    }
};

auto Tree::create(Pager &pager, Id *root_id_out) -> Status
{
    Node node;
    auto s = pager.allocate(node.ref);
    if (!s.is_ok()) {
        return s;
    }
    node.hdr.is_external = true;
    setup_node(node);

    const auto root_id = node.ref->page_id;
    node.hdr.write(node.ref->page + node_header_offset(node));
    pager.release(node.ref);

    s = PointerMap::write_entry(
        pager,
        root_id,
        {Id::null(), PointerMap::kTreeRoot});
    if (root_id_out) {
        *root_id_out = s.is_ok() ? root_id : Id::null();
    }
    return s;
}

auto Tree::find_external(const Slice &key, bool &exact) const -> Status
{
    m_c.seek_root();
    exact = false;

    while (m_c.is_valid()) {
        const auto found = m_c.seek(key);
        if (m_c.is_valid()) {
            if (m_c.node().hdr.is_external) {
                exact = found;
                break;
            }
            const auto next_id = read_child_id(m_c.node(), m_c.index());
            CALICODB_EXPECT_NE(next_id, m_c.node().ref->page_id); // Infinite loop.
            m_c.move_down(next_id);
        }
    }
    return m_c.status();
}

auto Tree::read_key(Node &node, std::size_t index, std::string &scratch, Slice *key_out, std::size_t limit) const -> Status
{
    Cell cell;
    if (read_cell(node, index, &cell)) {
        return corrupted_page(node.ref->page_id);
    }
    return read_key(cell, scratch, key_out, limit);
}
auto Tree::read_key(const Cell &cell, std::string &scratch, Slice *key_out, std::size_t limit) const -> Status
{
    if (limit == 0 || limit > cell.key_size) {
        limit = cell.key_size;
    }
    if (scratch.size() < limit) {
        scratch.resize(limit);
    }
    auto s = PayloadManager::access(*m_pager, cell, 0, limit, nullptr, scratch.data());
    if (s.is_ok() && key_out) {
        *key_out = Slice(scratch).truncate(limit);
    }
    return s;
}

auto Tree::read_value(Node &node, std::size_t index, std::string &scratch, Slice *value_out) const -> Status
{
    Cell cell;
    if (read_cell(node, index, &cell)) {
        return corrupted_page(node.ref->page_id);
    }
    return read_value(cell, scratch, value_out);
}
auto Tree::read_value(const Cell &cell, std::string &scratch, Slice *value_out) const -> Status
{
    const auto value_size = cell.total_pl_size - cell.key_size;
    if (scratch.size() < value_size) {
        scratch.resize(value_size);
    }
    auto s = PayloadManager::access(*m_pager, cell, cell.key_size, value_size, nullptr, scratch.data());
    if (s.is_ok() && value_out) {
        *value_out = Slice(scratch).truncate(value_size);
    }
    return s;
}

auto Tree::write_key(Node &node, std::size_t index, const Slice &key) -> Status
{
    Cell cell;
    if (read_cell(node, index, &cell)) {
        return corrupted_page(node.ref->page_id);
    }
    return PayloadManager::access(*m_pager, cell, 0, key.size(), key.data(), nullptr);
}

auto Tree::write_value(Node &node, std::size_t index, const Slice &value) -> Status
{
    Cell cell;
    if (read_cell(node, index, &cell)) {
        return corrupted_page(node.ref->page_id);
    }
    return PayloadManager::access(*m_pager, cell, cell.key_size, value.size(), value.data(), nullptr);
}

auto Tree::find_parent_id(Id page_id, Id &out) const -> Status
{
    PointerMap::Entry entry;
    CALICODB_TRY(PointerMap::read_entry(*m_pager, page_id, entry));
    out = entry.back_ptr;
    return Status::ok();
}

auto Tree::fix_parent_id(Id page_id, Id parent_id, PointerMap::Type type) -> Status
{
    PointerMap::Entry entry = {parent_id, type};
    return PointerMap::write_entry(*m_pager, page_id, entry);
}

auto Tree::maybe_fix_overflow_chain(const Cell &cell, Id parent_id) -> Status
{
    if (cell.local_pl_size != cell.total_pl_size) {
        return fix_parent_id(read_overflow_id(cell), parent_id, PointerMap::kOverflowHead);
    }
    return Status::ok();
}

auto Tree::insert_cell(Node &node, std::size_t index, const Cell &cell) -> Status
{
    write_cell(node, index, cell);
    if (!node.hdr.is_external) {
        CALICODB_TRY(fix_parent_id(read_child_id(cell), node.ref->page_id, PointerMap::kTreeNode));
    }
    return maybe_fix_overflow_chain(cell, node.ref->page_id);
}

auto Tree::remove_cell(Node &node, std::size_t index) -> Status
{
    Cell cell;
    if (read_cell(node, index, &cell)) {
        return corrupted_page(node.ref->page_id);
    }
    if (cell.local_pl_size != cell.total_pl_size) {
        CALICODB_TRY(free_overflow(read_overflow_id(cell)));
    }
    erase_cell(node, index, cell.footprint);
    return Status::ok();
}

auto Tree::free_overflow(Id head_id) -> Status
{
    while (!head_id.is_null()) {
        PageRef *page;
        CALICODB_TRY(m_pager->acquire(head_id, page));
        head_id = read_next_id(*page);
        CALICODB_TRY(m_pager->destroy(page));
    }
    return Status::ok();
}

// It is assumed that the children of `node` have incorrect parent pointers. This routine fixes
// these parent pointers using the pointer map. Using a pointer map is vital here: it allows us
// to access way fewer pages when updating the parent pointers (usually just a few as opposed to
// the number of children in `node` which can be very large).
auto Tree::fix_links(Node &node, Id parent_id) -> Status
{
    if (parent_id.is_null()) {
        parent_id = node.ref->page_id;
    }
    for (std::size_t index = 0; index < node.hdr.cell_count; ++index) {
        Cell cell;
        if (read_cell(node, index, &cell)) {
            return corrupted_page(node.ref->page_id);
        }
        // Fix the back pointer for the head of an overflow chain rooted at `node`.
        CALICODB_TRY(maybe_fix_overflow_chain(cell, parent_id));
        if (!node.hdr.is_external) {
            // Fix the parent pointer for the current child node.
            CALICODB_TRY(fix_parent_id(read_child_id(cell), parent_id, PointerMap::kTreeNode));
        }
    }
    if (!node.hdr.is_external) {
        CALICODB_TRY(fix_parent_id(node.hdr.next_id, parent_id, PointerMap::kTreeNode));
    }
    if (node.overflow) {
        CALICODB_TRY(maybe_fix_overflow_chain(*node.overflow, parent_id));
        if (!node.hdr.is_external) {
            CALICODB_TRY(fix_parent_id(read_child_id(*node.overflow), parent_id, PointerMap::kTreeNode));
        }
    }
    return Status::ok();
}

auto Tree::wset_allocate(bool is_external, Node *&node_out) -> Status
{
    const auto idx = m_wset_idx++;
    CALICODB_EXPECT_LT(idx, kMaxWorkingSetSize);
    auto s = allocate(is_external, wset_node(idx));
    node_out = s.is_ok() ? &wset_node(idx) : nullptr;
    return s;
}
auto Tree::allocate(bool is_external, Node &node) -> Status
{
    auto s = m_pager->allocate(node.ref);
    if (s.is_ok()) {
        CALICODB_EXPECT_FALSE(PointerMap::is_map(node.ref->page_id));
        node.hdr = NodeHdr();
        node.hdr.is_external = is_external;
        node.scratch = m_node_scratch;
        setup_node(node);
    }
    return s;
}

auto Tree::wset_acquire(Id page_id, bool upgrade, Node *&node_out) const -> Status
{
    CALICODB_EXPECT_FALSE(PointerMap::is_map(page_id));
    const auto idx = m_wset_idx++;
    CALICODB_EXPECT_LT(idx, kMaxWorkingSetSize);
    auto s = acquire(page_id, upgrade, wset_node(idx));
    node_out = s.is_ok() ? &wset_node(idx) : nullptr;
    return s;
}
auto Tree::acquire(Id page_id, bool write, Node &node) const -> Status
{
    // The internal cursor should use this method instead of acquire(), since it has a dedicated Node
    // object that is populated instead of one of the working set node slots.
    CALICODB_EXPECT_FALSE(PointerMap::is_map(page_id));
    auto s = m_pager->acquire(page_id, node.ref);
    if (s.is_ok()) {
        if (node.hdr.read(node.ref->page + node_header_offset(node))) {
            m_pager->release(node.ref);
            return corrupted_page(page_id);
        }
        node.scratch = m_node_scratch;
        setup_node(node);
        if (write) {
            upgrade(node);
        }
    }
    return s;
}

auto Tree::free(Node &node) -> Status
{
    return m_pager->destroy(node.ref);
}

auto Tree::upgrade(Node &node) const -> void
{
    m_pager->mark_dirty(*node.ref);
}

auto Tree::release(Node &node) const -> void
{
    if (node.ref && m_pager->mode() == Pager::kDirty) {
        // If the pager is in kWrite mode and a page is marked dirty, it immediately
        // transitions to kDirty mode. So, if this node is dirty, then the pager must
        // be in kDirty mode (unless there was an error).
        if (node.hdr.frag_count > 0xFF) {
            // Fragment count overflow.
            if (BlockAllocator::defragment(node)) {
                (void)corrupted_page(node.ref->page_id);
            }
        }
        node.hdr.write(node.ref->page + node_header_offset(node));
    }
    // Pager::release() NULLs out the page reference.
    m_pager->release(node.ref);
}

auto Tree::advance_cursor(Node &node, int diff) const -> void
{
    // InternalCursor::move_to() takes ownership of the page reference in `node`. When the working set
    // is cleared below, this reference is not released.
    m_c.move_to(node, diff);
    clear_working_set();
}

auto Tree::clear_working_set() const -> void
{
    while (m_wset_idx > 0) {
        release(wset_node(--m_wset_idx));
    }
}

auto Tree::finish_operation() const -> void
{
    clear_working_set();
    m_c.clear();
}

auto Tree::resolve_overflow() -> Status
{
    CALICODB_EXPECT_TRUE(m_c.is_valid());

    Status s;
    while (is_overflowing(m_c.node())) {
        if (m_c.node().ref->page_id == root()) {
            s = split_root();
        } else {
            s = split_nonroot();
        }
        if (s.is_ok()) {
            ++m_stats.stats[kStatSMOCount];
        } else {
            break;
        }
    }
    m_c.clear();
    return s;
}

auto Tree::split_root() -> Status
{
    auto &root = m_c.node();
    CALICODB_EXPECT_EQ(Tree::root(), root.ref->page_id);

    Node child;
    auto s = allocate(root.hdr.is_external, child);
    if (s.is_ok()) {
        // Copy the cell content area.
        const auto after_root_headers = cell_area_offset(root);
        auto memory_size = kPageSize - after_root_headers;
        auto *memory = child.ref->page + after_root_headers;
        std::memcpy(memory, root.ref->page + after_root_headers, memory_size);

        // Copy the header and cell pointers. Doesn't copy the page LSN.
        memory_size = root.hdr.cell_count * kPointerSize;
        memory = child.ref->page + cell_slots_offset(child);
        std::memcpy(memory, root.ref->page + cell_slots_offset(root), memory_size);
        child.hdr = root.hdr;

        CALICODB_EXPECT_TRUE(is_overflowing(root));
        std::swap(child.overflow, root.overflow);
        child.overflow_index = root.overflow_index;
        child.gap_size = root.gap_size;
        if (root.ref->page_id.is_root()) {
            child.gap_size += FileHdr::kSize;
        }

        root.hdr = NodeHdr();
        root.hdr.is_external = false;
        root.hdr.next_id = child.ref->page_id;
        setup_node(root);

        s = fix_parent_id(child.ref->page_id, root.ref->page_id, PointerMap::kTreeNode);
        if (s.is_ok()) {
            s = fix_links(child);
        }
        m_c.history[0].index = 0;
        advance_cursor(child, 1);
    }
    return s;
}

auto Tree::transfer_left(Node &left, Node &right) -> Status
{
    CALICODB_EXPECT_EQ(left.hdr.is_external, right.hdr.is_external);
    Cell cell;
    if (read_cell(right, 0, &cell)) {
        return corrupted_page(right.ref->page_id);
    }
    auto s = insert_cell(left, left.hdr.cell_count, cell);
    if (s.is_ok()) {
        CALICODB_EXPECT_FALSE(is_overflowing(left));
        erase_cell(right, 0, cell.footprint);
    }
    return s;
}

auto Tree::split_nonroot() -> Status
{
    auto &node = m_c.node();
    CALICODB_EXPECT_NE(node.ref->page_id, root());
    CALICODB_EXPECT_TRUE(is_overflowing(node));
    const auto &header = node.hdr;

    CALICODB_EXPECT_LT(0, m_c.level);
    const auto last_loc = m_c.history[m_c.level - 1];
    const auto parent_id = last_loc.page_id;
    CALICODB_EXPECT_FALSE(parent_id.is_null());

    Node *parent, *left;
    CALICODB_TRY(wset_acquire(parent_id, true, parent));
    CALICODB_TRY(wset_allocate(header.is_external, left));

    const auto overflow_index = node.overflow_index;
    auto overflow = *node.overflow;
    node.overflow.reset();

    if (overflow_index == header.cell_count) {
        // Note the reversal of the "left" and "right" parameters. We are splitting the other way.
        // This can greatly improve the performance of sequential writes.
        return split_nonroot_fast(
            *parent,
            *left,
            overflow);
    }

    // Fix the overflow. The overflow cell should fit in either "left" or "right". This routine
    // works by transferring cells, one-by-one, from "right" to "left", and trying to insert the
    // overflow cell. Where the overflow cell is written depends on how many cells we have already
    // transferred. If "overflow_index" is 0, we definitely have enough room in "left". Otherwise,
    // we transfer a cell and try to write the overflow cell to "right". If this isn't possible,
    // then the left node must have enough room, since the maximum cell size is limited to roughly
    // 1/4 of a page. If "right" is more than 3/4 full, then "left" must be less than 1/4 full, so
    // it must be able to accept the overflow cell without overflowing.
    for (std::size_t i = 0, n = header.cell_count; i < n; ++i) {
        if (i == overflow_index) {
            CALICODB_TRY(insert_cell(*left, left->hdr.cell_count, overflow));
            break;
        }
        CALICODB_TRY(transfer_left(*left, node));

        if (usable_space(node) >= overflow.footprint + 2) {
            CALICODB_TRY(insert_cell(node, overflow_index - i - 1, overflow));
            break;
        }
        CALICODB_EXPECT_NE(i + 1, n);
    }
    CALICODB_EXPECT_FALSE(is_overflowing(*left));
    CALICODB_EXPECT_FALSE(is_overflowing(node));

    Cell separator;
    if (read_cell(node, 0, &separator)) {
        return corrupted_page(node.ref->page_id);
    }
    detach_cell(separator, cell_scratch());

    if (header.is_external) {
        if (!header.prev_id.is_null()) {
            Node *left_sibling;
            CALICODB_TRY(wset_acquire(header.prev_id, true, left_sibling));
            left_sibling->hdr.next_id = left->ref->page_id;
            left->hdr.prev_id = left_sibling->ref->page_id;
        }
        node.hdr.prev_id = left->ref->page_id;
        left->hdr.next_id = node.ref->page_id;
        CALICODB_TRY(PayloadManager::promote(
            *m_pager,
            nullptr,
            separator,
            parent_id));
    } else {
        left->hdr.next_id = read_child_id(separator);
        CALICODB_TRY(fix_parent_id(left->hdr.next_id, left->ref->page_id, PointerMap::kTreeNode));
        erase_cell(node, 0, separator.footprint);
    }

    // Post the separator into the parent node. This call will fix the sibling's parent pointer.
    write_child_id(separator, left->ref->page_id);
    CALICODB_TRY(insert_cell(*parent, last_loc.index, separator));

    advance_cursor(*parent, -1);
    return Status::ok();
}

auto Tree::split_nonroot_fast(Node &parent, Node &right, const Cell &overflow) -> Status
{
    auto &left = m_c.node();
    const auto &header = left.hdr;
    CALICODB_TRY(insert_cell(right, 0, overflow));

    CALICODB_EXPECT_FALSE(is_overflowing(left));
    CALICODB_EXPECT_FALSE(is_overflowing(right));

    Cell separator;
    if (header.is_external) {
        if (!header.next_id.is_null()) {
            Node *right_sibling;
            CALICODB_TRY(wset_acquire(header.next_id, true, right_sibling));
            right_sibling->hdr.prev_id = right.ref->page_id;
            right.hdr.next_id = right_sibling->ref->page_id;
        }
        right.hdr.prev_id = left.ref->page_id;
        left.hdr.next_id = right.ref->page_id;

        if (read_cell(right, 0, &separator)) {
            return corrupted_page(right.ref->page_id);
        }
        CALICODB_TRY(PayloadManager::promote(
            *m_pager,
            cell_scratch(),
            separator,
            parent.ref->page_id));
    } else {
        if (read_cell(left, header.cell_count - 1, &separator)) {
            return corrupted_page(left.ref->page_id);
        }
        detach_cell(separator, cell_scratch());
        erase_cell(left, header.cell_count - 1, separator.footprint);

        right.hdr.next_id = left.hdr.next_id;
        left.hdr.next_id = read_child_id(separator);
        CALICODB_TRY(fix_parent_id(right.hdr.next_id, right.ref->page_id, PointerMap::kTreeNode));
        CALICODB_TRY(fix_parent_id(left.hdr.next_id, left.ref->page_id, PointerMap::kTreeNode));
    }

    CALICODB_EXPECT_LT(0, m_c.level);
    const auto last_loc = m_c.history[m_c.level - 1];

    // Post the separator into the parent node. This call will fix the sibling's parent pointer.
    write_child_id(separator, left.ref->page_id);
    CALICODB_TRY(insert_cell(parent, last_loc.index, separator));

    const auto offset = !is_overflowing(parent);
    write_child_id(parent, last_loc.index + offset, right.ref->page_id);
    CALICODB_TRY(fix_parent_id(right.ref->page_id, parent.ref->page_id, PointerMap::kTreeNode));

    advance_cursor(parent, -1);
    return Status::ok();
}

auto Tree::resolve_underflow() -> Status
{
    while (m_c.is_valid() && is_underflowing(m_c.node())) {
        if (m_c.node().ref->page_id == root()) {
            return fix_root();
        }
        CALICODB_EXPECT_LT(0, m_c.level);
        const auto last_loc = m_c.history[m_c.level - 1];

        Node *parent;
        CALICODB_TRY(wset_acquire(last_loc.page_id, true, parent));
        CALICODB_TRY(fix_nonroot(*parent, last_loc.index));

        ++m_stats.stats[kStatSMOCount];
    }
    return Status::ok();
}

auto Tree::merge_left(Node &left, Node &right, Node &parent, std::size_t index) -> Status
{
    CALICODB_EXPECT_FALSE(parent.hdr.is_external);
    CALICODB_EXPECT_TRUE(is_underflowing(left));

    if (left.hdr.is_external) {
        CALICODB_EXPECT_TRUE(right.hdr.is_external);
        left.hdr.next_id = right.hdr.next_id;
        CALICODB_TRY(remove_cell(parent, index));

        while (right.hdr.cell_count) {
            CALICODB_TRY(transfer_left(left, right));
        }
        write_child_id(parent, index, left.ref->page_id);

        if (!right.hdr.next_id.is_null()) {
            Node *right_sibling;
            CALICODB_TRY(wset_acquire(right.hdr.next_id, true, right_sibling));
            right_sibling->hdr.prev_id = left.ref->page_id;
        }
    } else {
        CALICODB_EXPECT_FALSE(right.hdr.is_external);
        Cell separator;
        if (read_cell(parent, index, &separator)) {
            return corrupted_page(parent.ref->page_id);
        }
        write_cell(left, left.hdr.cell_count, separator);
        write_child_id(left, left.hdr.cell_count - 1, left.hdr.next_id);
        CALICODB_TRY(fix_parent_id(left.hdr.next_id, left.ref->page_id, PointerMap::kTreeNode));
        CALICODB_TRY(maybe_fix_overflow_chain(separator, left.ref->page_id));
        erase_cell(parent, index, separator.footprint);

        while (right.hdr.cell_count) {
            CALICODB_TRY(transfer_left(left, right));
        }
        left.hdr.next_id = right.hdr.next_id;
        write_child_id(parent, index, left.ref->page_id);
    }
    CALICODB_TRY(fix_links(left));
    return free(right);
}

auto Tree::merge_right(Node &left, Node &right, Node &parent, std::size_t index) -> Status
{
    CALICODB_EXPECT_FALSE(parent.hdr.is_external);
    CALICODB_EXPECT_TRUE(is_underflowing(right));
    if (left.hdr.is_external) {
        CALICODB_EXPECT_TRUE(right.hdr.is_external);

        left.hdr.next_id = right.hdr.next_id;
        CALICODB_EXPECT_EQ(read_child_id(parent, index + 1), right.ref->page_id);
        write_child_id(parent, index + 1, left.ref->page_id);
        CALICODB_TRY(remove_cell(parent, index));

        while (right.hdr.cell_count) {
            CALICODB_TRY(transfer_left(left, right));
        }
        if (!right.hdr.next_id.is_null()) {
            Node *right_sibling;
            CALICODB_TRY(wset_acquire(right.hdr.next_id, true, right_sibling));
            right_sibling->hdr.prev_id = left.ref->page_id;
        }
    } else {
        CALICODB_EXPECT_FALSE(right.hdr.is_external);

        Cell separator;
        if (read_cell(parent, index, &separator)) {
            return corrupted_page(parent.ref->page_id);
        }
        write_cell(left, left.hdr.cell_count, separator);
        write_child_id(left, left.hdr.cell_count - 1, left.hdr.next_id);
        CALICODB_TRY(fix_parent_id(left.hdr.next_id, left.ref->page_id, PointerMap::kTreeNode));
        CALICODB_TRY(maybe_fix_overflow_chain(separator, left.ref->page_id));
        left.hdr.next_id = right.hdr.next_id;

        CALICODB_EXPECT_EQ(read_child_id(parent, index + 1), right.ref->page_id);
        write_child_id(parent, index + 1, left.ref->page_id);
        erase_cell(parent, index, separator.footprint);

        // Transfer the rest of the cells. left shouldn't overflow.
        while (right.hdr.cell_count) {
            CALICODB_TRY(transfer_left(left, right));
        }
    }
    CALICODB_TRY(fix_links(left));
    return free(right);
}

auto Tree::fix_nonroot(Node &parent, std::size_t index) -> Status
{
    auto &node = m_c.node();
    CALICODB_EXPECT_NE(node.ref->page_id, root());
    CALICODB_EXPECT_TRUE(is_underflowing(node));
    CALICODB_EXPECT_FALSE(is_overflowing(parent));

    if (index > 0) {
        Node *left;
        CALICODB_TRY(wset_acquire(read_child_id(parent, index - 1), true, left));
        if (left->hdr.cell_count == 1) {
            CALICODB_TRY(merge_right(*left, m_c.node(), parent, index - 1));
            CALICODB_EXPECT_FALSE(is_overflowing(parent));
            advance_cursor(parent, -1);
            return Status::ok();
        }
        CALICODB_TRY(rotate_right(parent, *left, node, index - 1));
    } else {
        Node *right;
        CALICODB_TRY(wset_acquire(read_child_id(parent, index + 1), true, right));
        if (right->hdr.cell_count == 1) {
            CALICODB_TRY(merge_left(node, *right, parent, index));
            CALICODB_EXPECT_FALSE(is_overflowing(parent));
            advance_cursor(parent, -1);
            return Status::ok();
        }
        CALICODB_TRY(rotate_left(parent, node, *right, index));
    }

    CALICODB_EXPECT_FALSE(is_overflowing(node));
    advance_cursor(parent, -1);
    if (is_overflowing(m_c.node())) {
        CALICODB_TRY(resolve_overflow());
    }
    return Status::ok();
}

auto Tree::fix_root() -> Status
{
    auto &node = m_c.node();
    CALICODB_EXPECT_EQ(node.ref->page_id, root());
    if (node.hdr.is_external) {
        // The whole tree is empty.
        return Status::ok();
    }

    Node child;
    auto s = acquire(node.hdr.next_id, true, child);
    if (s.is_ok()) {
        // We don't have enough room to transfer the child contents into the root, due to the space occupied by
        // the file header. In this case, we'll just split the child and insert the median cell into the root.
        // Note that the child needs an overflow cell for the split routine to work. We'll just fake it by
        // extracting an arbitrary cell and making it the overflow cell.
        if (node.ref->page_id.is_root() && usable_space(child) < FileHdr::kSize) {
            Cell cell;
            child.overflow_index = child.hdr.cell_count / 2;
            if (read_cell(child, child.overflow_index, &cell)) {
                s = corrupted_page(node.ref->page_id);
            } else {
                child.overflow = cell;
                detach_cell(*child.overflow, cell_scratch());
                erase_cell(child, child.overflow_index, cell.footprint);
                advance_cursor(child, 0);
                s = split_nonroot();
            }
        } else {
            if (merge_root(node, child)) {
                s = corrupted_page(node.ref->page_id);
            } else {
                s = free(child);
            }
            if (s.is_ok()) {
                s = fix_links(node);
            }
        }
        release(child);
    }
    return s;
}

auto Tree::rotate_left(Node &parent, Node &left, Node &right, std::size_t index) -> Status
{
    CALICODB_EXPECT_FALSE(parent.hdr.is_external);
    CALICODB_EXPECT_GT(parent.hdr.cell_count, 0);
    CALICODB_EXPECT_GT(right.hdr.cell_count, 1);
    if (left.hdr.is_external) {
        CALICODB_EXPECT_TRUE(right.hdr.is_external);

        Cell lowest;
        if (read_cell(right, 0, &lowest)) {
            return corrupted_page(right.ref->page_id);
        }
        CALICODB_TRY(insert_cell(left, left.hdr.cell_count, lowest));
        CALICODB_EXPECT_FALSE(is_overflowing(left));
        erase_cell(right, 0, lowest.footprint);

        Cell separator;
        if (read_cell(right, 0, &separator)) {
            return corrupted_page(right.ref->page_id);
        }
        CALICODB_TRY(PayloadManager::promote(
            *m_pager,
            cell_scratch(),
            separator,
            parent.ref->page_id));
        write_child_id(separator, left.ref->page_id);

        CALICODB_TRY(remove_cell(parent, index));
        return insert_cell(parent, index, separator);
    } else {
        CALICODB_EXPECT_FALSE(right.hdr.is_external);

        Node *child;
        CALICODB_TRY(wset_acquire(read_child_id(right, 0), true, child));
        const auto saved_id = left.hdr.next_id;
        left.hdr.next_id = child->ref->page_id;
        CALICODB_TRY(fix_parent_id(child->ref->page_id, left.ref->page_id, PointerMap::kTreeNode));

        Cell separator;
        if (read_cell(parent, index, &separator)) {
            return corrupted_page(parent.ref->page_id);
        }
        CALICODB_TRY(insert_cell(left, left.hdr.cell_count, separator));
        CALICODB_EXPECT_FALSE(is_overflowing(left));
        write_child_id(left, left.hdr.cell_count - 1, saved_id);
        erase_cell(parent, index, separator.footprint);

        Cell lowest;
        if (read_cell(right, 0, &lowest)) {
            return corrupted_page(right.ref->page_id);
        }
        detach_cell(lowest, cell_scratch());
        erase_cell(right, 0, lowest.footprint);
        write_child_id(lowest, left.ref->page_id);
        return insert_cell(parent, index, lowest);
    }
}

auto Tree::rotate_right(Node &parent, Node &left, Node &right, std::size_t index) -> Status
{
    CALICODB_EXPECT_FALSE(parent.hdr.is_external);
    CALICODB_EXPECT_GT(parent.hdr.cell_count, 0);
    CALICODB_EXPECT_GT(left.hdr.cell_count, 1);

    if (left.hdr.is_external) {
        CALICODB_EXPECT_TRUE(right.hdr.is_external);

        Cell highest;
        if (read_cell(left, left.hdr.cell_count - 1, &highest)) {
            return corrupted_page(left.ref->page_id);
        }
        CALICODB_TRY(insert_cell(right, 0, highest));
        CALICODB_EXPECT_FALSE(is_overflowing(right));

        auto separator = highest;
        CALICODB_TRY(PayloadManager::promote(
            *m_pager,
            cell_scratch(),
            separator,
            parent.ref->page_id));
        write_child_id(separator, left.ref->page_id);

        // Don't erase the cell until it has been detached.
        erase_cell(left, left.hdr.cell_count - 1, highest.footprint);

        CALICODB_TRY(remove_cell(parent, index));
        CALICODB_TRY(insert_cell(parent, index, separator));
    } else {
        CALICODB_EXPECT_FALSE(right.hdr.is_external);

        Node *child;
        CALICODB_TRY(wset_acquire(left.hdr.next_id, true, child));
        const auto child_id = child->ref->page_id;
        CALICODB_TRY(fix_parent_id(child->ref->page_id, right.ref->page_id, PointerMap::kTreeNode));
        left.hdr.next_id = read_child_id(left, left.hdr.cell_count - 1);

        Cell separator;
        if (read_cell(parent, index, &separator)) {
            return corrupted_page(parent.ref->page_id);
        }
        CALICODB_TRY(insert_cell(right, 0, separator));
        CALICODB_EXPECT_FALSE(is_overflowing(right));
        write_child_id(right, 0, child_id);
        erase_cell(parent, index, separator.footprint);

        Cell highest;
        if (read_cell(left, left.hdr.cell_count - 1, &highest)) {
            return corrupted_page(left.ref->page_id);
        }
        detach_cell(highest, cell_scratch());
        write_child_id(highest, left.ref->page_id);
        erase_cell(left, left.hdr.cell_count - 1, highest.footprint);
        CALICODB_TRY(insert_cell(parent, index, highest));
    }
    return Status::ok();
}

Tree::Tree(Pager &pager, char *scratch, const Id *root_id)
    : m_c(*this),
      m_node_scratch(scratch),
      m_cell_scratch(scratch + kPageSize),
      m_pager(&pager),
      m_root_id(root_id)
{
}

auto Tree::cell_scratch() -> char *
{
    // Leave space for a child ID. We need the maximum difference between the size of a varint and
    // an Id. When a cell is promoted (i.e. made into an internal cell, so it can be posted to the
    // parent node) it loses a varint (the value size), but gains an Id (the left child pointer).
    // We should be able to write any external cell to this location, and still have room to write
    // the left child ID before the key size field, regardless of the number of bytes occupied by
    // the varint value size.
    return m_cell_scratch + sizeof(U32) - 1;
}

auto Tree::get(const Slice &key, std::string *value) const -> Status
{
    bool found;
    auto s = find_external(key, found);
    if (!s.is_ok()) {
        // Do nothing. A low-level I/O error has occurred.
    } else if (!found) {
        s = Status::not_found();
    } else if (value) {
        Slice slice;
        s = read_value(m_c.node(), m_c.index(), *value, &slice);
        value->resize(slice.size());
        if (s.is_ok()) {
            m_stats.stats[kStatRead] += slice.size();
        }
    }
    m_c.clear();
    return s;
}

auto Tree::put(const Slice &key, const Slice &value) -> Status
{
    static constexpr auto kMaxLength = std::numeric_limits<U32>::max();
    if (key.is_empty()) {
        return Status::invalid_argument("key is empty");
    } else if (key.size() > kMaxLength) {
        return Status::invalid_argument("key is too long");
    } else if (value.size() > kMaxLength) {
        return Status::invalid_argument("value is too long");
    }
    bool exact;
    auto s = find_external(key, exact);
    if (s.is_ok()) {
        upgrade(m_c.node());
        if (exact) {
            s = remove_cell(m_c.node(), m_c.index());
        }
        if (s.is_ok()) {
            bool overflow;
            // Create a cell representing the `key` and `value`. This routine also populates any
            // overflow pages necessary to hold a `key` and/or `value` that won't fit on a single
            // node page. If the cell cannot fit in `node`, it will be written to scratch memory.
            s = emplace(m_c.node(), key, value, m_c.index(), overflow);

            if (s.is_ok()) {
                if (overflow) {
                    // There wasn't enough room in `node` to hold the cell. Get the node back and
                    // perform a split.
                    Cell ovfl;
                    if (external_parse_cell(cell_scratch(), cell_scratch() + kPageSize, &ovfl)) {
                        s = corrupted_page(m_c.node().ref->page_id);
                    } else {
                        m_c.node().overflow = ovfl;
                        s = resolve_overflow();
                    }
                }
                m_stats.stats[kStatWrite] += key.size() + value.size();
            }
        }
    }
    m_c.clear();
    return s;
}

auto Tree::emplace(Node &node, const Slice &key, const Slice &value, std::size_t index, bool &overflow) -> Status
{
    CALICODB_EXPECT_TRUE(node.hdr.is_external);
    auto k = key.size();
    auto v = value.size();
    const auto local_pl_size = compute_local_pl_size(k, v);
    const auto has_remote = k + v > local_pl_size;

    if (k > local_pl_size) {
        k = local_pl_size;
        v = 0;
    } else if (has_remote) {
        v = local_pl_size - k;
    }

    CALICODB_EXPECT_EQ(k + v, local_pl_size);
    // Serialize the cell header for the external cell and determine the number
    // of bytes needed for the cell.
    char header[kVarintMaxLength * 2];
    auto *ptr = header;
    ptr = encode_varint(ptr, value.size());
    ptr = encode_varint(ptr, key.size());
    const auto hdr_size = std::uintptr_t(ptr - header);
    const auto cell_size = local_pl_size + hdr_size + sizeof(U32) * has_remote;

    // Attempt to allocate space for the cell in the node. If this is not possible,
    // write the cell to scratch memory. allocate_block() should not return an offset
    // that would interfere with the node header/indirection vector or cause an out-of-
    // bounds write (this only happens if the node is corrupted).
    ptr = cell_scratch();
    const auto local_offset = allocate_block(
        node,
        static_cast<U32>(index),
        static_cast<U32>(cell_size));
    if (local_offset > 0) {
        ptr = node.ref->page + local_offset;
        overflow = false;
    } else if (local_offset == 0) {
        overflow = true;
    } else {
        return corrupted_page(node.ref->page_id);
    }
    // Write the cell header.
    std::memcpy(ptr, header, hdr_size);
    ptr += hdr_size;

    PageRef *prev = nullptr;
    auto payload_left = key.size() + value.size();
    auto prev_pgno = node.ref->page_id;
    auto prev_type = PointerMap::kOverflowHead;
    auto *next_ptr = ptr + local_pl_size;
    auto len = local_pl_size;
    auto src = key;

    Status s;
    while (s.is_ok()) {
        const auto n = std::min(len, static_cast<U32>(src.size()));
        // Copy a chunk of the payload to a page. ptr either points to where the local payload
        // should go in node, or somewhere in prev, which holds the overflow page being written.
        std::memcpy(ptr, src.data(), n);
        src.advance(n);
        payload_left -= n;
        if (payload_left == 0) {
            break;
        }
        ptr += n;
        len -= n;
        if (src.is_empty()) {
            src = value;
        }
        CALICODB_EXPECT_FALSE(src.is_empty());
        if (len == 0) {
            PageRef *ovfl;
            s = m_pager->allocate(ovfl);
            if (s.is_ok()) {
                put_u32(next_ptr, ovfl->page_id.value);
                len = kLinkContentSize;
                ptr = ovfl->page + sizeof(U32);
                next_ptr = ovfl->page;
                if (prev) {
                    m_pager->release(prev, Pager::kNoCache);
                }
                s = PointerMap::write_entry(
                    *m_pager, ovfl->page_id, {prev_pgno, prev_type});
                prev_type = PointerMap::kOverflowLink;
                prev_pgno = ovfl->page_id;
                prev = ovfl;
            }
        }
    }
    if (prev) {
        // prev holds the last page in the overflow chain.
        put_u32(prev->page, 0);
        m_pager->release(prev, Pager::kNoCache);
    }
    return s;
}

auto Tree::erase(const Slice &key) -> Status
{
    bool exact;
    auto s = find_external(key, exact);
    if (s.is_ok() && exact) {
        upgrade(m_c.node());
        s = remove_cell(m_c.node(), m_c.index());
        if (s.is_ok() && is_underflowing(m_c.node())) {
            s = resolve_underflow();
        }
    }
    m_c.clear();
    return s;
}

auto Tree::find_lowest(Node &node_out) const -> Status
{
    auto s = acquire(root(), false, node_out);
    while (s.is_ok() && !node_out.hdr.is_external) {
        const auto next_id = read_child_id(node_out, 0);
        release(node_out);
        s = acquire(next_id, false, node_out);
    }
    return s;
}

auto Tree::find_highest(Node &node_out) const -> Status
{
    auto s = acquire(root(), false, node_out);
    while (s.is_ok() && !node_out.hdr.is_external) {
        const auto next_id = node_out.hdr.next_id;
        release(node_out);
        s = acquire(next_id, false, node_out);
    }
    return s;
}

[[nodiscard]] static constexpr auto is_overflow_type(PointerMap::Type type) -> bool
{
    return type == PointerMap::kOverflowHead ||
           type == PointerMap::kOverflowLink;
}

auto Tree::vacuum_step(PageRef &free, PointerMap::Entry entry, Schema &schema, Id last_id) -> Status
{
    CALICODB_EXPECT_NE(free.page_id, last_id);
    switch (entry.type) {
        case PointerMap::kOverflowLink:
            // Back pointer points to another overflow chain link, or the head of the chain.
            if (!entry.back_ptr.is_null()) {
                PageRef *parent;
                CALICODB_TRY(m_pager->acquire(entry.back_ptr, parent));
                m_pager->mark_dirty(*parent);
                write_next_id(*parent, free.page_id);
                m_pager->release(parent, Pager::kNoCache);
            }
            break;
        case PointerMap::kOverflowHead: {
            // Back pointer points to the node that the overflow chain is rooted in. Search through that node's cells
            // for the target overflowing cell.
            Node *parent;
            CALICODB_TRY(wset_acquire(entry.back_ptr, true, parent));
            bool found = false;
            for (std::size_t i = 0; i < parent->hdr.cell_count; ++i) {
                Cell cell;
                if (read_cell(*parent, i, &cell)) {
                    return corrupted_page(parent->ref->page_id);
                }
                found = cell.local_pl_size != cell.total_pl_size && read_overflow_id(cell) == last_id;
                if (found) {
                    write_overflow_id(cell, free.page_id);
                    break;
                }
            }
            if (!found) {
                return corrupted_page(parent->ref->page_id);
            }
            break;
        }
        case PointerMap::kTreeRoot: {
            schema.vacuum_reroot(last_id, free.page_id);
            // Tree root pages are also node pages (with no parent page). Handle them the same, but
            // note the guard against updating the parent page's child pointers below.
            [[fallthrough]];
        }
        case PointerMap::kTreeNode: {
            if (entry.type != PointerMap::kTreeRoot) {
                // Back pointer points to another node, i.e. this is not a root. Search through the
                // parent for the target child pointer and overwrite it with the new page ID.
                Node *parent;
                CALICODB_TRY(wset_acquire(entry.back_ptr, true, parent));
                CALICODB_EXPECT_FALSE(parent->hdr.is_external);
                bool found = false;
                for (std::size_t i = 0; !found && i <= parent->hdr.cell_count; ++i) {
                    const auto child_id = read_child_id(*parent, i);
                    found = child_id == last_id;
                    if (found) {
                        write_child_id(*parent, i, free.page_id);
                    }
                }
                if (!found) {
                    return corrupted_page(parent->ref->page_id);
                }
            }
            // Update references.
            Node *last;
            CALICODB_TRY(wset_acquire(last_id, true, last));
            CALICODB_TRY(fix_links(*last, free.page_id));
            if (last->hdr.is_external) {
                // Fix sibling links. fix_links() only fixes back pointers (parent pointers and overflow chain
                // head back pointers).
                if (!last->hdr.prev_id.is_null()) {
                    Node *prev;
                    CALICODB_TRY(wset_acquire(last->hdr.prev_id, true, prev));
                    prev->hdr.next_id = free.page_id;
                }
                if (!last->hdr.next_id.is_null()) {
                    Node *next;
                    CALICODB_TRY(wset_acquire(last->hdr.next_id, true, next));
                    next->hdr.prev_id = free.page_id;
                }
            }
            break;
        }
        default:
            return corrupted_page(PointerMap::lookup(last_id));
    }
    CALICODB_TRY(PointerMap::write_entry(*m_pager, last_id, {}));
    CALICODB_TRY(PointerMap::write_entry(*m_pager, free.page_id, entry));
    clear_working_set();

    PageRef *last;
    auto s = m_pager->acquire(last_id, last);
    if (!s.is_ok()) {
        return s;
    }
    if (is_overflow_type(entry.type)) {
        const auto next_id = read_next_id(*last);
        if (!next_id.is_null()) {
            s = PointerMap::read_entry(*m_pager, next_id, entry);
            if (s.is_ok()) {
                entry.back_ptr = free.page_id;
                s = PointerMap::write_entry(*m_pager, next_id, entry);
            }
        }
    }
    std::memcpy(free.page, last->page, kPageSize);
    m_pager->release(last, Pager::kDiscard);
    return s;
}

// Determine what the last page number should be after a vacuum operation completes on a database with the
// given number of pages `db_size` and number of freelist (trunk + leaf) pages `free_size`. This computation
// was taken from SQLite (src/btree.c:finalDbSize()).
static auto vacuum_end_page(U32 db_size, U32 free_size) -> Id
{
    // Number of entries that can fit on a pointer map page.
    static constexpr auto kEntriesPerMap = kPageSize / 5;
    // PageRef *ID of the most-recent pointer map page (the page that holds the back pointer for the last page
    // in the database file).
    const auto pm_page = PointerMap::lookup(Id(db_size));
    // Number of pointer map pages between the current last page and the after-vacuum last page.
    const auto pm_size = (free_size + pm_page.value + kEntriesPerMap - db_size) / kEntriesPerMap;

    auto end_page = Id(db_size - free_size - pm_size);
    end_page.value -= PointerMap::is_map(end_page);
    return end_page;
}

// The CalicoDB database file format does not store the number of free pages; this number must be determined
// by iterating through the freelist trunk pages. At present, this only happens when a vacuum is performed.
[[nodiscard]] static auto determine_freelist_size(Pager &pager, Id free_head, U32 &size_out) -> Status
{
    Status s;
    size_out = 0;
    while (!free_head.is_null()) {
        PageRef *trunk;
        s = pager.acquire(free_head, trunk);
        if (!s.is_ok()) {
            return s;
        }
        size_out += 1 + get_u32(trunk->page + sizeof(U32));
        free_head.value = get_u32(trunk->page);
        pager.release(trunk);
    }
    return s;
}

static constexpr auto is_freelist_type(PointerMap::Type type) -> bool
{
    return type == PointerMap::kFreelistTrunk ||
           type == PointerMap::kFreelistLeaf;
}

auto Tree::vacuum(Schema &schema) -> Status
{
    auto db_size = m_pager->page_count();
    if (db_size == 0) {
        return Status::ok();
    }

    Status s;
    auto &root = m_pager->get_root();

    U32 free_size;
    const auto free_head = FileHdr::get_freelist_head(root.page);
    // Count the number of pages in the freelist, since we don't keep this information stored
    // anywhere. This involves traversing the list of freelist trunk pages. Luckily, these pages
    // are likely to be accessed again soon, so it may not hurt have them in the pager cache.
    s = determine_freelist_size(*m_pager, free_head, free_size);
    // Determine what the last page in the file should be after this vacuum is run to completion.
    const auto end_page = vacuum_end_page(db_size, free_size);
    for (; s.is_ok() && db_size > end_page.value; --db_size) {
        const Id last_page_id(db_size);
        if (!PointerMap::is_map(last_page_id)) {
            PointerMap::Entry entry;
            s = PointerMap::read_entry(*m_pager, last_page_id, entry);
            if (!s.is_ok()) {
                break;
            }
            if (!is_freelist_type(entry.type)) {
                PageRef *free = nullptr;
                // Find an unused page that will exist after the vacuum. Copy the last occupied
                // page into it. Once there are no more such unoccupied pages, the vacuum is
                // finished and all occupied pages are tightly packed at the start of the file.
                while (s.is_ok()) {
                    m_pager->release(free);
                    s = m_pager->allocate(free);
                    if (s.is_ok()) {
                        if (free->page_id <= end_page) {
                            s = vacuum_step(*free, entry, schema, last_page_id);
                            m_pager->release(free);
                            break;
                        }
                    }
                }
            }
        }
    }
    if (s.is_ok() && db_size != end_page.value) {
        std::string msg;
        append_fmt_string(
            msg, "unexpected page count %u (expected %u pages)",
            db_size, end_page.value);
        s = Status::corruption(msg);
    }
    if (s.is_ok()) {
        s = schema.vacuum_finish();
    }
    if (s.is_ok() && db_size < m_pager->page_count()) {
        m_pager->mark_dirty(root);
        FileHdr::put_freelist_head(root.page, Id::null());
        m_pager->set_page_count(db_size);
    }
    return s;
}

// NOTE: `node` is not part of the working set.
auto Tree::destroy_impl(Node &node) -> Status
{
    ScopeGuard guard = [this, &node] {
        release(node);
    };
    for (std::size_t i = 0; i <= node.hdr.cell_count; ++i) {
        if (i < node.hdr.cell_count) {
            Cell cell;
            if (read_cell(node, i, &cell)) {
                return corrupted_page(node.ref->page_id);
            }
            if (cell.local_pl_size != cell.total_pl_size) {
                CALICODB_TRY(free_overflow(read_overflow_id(cell)));
            }
        }
        if (!node.hdr.is_external) {
            const auto save_id = node.ref->page_id;
            const auto next_id = read_child_id(node, i);
            release(node);

            Node next;
            CALICODB_TRY(acquire(next_id, false, next));
            CALICODB_TRY(destroy_impl(next));
            CALICODB_TRY(acquire(save_id, false, node));
        }
    }
    if (!node.ref->page_id.is_root()) {
        std::move(guard).cancel();
        return free(node);
    }
    return Status::ok();
}

auto Tree::destroy(Tree &tree) -> Status
{
    Node root;
    CALICODB_TRY(tree.acquire(tree.root(), false, root));
    return tree.destroy_impl(root);
}

#if CALICODB_TEST

#define CHECK_OK(expr)                                           \
    do {                                                         \
        if (const auto check_s = (expr); !check_s.is_ok()) {     \
            std::fprintf(stderr, "error(line %d): %s\n",         \
                         __LINE__, check_s.to_string().c_str()); \
            std::abort();                                        \
        }                                                        \
    } while (0)

#define CHECK_TRUE(expr)                                                                   \
    do {                                                                                   \
        if (!(expr)) {                                                                     \
            std::fprintf(stderr, "error: \"%s\" was false on line %d\n", #expr, __LINE__); \
            std::abort();                                                                  \
        }                                                                                  \
    } while (0)

#define CHECK_EQ(lhs, rhs)                                                                         \
    do {                                                                                           \
        if ((lhs) != (rhs)) {                                                                      \
            std::fprintf(stderr, "error: \"" #lhs " != " #rhs "\" failed on line %d\n", __LINE__); \
            std::abort();                                                                          \
        }                                                                                          \
    } while (0)

auto Node::TEST_validate() -> void
{
    std::vector<char> used(kPageSize);
    const auto account = [&x = used](auto from, auto size) {
        auto lower = begin(x) + long(from);
        auto upper = begin(x) + long(from) + long(size);
        CHECK_TRUE(!std::any_of(lower, upper, [](auto byte) {
            return byte != '\x00';
        }));

        std::fill(lower, upper, 1);
    };
    // Header(s) and cell pointers.
    {
        account(0, cell_area_offset(*this));

        // Make sure the header fields are not obviously wrong.
        CHECK_TRUE(hdr.frag_count < static_cast<U8>(-1) * 2);
        CHECK_TRUE(hdr.cell_count < static_cast<U16>(-1));
        CHECK_TRUE(hdr.free_start < static_cast<U16>(-1));
    }
    // Gap space.
    {
        account(cell_area_offset(*this), gap_size);
    }
    // Free list blocks.
    {
        std::vector<unsigned> offsets;
        auto i = hdr.free_start;
        const char *data = ref->page;
        while (i) {
            const auto size = get_u16(data + i + kPointerSize);
            account(i, size);
            offsets.emplace_back(i);
            i = get_u16(data + i);
        }
        const auto offsets_copy = offsets;
        std::sort(begin(offsets), end(offsets));
        CHECK_EQ(offsets, offsets_copy);
    }
    // Cell bodies. Also makes sure the cells are in order where possible.
    for (std::size_t n = 0; n < hdr.cell_count; ++n) {
        const auto lhs_ptr = node_get_slot(*this, n);
        Cell lhs_cell;
        CHECK_EQ(0, read_cell_at(*this, lhs_ptr, &lhs_cell));
        CHECK_TRUE(lhs_cell.footprint >= 3);
        account(lhs_ptr, lhs_cell.footprint);

        if (n + 1 < hdr.cell_count) {
            const auto rhs_ptr = node_get_slot(*this, n + 1);
            Cell rhs_cell;
            CHECK_EQ(0, read_cell_at(*this, rhs_ptr, &rhs_cell));
            if (lhs_cell.local_pl_size == lhs_cell.total_pl_size &&
                rhs_cell.local_pl_size == rhs_cell.total_pl_size) {
                const Slice lhs_key(lhs_cell.key, lhs_cell.key_size);
                const Slice rhs_key(rhs_cell.key, rhs_cell.key_size);
                CHECK_TRUE(lhs_key < rhs_key);
            }
        }
    }

    // Every byte should be accounted for, except for fragments.
    const auto total_bytes = std::accumulate(
        begin(used),
        end(used),
        static_cast<int>(hdr.frag_count),
        [](auto accum, auto next) {
            return accum + next;
        });
    CHECK_EQ(kPageSize, std::size_t(total_bytes));
}

class TreeValidator
{
    using NodeCallback = std::function<void(Node &, std::size_t)>;
    using PageCallback = std::function<void(PageRef *&)>;

    struct PrinterData {
        std::vector<std::string> levels;
        std::vector<std::size_t> spaces;
    };

    static auto traverse_inorder_helper(const Tree &tree, Node node, const NodeCallback &callback) -> void
    {
        for (std::size_t index = 0; index <= node.hdr.cell_count; ++index) {
            if (!node.hdr.is_external) {
                const auto saved_id = node.ref->page_id;
                const auto next_id = read_child_id(node, index);

                // "node" must be released while we traverse, otherwise we are limited in how long of a traversal we can
                // perform by the number of pager frames.
                tree.release(node);

                Node next;
                CHECK_OK(tree.acquire(next_id, false, next));
                traverse_inorder_helper(tree, next, callback);
                CHECK_OK(tree.acquire(saved_id, false, node));
            }
            if (index < node.hdr.cell_count) {
                callback(node, index);
            }
        }
        tree.release(node);
    }

    static auto traverse_inorder(const Tree &tree, const NodeCallback &callback) -> void
    {
        Node root;
        CHECK_OK(tree.acquire(tree.root(), false, root));
        traverse_inorder_helper(tree, root, callback);
    }

    static auto traverse_chain(Pager &pager, PageRef *page, const PageCallback &callback) -> void
    {
        for (;;) {
            callback(page);

            const auto next_id = read_next_id(*page);
            pager.release(page);
            if (next_id.is_null()) {
                break;
            }
            CHECK_OK(pager.acquire(next_id, page));
        }
    }

    static auto add_to_level(PrinterData &data, const std::string &message, std::size_t target) -> void
    {
        // If target is equal to levels.size(), add spaces to all levels.
        CHECK_TRUE(target <= data.levels.size());
        std::size_t i = 0;

        auto s_itr = begin(data.spaces);
        auto L_itr = begin(data.levels);
        while (s_itr != end(data.spaces)) {
            CHECK_TRUE(L_itr != end(data.levels));
            if (i++ == target) {
                // Don't leave trailing spaces. Only add them if there will be more text.
                L_itr->resize(L_itr->size() + *s_itr, ' ');
                L_itr->append(message);
                *s_itr = 0;
            } else {
                *s_itr += message.size();
            }
            ++L_itr;
            ++s_itr;
        }
    }

    static auto ensure_level_exists(PrinterData &data, std::size_t level) -> void
    {
        while (level >= data.levels.size()) {
            data.levels.emplace_back();
            data.spaces.emplace_back();
        }
        CHECK_TRUE(data.levels.size() > level);
        CHECK_TRUE(data.levels.size() == data.spaces.size());
    }

    static auto collect_levels(const Tree &tree, PrinterData &data, Node &node, std::size_t level) -> void
    {
        const auto &header = node.hdr;
        ensure_level_exists(data, level);
        for (std::size_t cid = 0; cid < header.cell_count; ++cid) {
            const auto is_first = cid == 0;
            const auto not_last = cid + 1 < header.cell_count;
            Cell cell;
            CHECK_EQ(0, read_cell(node, cid, &cell));

            if (!header.is_external) {
                Node next;
                CHECK_OK(tree.acquire(read_child_id(cell), false, next));
                collect_levels(tree, data, next, level + 1);
            }

            if (is_first) {
                add_to_level(data, std::to_string(node.ref->page_id.value) + ":[", level);
            }

            const auto key = Slice(cell.key, std::min<std::size_t>(3, cell.key_size)).to_string();
            add_to_level(data, escape_string(key), level);
            if (cell.local_pl_size != cell.total_pl_size) {
                add_to_level(data, "(" + number_to_string(read_overflow_id(cell).value) + ")", level);
            }

            if (not_last) {
                add_to_level(data, ",", level);
            } else {
                add_to_level(data, "]", level);
            }
        }
        if (!node.hdr.is_external) {
            Node next;
            CHECK_OK(tree.acquire(node.hdr.next_id, false, next));
            collect_levels(tree, data, next, level + 1);
        }

        tree.release(node);
    }

    [[nodiscard]] static auto get_readable_content(const PageRef &page, U32 size_limit) -> Slice
    {
        return Slice(page.page, kPageSize).range(kLinkContentOffset, std::min(size_limit, kLinkContentSize));
    }

public:
    static auto validate_tree(const Tree &tree) -> void
    {
        auto check_parent_child = [&tree](auto &node, auto index) -> void {
            Node child;
            CHECK_OK(tree.acquire(read_child_id(node, index), false, child));

            Id parent_id;
            CHECK_OK(tree.find_parent_id(child.ref->page_id, parent_id));
            CHECK_TRUE(parent_id == node.ref->page_id);

            tree.release(child);
        };
        traverse_inorder(tree, [f = std::move(check_parent_child)](const auto &node, auto index) {
            const auto count = node.hdr.cell_count;
            CHECK_TRUE(index < count);

            if (!node.hdr.is_external) {
                f(node, index);
                // Rightmost child.
                if (index + 1 == count) {
                    f(node, index + 1);
                }
            }
        });

        traverse_inorder(tree, [&tree](auto &node, auto index) {
            Cell cell;
            CHECK_EQ(0, read_cell(node, index, &cell));

            auto accumulated = cell.local_pl_size;
            auto requested = cell.key_size;
            if (node.hdr.is_external) {
                U32 value_size;
                decode_varint(cell.ptr, node.ref->page + kPageSize, value_size);
                requested += value_size;
            }

            if (cell.local_pl_size != cell.total_pl_size) {
                const auto overflow_id = read_overflow_id(cell);
                PageRef *head;
                CHECK_OK(tree.m_pager->acquire(overflow_id, head));
                traverse_chain(*tree.m_pager, head, [&](auto &page) {
                    CHECK_TRUE(requested > accumulated);
                    const auto size_limit = std::min(static_cast<U32>(kPageSize), requested - accumulated);
                    accumulated += U32(get_readable_content(*page, size_limit).size());
                });
                CHECK_EQ(requested, accumulated);
            }

            if (index == 0) {
                node.TEST_validate();

                if (node.hdr.is_external && !node.hdr.next_id.is_null()) {
                    Node next;
                    CHECK_OK(tree.acquire(node.hdr.next_id, false, next));

                    tree.release(next);
                }
            }
        });

        // Find the leftmost external node.
        Node node;
        CHECK_OK(tree.acquire(tree.root(), false, node));
        while (!node.hdr.is_external) {
            const auto id = read_child_id(node, 0);
            tree.release(node);
            CHECK_OK(tree.acquire(id, false, node));
        }
        while (!node.hdr.next_id.is_null()) {
            Node right;
            CHECK_OK(tree.acquire(node.hdr.next_id, false, right));
            std::string lhs_buffer, rhs_buffer;
            CHECK_OK(const_cast<Tree &>(tree).read_key(node, 0, lhs_buffer, nullptr));
            CHECK_OK(const_cast<Tree &>(tree).read_key(right, 0, rhs_buffer, nullptr));
            CHECK_TRUE(lhs_buffer < rhs_buffer);
            CHECK_EQ(right.hdr.prev_id, node.ref->page_id);
            tree.release(node);
            node = right;
        }
        tree.release(node);
    }

    [[nodiscard]] static auto to_string(const Tree &tree) -> std::string
    {
        std::string repr;
        PrinterData data;

        Node root;
        CHECK_OK(tree.acquire(tree.root(), false, root));
        collect_levels(tree, data, root, 0);
        for (const auto &level : data.levels) {
            repr.append(level + '\n');
        }
        return repr;
    }
};

auto Tree::TEST_validate() const -> void
{
    TreeValidator::validate_tree(*this);
}

auto Tree::TEST_to_string() const -> std::string
{
    return TreeValidator::to_string(*this);
}

#undef CHECK_TRUE
#undef CHECK_EQ
#undef CHECK_OK

#else

auto Node::TEST_validate() -> void
{
}

auto Tree::TEST_to_string() const -> std::string
{
    return "";
}

auto Tree::TEST_validate() const -> void
{
}

#endif // CALICODB_TEST

Tree::InternalCursor::InternalCursor(Tree &tree)
    : m_tree(&tree),
      m_node(&tree.m_wset[0])
{
}

Tree::InternalCursor::~InternalCursor()
{
    clear();
}

auto Tree::InternalCursor::clear() -> void
{
    m_tree->release(*m_node);
    m_status = Status::ok();
}

auto Tree::InternalCursor::seek_root() -> void
{
    clear();
    level = 0;
    history[0] = {m_tree->root(), 0};
    m_status = m_tree->acquire(m_tree->root(), false, *m_node);
}

auto Tree::InternalCursor::seek(const Slice &key) -> bool
{
    CALICODB_EXPECT_TRUE(is_valid());

    auto exact = false;
    auto upper = m_node->hdr.cell_count;
    unsigned lower = 0;
    while (lower < upper) {
        Slice rhs;
        const auto mid = (lower + upper) / 2;
        // This call to Tree::read_key() may return a partial key, if the whole key wasn't
        // needed for the comparison. We read at most 1 byte more than is present in `key`
        // so we still have necessary length information to break ties. This lets us avoid
        // reading overflow chains if it isn't really necessary.
        m_status = m_tree->read_key(*m_node, mid, m_buffer,
                                    &rhs, key.size() + 1);
        if (!m_status.is_ok()) {
            break;
        }
        const auto cmp = key.compare(rhs);
        if (cmp <= 0) {
            exact = cmp == 0;
            upper = mid;
        } else if (cmp > 0) {
            lower = mid + 1;
        }
    }

    const unsigned shift = exact * !m_node->hdr.is_external;
    history[level].index = lower + shift;
    return exact;
}

auto Tree::InternalCursor::move_down(Id child_id) -> void
{
    CALICODB_EXPECT_TRUE(is_valid());
    clear();
    history[++level] = {child_id, 0};
    m_status = m_tree->acquire(child_id, false, *m_node);
}

Cursor::Cursor() = default;

Cursor::~Cursor() = default;

CursorImpl::~CursorImpl()
{
    clear();
    if (m_count_ptr) {
        --*m_count_ptr;
    }
}

auto CursorImpl::fetch_payload(Node &node, std::size_t index) -> Status
{
    m_key.clear();
    m_val.clear();

    Cell cell;
    if (read_cell(node, index, &cell)) {
        return m_tree->corrupted_page(node.ref->page_id);
    }

    auto s = m_tree->read_key(cell, m_key_buf, &m_key);
    if (s.is_ok()) {
        s = m_tree->read_value(cell, m_val_buf, &m_val);
    }
    return s;
}

auto CursorImpl::seek_first() -> void
{
    clear();

    Node lowest;
    m_status = m_tree->find_lowest(lowest);
    if (m_status.is_ok()) {
        seek_to(lowest, 0);
    }
}

auto CursorImpl::seek_last() -> void
{
    clear();

    Node highest;
    m_status = m_tree->find_highest(highest);
    if (!m_status.is_ok()) {
        return;
    }
    if (highest.hdr.cell_count > 0) {
        seek_to(highest, highest.hdr.cell_count - 1);
    } else {
        m_tree->release(highest);
    }
}

auto CursorImpl::next() -> void
{
    CALICODB_EXPECT_TRUE(is_valid());
    if (++m_index < m_node.hdr.cell_count) {
        auto s = fetch_payload(m_node, m_index);
        if (!s.is_ok()) {
            clear(s);
        }
        return;
    }
    const auto next_id = m_node.hdr.next_id;
    clear();

    if (next_id.is_null()) {
        return;
    }
    Node node;
    m_status = m_tree->acquire(next_id, false, node);
    if (m_status.is_ok()) {
        seek_to(node, 0);
    }
}

auto CursorImpl::previous() -> void
{
    CALICODB_EXPECT_TRUE(is_valid());
    if (m_index) {
        auto s = fetch_payload(m_node, --m_index);
        if (!s.is_ok()) {
            clear(s);
        }
        return;
    }
    const auto prev_id = m_node.hdr.prev_id;
    clear();

    if (prev_id.is_null()) {
        return;
    }
    Node node;
    m_status = m_tree->acquire(prev_id, false, node);
    if (m_status.is_ok()) {
        // node should never be empty. TODO: Report corruption
        seek_to(node, std::max<U32>(1, node.hdr.cell_count) - 1);
    }
}

auto CursorImpl::seek_to(Node &node, std::size_t index) -> void
{
    CALICODB_EXPECT_EQ(nullptr, m_node.ref);
    CALICODB_EXPECT_TRUE(m_status.is_ok());
    const auto *hdr = &node.hdr;
    CALICODB_EXPECT_TRUE(hdr->is_external);

    if (index == hdr->cell_count && !hdr->next_id.is_null()) {
        m_tree->release(node);
        auto s = m_tree->acquire(hdr->next_id, false, node);
        if (!s.is_ok()) {
            m_status = s;
            return;
        }
        hdr = &node.hdr;
        index = 0;
    }
    if (index < hdr->cell_count) {
        m_status = fetch_payload(node, index);
        if (m_status.is_ok()) {
            // Take ownership of the node.
            m_node = node;
            node.ref = nullptr;
            m_index = static_cast<U32>(index);
            return;
        }
    }
    m_tree->release(node);
}

auto CursorImpl::seek(const Slice &key) -> void
{
    clear();

    bool unused;
    auto s = m_tree->find_external(key, unused);
    if (s.is_ok()) {
        // On success, seek_to() transfers ownership of the internal cursor's page reference
        // to this cursor. Otherwise, Tree::InternalCursor::clear() will release it below.
        seek_to(m_tree->m_c.node(), m_tree->m_c.index());
        m_tree->m_c.clear();
    } else {
        m_status = s;
    }
}

auto CursorImpl::clear(Status s) -> void
{
    m_tree->release(m_node);
    m_status = std::move(s);
}

} // namespace calicodb
