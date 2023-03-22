// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#include "tree.h"
#include "db_impl.h"
#include "logging.h"
#include "pager.h"
#include "utils.h"
#include <array>
#include <functional>
#include <numeric>

namespace calicodb
{

static constexpr auto kMaxCellHeaderSize =
    kVarintMaxLength + // Value size  (10 B)
    kVarintMaxLength + // Key size    (10 B)
    sizeof(Id);        // Overflow ID (8 B)

inline constexpr auto compute_min_local(std::size_t page_size) -> std::size_t
{
    CALICODB_EXPECT_TRUE(is_power_of_two(page_size));
    // NOTE: This computation was adapted from a similar one in SQLite3.
    return (page_size - kPageHeaderSize - NodeHeader::kSize) * 32 / 256 -
           kMaxCellHeaderSize - sizeof(PageSize);
}

inline constexpr auto compute_max_local(std::size_t page_size) -> std::size_t
{
    CALICODB_EXPECT_TRUE(is_power_of_two(page_size));
    // NOTE: This computation was adapted from a similar one in SQLite3.
    return (page_size - kPageHeaderSize - NodeHeader::kSize) * 64 / 256 -
           kMaxCellHeaderSize - sizeof(PageSize);
}

inline constexpr auto compute_local_size(std::size_t key_size, std::size_t value_size, std::size_t min_local, std::size_t max_local) -> std::size_t
{
    if (key_size + value_size <= max_local) {
        return key_size + value_size;
    } else if (key_size > max_local) {
        return max_local;
    }
    // Try to prevent the key from being split.
    return std::max(min_local, key_size);
}

static auto node_header_offset(const Node &node)
{
    return page_offset(node.page) + kPageHeaderSize;
}

static auto cell_slots_offset(const Node &node)
{
    return node_header_offset(node) + NodeHeader::kSize;
}

static auto cell_area_offset(const Node &node)
{
    return cell_slots_offset(node) + node.header.cell_count * sizeof(PageSize);
}

static auto usable_space(const Node &node) -> std::size_t
{
    // TODO: May need to cache this result, freelist could be long. Compute once when node is constructed.
    return node.gap_size + BlockAllocatorV2::accumulate_free_bytes(node);// node.header.free_total;
}

static auto detach_cell(Cell &cell, char *backing) -> void
{
    if (cell.is_free) {
        return;
    }
    std::memcpy(backing, cell.ptr, cell.size);
    const auto diff = cell.key - cell.ptr;
    cell.ptr = backing;
    cell.key = backing + diff;
    cell.is_free = true;
}

static auto read_child_id_at(const Node &node, std::size_t offset) -> Id
{
    return {get_u64(node.page.data() + offset)};
}

static auto write_child_id_at(Node &node, std::size_t offset, Id child_id) -> void
{
    put_u64(node.page.mutate(offset, sizeof(Id)), child_id.value);
}

static auto read_child_id(const Node &node, std::size_t index) -> Id
{
    const auto &header = node.header;
    CALICODB_EXPECT_LE(index, header.cell_count);
    CALICODB_EXPECT_FALSE(header.is_external);
    if (index == header.cell_count) {
        return header.next_id;
    }
    return read_child_id_at(node, node.get_slot(index));
}

static auto read_child_id(const Cell &cell) -> Id
{
    return {get_u64(cell.ptr)};
}

static auto read_overflow_id(const Cell &cell) -> Id
{
    return {get_u64(cell.key + cell.local_size)};
}

static auto write_overflow_id(Cell &cell, Id overflow_id) -> void
{
    put_u64(cell.key + cell.local_size, overflow_id.value);
}

static auto write_child_id(Node &node, std::size_t index, Id child_id) -> void
{
    auto &header = node.header;
    CALICODB_EXPECT_LE(index, header.cell_count);
    CALICODB_EXPECT_FALSE(header.is_external);
    if (index == header.cell_count) {
        header.next_id = child_id;
    } else {
        write_child_id_at(node, node.get_slot(index), child_id);
    }
}

static auto write_child_id(Cell &cell, Id child_id) -> void
{
    put_u64(cell.ptr, child_id.value);
}

[[nodiscard]] auto read_next_id(const Page &page) -> Id
{
    return {get_u64(page.view(page_offset(page) + kPageHeaderSize))};
}

auto write_next_id(Page &page, Id next_id) -> void
{
    put_u64(page.mutate(page_offset(page) + kPageHeaderSize, sizeof(Id)), next_id.value);
}

static auto parse_external_cell(const NodeMeta &meta, char *data) -> Cell
{
    std::uint64_t key_size, value_size;
    const auto *ptr = data;
    ptr = decode_varint(ptr, value_size);
    ptr = decode_varint(ptr, key_size);
    const auto header_size = static_cast<std::size_t>(ptr - data);

    Cell cell;
    cell.ptr = data;
    cell.key = data + header_size;

    cell.key_size = key_size;
    cell.local_size = compute_local_size(key_size, value_size, meta.min_local, meta.max_local);
    cell.has_remote = cell.local_size < key_size + value_size;
    cell.size = header_size + cell.local_size + cell.has_remote * sizeof(Id);
    return cell;
}

static auto parse_internal_cell(const NodeMeta &meta, char *data) -> Cell
{
    std::uint64_t key_size;
    const auto *ptr = decode_varint(data + sizeof(Id), key_size);
    const auto header_size = static_cast<std::size_t>(ptr - data);

    Cell cell;
    cell.ptr = data;
    cell.key = data + header_size;

    cell.key_size = key_size;
    cell.local_size = compute_local_size(key_size, 0, meta.min_local, meta.max_local);
    cell.has_remote = cell.local_size < key_size;
    cell.size = header_size + cell.local_size + cell.has_remote * sizeof(Id);
    return cell;
}

static constexpr auto sizeof_meta_lookup() -> std::size_t
{
    std::size_t i {};
    for (auto n = kMinPageSize; n <= kMaxPageSize; n *= 2) {
        ++i;
    }
    return i;
}

static constexpr auto create_meta_lookup() -> std::array<NodeMeta[2], sizeof_meta_lookup()>
{
    std::array<NodeMeta[2], sizeof_meta_lookup()> lookup;
    for (std::size_t i {}; i < lookup.size(); ++i) {
        const auto page_size = kMinPageSize << i;
        lookup[i][0].min_local = compute_min_local(page_size);
        lookup[i][0].max_local = compute_max_local(page_size);
        lookup[i][0].parse_cell = parse_internal_cell;
        lookup[i][1].min_local = compute_min_local(page_size);
        lookup[i][1].max_local = compute_max_local(page_size);
        lookup[i][1].parse_cell = parse_external_cell;
    }
    return lookup;
}

// Stores node-type-specific function pointer lookup tables for every possible page size.
static constexpr auto kMetaLookup = create_meta_lookup();

static constexpr auto lookup_meta(std::size_t page_size, bool is_external) -> const NodeMeta *
{
    std::size_t index {};
    for (auto size = kMinPageSize; size != page_size; size <<= 1, ++index)
        ;
    return &kMetaLookup[index][is_external];
}

static auto read_cell_at(Node &node, std::size_t offset) -> Cell
{
    return node.meta->parse_cell(*node.meta, node.page.data() + offset);
}

auto read_cell(Node &node, std::size_t index) -> Cell
{
    return read_cell_at(node, node.get_slot(index));
}

[[nodiscard]] auto BlockAllocatorV2::get_next_pointer(const Node &node, std::size_t offset) -> PageSize
{
    return get_u16(node.page.data() + offset);
}

[[nodiscard]] auto BlockAllocatorV2::get_block_size(const Node &node, std::size_t offset) -> PageSize
{
    return get_u16(node.page.data() + offset + sizeof(PageSize));
}

auto BlockAllocatorV2::set_next_pointer(Node &node, std::size_t offset, PageSize value) -> void
{
    CALICODB_EXPECT_LT(value, node.page.size());
    return put_u16(node.page.mutate(offset, sizeof(PageSize)), value);
}

auto BlockAllocatorV2::set_block_size(Node &node, std::size_t offset, PageSize value) -> void
{
    CALICODB_EXPECT_GE(value, 4);
    CALICODB_EXPECT_LT(value, node.page.size());
    return put_u16(node.page.mutate(offset + sizeof(PageSize), sizeof(PageSize)), value);
}

auto BlockAllocatorV2::accumulate_free_bytes(const Node &node) -> std::size_t
{
    std::size_t total {};
    for (auto ptr = node.header.free_start; ptr != 0; ) {
//        const auto np = get_next_pointer(node, ptr);
//        const auto bs = get_block_size(node, ptr);
//        std::fprintf(stderr,"%d: next = %d, size = %d\n", ptr - 512 + 40, np ? np - 512 + 40 : 0, bs);

        total += get_block_size(node, ptr);
        ptr = get_next_pointer(node, ptr);
    }
//    std::fputc('\n', stderr);
    return total;
}

auto BlockAllocatorV2::allocate_from_free_list(Node &node, PageSize needed_size) -> PageSize
{
    PageSize prev_ptr {};
    PageSize curr_ptr {node.header.free_start};

    while (curr_ptr) {
        if (needed_size <= get_block_size(node, curr_ptr)) {
            return take_free_space(node, prev_ptr, curr_ptr, needed_size);
        }
        prev_ptr = curr_ptr;
        curr_ptr = get_next_pointer(node, curr_ptr);
    }
    return 0;
}

auto BlockAllocatorV2::allocate_from_gap(Node &node, PageSize needed_size) -> PageSize
{
    if (node.gap_size >= needed_size) {
        node.gap_size -= needed_size;
        node.header.cell_start -= needed_size;
        return node.header.cell_start;
    }
    return 0;
}

auto BlockAllocatorV2::take_free_space(Node &node, PageSize ptr0, PageSize ptr1, PageSize needed_size) -> PageSize
{
    CALICODB_EXPECT_LT(ptr0, node.page.size());
    CALICODB_EXPECT_LT(ptr1, node.page.size());
    CALICODB_EXPECT_LT(needed_size, node.page.size());

    const auto ptr2 = get_next_pointer(node, ptr1);
    const auto free_size = get_block_size(node, ptr1);
    auto &header = node.header;

    CALICODB_EXPECT_GE(free_size, needed_size);
    const auto diff = static_cast<PageSize>(free_size - needed_size);

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

auto BlockAllocatorV2::allocate(Node &node, PageSize needed_size) -> PageSize
{
    CALICODB_EXPECT_LT(needed_size, node.page.size());

    if (const auto offset = allocate_from_gap(node, needed_size)) {
        return offset;
    }
    return allocate_from_free_list(node, needed_size);
}

auto BlockAllocatorV2::release(Node &node, std::uint16_t block_start, std::uint16_t block_size) -> void
{
    auto &header = node.header;
    auto &page = node.page;
    // Largest possible fragment that can be reclaimed in this process.
    // Fragments are 3 bytes at most, but (external) cells are at least
    // 3 bytes.
    const std::size_t fragment_cutoff = 2 + !header.is_external;
    CALICODB_EXPECT_NE(block_size, 0);

    // Blocks of less than 4 bytes are too small to hold the free block
    // header, so they must become fragment bytes.
    if (block_size < 4) {
        header.frag_count += block_size;
        return;
    }
    // The free block list is sorted by start position. Find where the
    // new block should go.
    PageSize prev {};
    auto next = header.free_start;
    while (next && next < block_start) {
        prev = next;
        next = get_next_pointer(node, next);
    }

    // Insert the new block.
    set_next_pointer(node, block_start, next);
    set_block_size(node, block_start, block_size);
    if (prev != 0) {
        set_next_pointer(node, prev, block_start);
    } else {
        header.free_start = block_start;
    }
    if (next != 0) {
        // Merge with the "after" block.
        const auto block_end = PageSize(block_start + block_size);
        if (block_end + fragment_cutoff >= next) {
            const auto diff = next - block_end;
            block_size += PageSize(get_block_size(node, next) + diff);
            header.frag_count = PageSize(header.frag_count - diff);
            next = get_next_pointer(node, next);
            set_next_pointer(node, block_start, next);
            set_block_size(node, block_start, block_size);
        }
    }
    if (prev != 0) {
        // Attempt to merge with the "before" block. If there was already a merge with the
        // "after" block, this will connect "before" to "after" via the current free block.
        const auto before_end = PageSize(prev + get_u16(page.data() + prev + 2));
        if (before_end + fragment_cutoff >= block_start) {
            const auto diff = block_start - before_end;
            const auto prev_size = get_block_size(node, prev);
            set_next_pointer(node, prev, next);
            set_block_size(node, prev, PageSize(prev_size + block_size + diff));
            header.frag_count = PageSize(header.frag_count - diff);
        }
    }
    if (header.free_start == 0) {
        // The whole list was empty.
        header.free_start = block_start;
    }
}

auto BlockAllocatorV2::defragment(Node &node, int skip) -> void
{
    auto &header = node.header;
    const auto n = header.cell_count;
    const auto to_skip = static_cast<std::size_t>(skip >= 0 ? skip : n);
    auto end = static_cast<PageSize>(node.page.size());
    auto ptr = node.page.data();
    std::vector<PageSize> ptrs(n);

    for (std::size_t index {}; index < n; ++index) {
        if (index == to_skip) {
            continue;
        }
        const auto offset = node.get_slot(index);
        const auto size = read_cell_at(node, offset).size;

        end -= PageSize(size);
        std::memcpy(node.scratch + end, ptr + offset, size);
        ptrs[index] = end;
    }
    for (std::size_t index {}; index < n; ++index) {
        if (index == to_skip) {
            continue;
        }
        node.set_slot(index, ptrs[index]);
    }
    const auto offset = cell_area_offset(node);
    const auto size = node.page.size() - offset;
    std::memcpy(node.page.mutate(offset, size), node.scratch + offset, size);

    header.cell_start = end;
    header.frag_count = 0;
    header.free_start = 0;
    header.free_total = 0;
    node.gap_size = static_cast<PageSize>(end - cell_area_offset(node));
}

class BlockAllocator
{
    Node *m_node {};

    [[nodiscard]] auto get_next_pointer(std::size_t offset) -> PageSize
    {
        return get_u16(m_node->page.data() + offset);
    }

    [[nodiscard]] auto get_block_size(std::size_t offset) -> PageSize
    {
        return get_u16(m_node->page.data() + offset + sizeof(PageSize));
    }

    auto set_next_pointer(std::size_t offset, PageSize value) -> void
    {
        CALICODB_EXPECT_LT(value, m_node->page.size());
        return put_u16(m_node->page.mutate(offset, sizeof(PageSize)), value);
    }

    auto set_block_size(std::size_t offset, PageSize value) -> void
    {
        CALICODB_EXPECT_GE(value, 4);
        CALICODB_EXPECT_LT(value, m_node->page.size());
        return put_u16(m_node->page.mutate(offset + sizeof(PageSize), sizeof(PageSize)), value);
    }

    [[nodiscard]] auto allocate_from_free_list(PageSize needed_size) -> PageSize;
    [[nodiscard]] auto allocate_from_gap(PageSize needed_size) -> PageSize;
    [[nodiscard]] auto take_free_space(PageSize ptr0, PageSize ptr1, PageSize needed_size) -> PageSize;

public:
    explicit BlockAllocator(Node &node)
        : m_node {&node}
    {
    }

    [[nodiscard]] auto allocate(PageSize needed_size) -> PageSize;
    auto free(PageSize ptr, PageSize size) -> void;
    auto defragment(std::optional<PageSize> skip = std::nullopt) -> void;
};

auto BlockAllocator::allocate_from_free_list(PageSize needed_size) -> PageSize
{
    PageSize prev_ptr {};
    PageSize curr_ptr {m_node->header.free_start};

    while (curr_ptr) {
        if (needed_size <= get_block_size(curr_ptr)) {
            return take_free_space(prev_ptr, curr_ptr, needed_size);
        }
        prev_ptr = curr_ptr;
        curr_ptr = get_next_pointer(curr_ptr);
    }
    return 0;
}

auto BlockAllocator::allocate_from_gap(PageSize needed_size) -> PageSize
{
    if (m_node->gap_size >= needed_size) {
        m_node->gap_size -= needed_size;
        m_node->header.cell_start -= needed_size;
        return m_node->header.cell_start;
    }
    return 0;
}

auto BlockAllocator::take_free_space(PageSize ptr0, PageSize ptr1, PageSize needed_size) -> PageSize
{
    CALICODB_EXPECT_LT(ptr0, m_node->page.size());
    CALICODB_EXPECT_LT(ptr1, m_node->page.size());
    CALICODB_EXPECT_LT(needed_size, m_node->page.size());

    const auto is_first = !ptr0;
    const auto ptr2 = get_next_pointer(ptr1);
    const auto free_size = get_block_size(ptr1);
    auto &header = m_node->header;

    CALICODB_EXPECT_GE(free_size, needed_size);
    const auto diff = static_cast<PageSize>(free_size - needed_size);

    if (diff < 4) {
        header.frag_count = static_cast<std::uint8_t>(header.frag_count + diff);

        if (is_first) {
            header.free_start = static_cast<PageSize>(ptr2);
        } else {
            set_next_pointer(ptr0, ptr2);
        }
    } else {
        set_block_size(ptr1, diff);
    }
    CALICODB_EXPECT_GE(header.free_total, needed_size);
    header.free_total -= needed_size;
    return ptr1 + diff;
}

auto BlockAllocator::allocate(PageSize needed_size) -> PageSize
{
    CALICODB_EXPECT_LT(needed_size, m_node->page.size());

    if (const auto offset = allocate_from_gap(needed_size)) {
        return offset;
    }
    return allocate_from_free_list(needed_size);
}

auto BlockAllocator::free(PageSize ptr, PageSize size) -> void
{
    CALICODB_EXPECT_GE(ptr, cell_area_offset(*m_node));
    CALICODB_EXPECT_LE(ptr + size, m_node->page.size());
    auto &header = m_node->header;

    if (size < 4) {
        header.frag_count = static_cast<std::uint8_t>(header.frag_count + size);
    } else {
        set_next_pointer(ptr, header.free_start);
        set_block_size(ptr, size);
        header.free_start = ptr;
    }
    header.free_total += size;
}

auto BlockAllocator::defragment(std::optional<PageSize> skip) -> void
{
    auto &header = m_node->header;
    const auto n = header.cell_count;
    const auto to_skip = skip.has_value() ? *skip : n;
    auto end = static_cast<PageSize>(m_node->page.size());
    auto ptr = m_node->page.data();
    std::vector<PageSize> ptrs(n);

    for (std::size_t index {}; index < n; ++index) {
        if (index == to_skip) {
            continue;
        }
        const auto offset = m_node->get_slot(index);
        const auto size = read_cell_at(*m_node, offset).size;

        end -= PageSize(size);
        std::memcpy(m_node->scratch + end, ptr + offset, size);
        ptrs[index] = end;
    }
    for (std::size_t index {}; index < n; ++index) {
        if (index == to_skip) {
            continue;
        }
        m_node->set_slot(index, ptrs[index]);
    }
    const auto offset = cell_area_offset(*m_node);
    const auto size = m_node->page.size() - offset;
    std::memcpy(m_node->page.mutate(offset, size), m_node->scratch + offset, size);

    header.cell_start = end;
    header.frag_count = 0;
    header.free_start = 0;
    header.free_total = 0;
    m_node->gap_size = static_cast<PageSize>(end - cell_area_offset(*m_node));
}

static auto setup_node(Node &node) -> void
{
    node.meta = lookup_meta(node.page.size(), node.header.is_external);
    node.slots_offset = static_cast<PageSize>(cell_slots_offset(node));

    if (node.header.cell_start == 0) {
        node.header.cell_start = static_cast<PageSize>(node.page.size());
    }

    const auto bottom = cell_area_offset(node);
    const auto top = node.header.cell_start;

    CALICODB_EXPECT_GE(top, bottom);
    node.gap_size = static_cast<PageSize>(top - bottom);
}

static auto allocate_block(Node &node, PageSize index, PageSize size) -> std::size_t
{
    CALICODB_EXPECT_LE(index, node.header.cell_count);

    if (size + sizeof(PageSize) > usable_space(node)) {
        node.overflow_index = index;
        return 0;
    }

    BlockAllocator alloc {node};

    // We don't have room to insert the cell pointer.
    if (node.gap_size < sizeof(PageSize)) {
        alloc.defragment(std::nullopt);
    }
    // Insert a dummy cell pointer to save the slot.
    node.insert_slot(index, node.page.size() - 1);

    auto offset = alloc.allocate(size);
    if (offset == 0) {
        alloc.defragment(index);
        offset = alloc.allocate(size);
    }
    // We already made sure we had enough room to fulfill the request. If we had to defragment, the call
    // to allocate() following defragmentation should succeed.
    CALICODB_EXPECT_NE(offset, 0);
    node.set_slot(index, offset);

    // Signal that there will be a change here, but don't write anything yet.
    (void)node.page.mutate(offset, size);
    return offset;
}

static auto free_block(Node &node, PageSize index, PageSize size) -> void
{
//    BlockAllocatorV2::release(node, PageSize(node.get_slot(index)), size);
    BlockAllocator alloc {node};
    alloc.free(static_cast<PageSize>(node.get_slot(index)), size);
    node.remove_slot(index);
}

auto write_cell(Node &node, std::size_t index, const Cell &cell) -> std::size_t
{
    if (const auto offset = allocate_block(node, static_cast<PageSize>(index), static_cast<PageSize>(cell.size))) {
        std::memcpy(node.page.mutate(offset, cell.size), cell.ptr, cell.size);
        return offset;
    }
    node.overflow_index = PageSize(index);
    node.overflow = cell;
    return 0;
}

static auto erase_cell(Node &node, std::size_t index, std::size_t size_hint) -> void
{
    CALICODB_EXPECT_LT(index, node.header.cell_count);
    free_block(node, PageSize(index), PageSize(size_hint));
}

auto erase_cell(Node &node, std::size_t index) -> void
{
    erase_cell(node, index, read_cell(node, index).size);
}

static auto emplace_cell(char *out, std::size_t key_size, std::size_t value_size, const Slice &local_key, const Slice &local_value, Id overflow_id) -> char *
{
    out = encode_varint(out, value_size);
    out = encode_varint(out, key_size);

    std::memcpy(out, local_key.data(), local_key.size());
    out += local_key.size();

    std::memcpy(out, local_value.data(), local_value.size());
    out += local_value.size();

    if (!overflow_id.is_null()) {
        put_u64(out, overflow_id.value);
        out += sizeof(overflow_id);
    }
    return out;
}

auto manual_defragment(Node &node) -> void
{
    BlockAllocator alloc {node};
    alloc.defragment();
}

auto Node::get_slot(std::size_t index) const -> std::size_t
{
    CALICODB_EXPECT_LT(index, header.cell_count);
    return get_u16(page.data() + slots_offset + index * sizeof(PageSize));
}

auto Node::set_slot(std::size_t index, std::size_t pointer) -> void
{
    CALICODB_EXPECT_LT(index, header.cell_count);
    return put_u16(page.mutate(slots_offset + index * sizeof(PageSize), sizeof(PageSize)), static_cast<PageSize>(pointer));
}

auto Node::insert_slot(std::size_t index, std::size_t pointer) -> void
{
    CALICODB_EXPECT_LE(index, header.cell_count);
    CALICODB_EXPECT_GE(gap_size, sizeof(PageSize));
    const auto offset = slots_offset + index * sizeof(PageSize);
    const auto size = (header.cell_count - index) * sizeof(PageSize);
    auto *data = page.data() + offset;

    std::memmove(data + sizeof(PageSize), data, size);
    put_u16(data, static_cast<PageSize>(pointer));

    insert_delta(page.m_deltas, {offset, size + sizeof(PageSize)});
    gap_size -= static_cast<PageSize>(sizeof(PageSize));
    ++header.cell_count;
}

auto Node::remove_slot(std::size_t index) -> void
{
    CALICODB_EXPECT_LT(index, header.cell_count);
    const auto offset = slots_offset + index * sizeof(PageSize);
    const auto size = (header.cell_count - index) * sizeof(PageSize);
    auto *data = page.data() + offset;

    std::memmove(data, data + sizeof(PageSize), size);

    insert_delta(page.m_deltas, {offset, size + sizeof(PageSize)});
    gap_size += sizeof(PageSize);
    --header.cell_count;
}

static auto write_node_header_diff(Node &node) -> void
{
    const auto &header = node.header;
    auto offset = node_header_offset(node);
    auto &page = node.page;

    char buffer[NodeHeader::kSize];
    header.write(buffer);

    for (std::size_t i {}; i < sizeof(buffer); ++i) {
        if (page.data()[offset + i] != buffer[i]) {
            page.mutate(offset + i, 1)[0] = buffer[i];
        }
    }
}

auto Node::take() && -> Page
{
    if (page.is_writable()) {
        if (header.frag_count > 0xFF) {
            // Fragment count overflow.
            manual_defragment(*this);
        }
        write_node_header_diff(*this);
    }
    return std::move(page);
}

static auto merge_root(Node &root, Node &child) -> void
{
    CALICODB_EXPECT_EQ(root.header.next_id, child.page.id());
    const auto &header = child.header;
    if (header.free_total) {
        manual_defragment(child);
    }

    // Copy the cell content area.
    CALICODB_EXPECT_GE(header.cell_start, cell_slots_offset(root));
    auto memory_size = child.page.size() - header.cell_start;
    auto memory = root.page.mutate(header.cell_start, memory_size);
    std::memcpy(memory, child.page.data() + header.cell_start, memory_size);

    // Copy the header and cell pointers.
    memory_size = header.cell_count * sizeof(PageSize);
    memory = root.page.mutate(cell_slots_offset(root), memory_size);
    std::memcpy(memory, child.page.data() + cell_slots_offset(child), memory_size);
    root.header = header;
    root.meta = child.meta;
}

struct SeekResult {
    unsigned index {};
    bool exact {};
};

using FetchKey = std::function<Slice(std::size_t)>;

static auto seek_binary(unsigned n, const Slice &key, const FetchKey &fetch) -> SeekResult
{
    auto upper {n};
    unsigned lower {};

    while (lower < upper) {
        const auto mid = (lower + upper) / 2;
        const auto rhs = fetch(mid);
        if (const auto cmp = key.compare(rhs); cmp < 0) {
            upper = mid;
        } else if (cmp > 0) {
            lower = mid + 1;
        } else {
            return {mid, true};
        }
    }
    return {lower, false};
}

NodeIterator::NodeIterator(Node &node, const Parameters &param)
    : m_pager {param.pager},
      m_lhs_key {param.lhs_key},
      m_rhs_key {param.rhs_key},
      m_node {&node}
{
    CALICODB_EXPECT_NE(m_pager, nullptr);
    CALICODB_EXPECT_NE(m_lhs_key, nullptr);
    CALICODB_EXPECT_NE(m_rhs_key, nullptr);
}

// NOTE: "buffer" is only used if the key is fragmented.
auto NodeIterator::fetch_key(std::string &buffer, const Cell &cell, Slice &out) const -> Status
{
    if (!cell.has_remote || cell.key_size <= cell.local_size) {
        out = Slice {cell.key, cell.key_size};
        return Status::ok();
    }

    if (buffer.size() < cell.key_size) {
        buffer.resize(cell.key_size);
    }
    std::memcpy(buffer.data(), cell.key, cell.local_size);

    auto *remote = buffer.data() + cell.local_size;
    CALICODB_TRY(OverflowList::read(*m_pager, read_overflow_id(cell), 0, cell.key_size - cell.local_size, remote));

    out = Slice {buffer.data(), cell.key_size};
    return Status::ok();
}

auto NodeIterator::index() const -> std::size_t
{
    return m_index;
}

auto NodeIterator::seek(const Slice &key, bool *found) -> Status
{
    Status s;
    const auto fetch = [&s, this](auto index) {
        Slice out;
        if (s.is_ok()) {
            s = fetch_key(*m_lhs_key, read_cell(*m_node, index), out);
        }
        return out;
    };

    const auto [index, exact] = seek_binary(
        m_node->header.cell_count, key, fetch);

    m_index = index;
    if (found != nullptr) {
        *found = exact;
    }
    return s;
}

auto NodeIterator::seek(const Cell &cell, bool *found) -> Status
{
    if (!cell.has_remote) {
        return seek({cell.key, cell.key_size});
    }
    Slice key;
    CALICODB_TRY(fetch_key(*m_rhs_key, cell, key));
    return seek(key, found);
}

[[nodiscard]] static auto is_overflowing(const Node &node) -> bool
{
    return node.overflow.has_value();
}

[[nodiscard]] static auto is_underflowing(const Node &node) -> bool
{
    return node.header.cell_count == 0;
}

auto Tree::create(Pager &pager, Id table_id, Id &freelist_head, Id *out) -> Status
{
    Node node;
    std::string scratch;
    Freelist freelist {pager, freelist_head};
    CALICODB_TRY(NodeManager::allocate(pager, freelist, node, scratch, true));
    const auto root_id = node.page.id();
    NodeManager::release(pager, std::move(node));

    if (!table_id.is_root()) {
        CALICODB_EXPECT_FALSE(root_id.is_root());
        // If the page is a root page other than the database root, the back pointer field is used
        // to store the table ID. This lets the vacuum routine quickly locate open tables so their
        // in-memory root variables can be updated.
        PointerMap::Entry entry {table_id, PointerMap::kTreeRoot};
        CALICODB_TRY(PointerMap::write_entry(pager, root_id, entry));
    }
    if (out != nullptr) {
        *out = root_id;
    }
    return Status::ok();
}

auto Tree::node_iterator(Node &node) const -> NodeIterator
{
    const NodeIterator::Parameters param {
        m_pager,
        &m_key_scratch[0],
        &m_key_scratch[1],
    };
    return NodeIterator {node, param};
}

auto Tree::find_external(const Slice &key, SearchResult &out) const -> Status
{
    Node root;
    CALICODB_TRY(acquire(m_root_id, false, root));
    return find_external(key, std::move(root), out);
}

auto Tree::find_external(const Slice &key, Node node, SearchResult &out) const -> Status
{
    for (;;) {
        bool exact;
        auto itr = node_iterator(node);
        CALICODB_TRY(itr.seek(key, &exact));

        if (node.header.is_external) {
            out.node = std::move(node);
            out.index = itr.index();
            out.exact = exact;
            return Status::ok();
        }
        const auto next_id = read_child_id(node, itr.index() + exact);
        CALICODB_EXPECT_NE(next_id, node.page.id()); // Infinite loop.
        release(std::move(node));
        CALICODB_TRY(acquire(next_id, false, node));
    }
}

auto Tree::find_parent_id(Id pid, Id &out) const -> Status
{
    PointerMap::Entry entry;
    CALICODB_TRY(PointerMap::read_entry(*m_pager, pid, entry));
    out = entry.back_ptr;
    return Status::ok();
}

auto Tree::fix_parent_id(Id pid, Id parent_id, PointerMap::Type type) -> Status
{
    PointerMap::Entry entry {parent_id, type};
    return PointerMap::write_entry(*m_pager, pid, entry);
}

auto Tree::maybe_fix_overflow_chain(const Cell &cell, Id parent_id) -> Status
{
    if (cell.has_remote) {
        return fix_parent_id(read_overflow_id(cell), parent_id, PointerMap::kOverflowHead);
    }
    return Status::ok();
}

auto Tree::insert_cell(Node &node, std::size_t index, const Cell &cell) -> Status
{
    write_cell(node, index, cell);
    if (!node.header.is_external) {
        CALICODB_TRY(fix_parent_id(read_child_id(cell), node.page.id(), PointerMap::kTreeNode));
    }
    return maybe_fix_overflow_chain(cell, node.page.id());
}

auto Tree::remove_cell(Node &node, std::size_t index) -> Status
{
    const auto cell = read_cell(node, index);
    if (cell.has_remote) {
        CALICODB_TRY(OverflowList::erase(*m_pager, m_freelist, read_overflow_id(cell)));
    }
    erase_cell(node, index, cell.size);
    return Status::ok();
}

auto Tree::fix_links(Node &node) -> Status
{
    for (std::size_t index {}; index < node.header.cell_count; ++index) {
        const auto cell = read_cell(node, index);
        CALICODB_TRY(maybe_fix_overflow_chain(cell, node.page.id()));
        if (!node.header.is_external) {
            CALICODB_TRY(fix_parent_id(read_child_id(cell), node.page.id(), PointerMap::kTreeNode));
        }
    }
    if (!node.header.is_external) {
        CALICODB_TRY(fix_parent_id(node.header.next_id, node.page.id(), PointerMap::kTreeNode));
    }
    if (node.overflow) {
        CALICODB_TRY(maybe_fix_overflow_chain(*node.overflow, node.page.id()));
        if (!node.header.is_external) {
            CALICODB_TRY(fix_parent_id(read_child_id(*node.overflow), node.page.id(), PointerMap::kTreeNode));
        }
    }
    return Status::ok();
}

auto Tree::allocate(bool is_external, Node &out) -> Status
{
    return NodeManager::allocate(*m_pager, m_freelist, out, m_node_scratch, is_external);
}

auto Tree::acquire(Id page_id, bool upgrade, Node &out) const -> Status
{
    return NodeManager::acquire(*m_pager, page_id, out, m_node_scratch, upgrade);
}

auto Tree::destroy(Node node) -> Status
{
    return NodeManager::destroy(m_freelist, std::move(node));
}

auto Tree::upgrade(Node &node) const -> void
{
    NodeManager::upgrade(*m_pager, node);
}

auto Tree::release(Node node) const -> void
{
    NodeManager::release(*m_pager, std::move(node));
}

auto Tree::resolve_overflow(Node node) -> Status
{
    Node next;
    while (is_overflowing(node)) {
        if (node.page.id() == m_root_id) {
            CALICODB_TRY(split_root(std::move(node), next));
        } else {
            CALICODB_TRY(split_non_root(std::move(node), next));
        }
        node = std::move(next);
    }
    release(std::move(node));
    return Status::ok();
}

auto Tree::split_root(Node root, Node &out) -> Status
{
    Node child;
    CALICODB_TRY(allocate(root.header.is_external, child));

    // Copy the cell content area.
    const auto after_root_headers = cell_area_offset(root);
    auto memory_size = root.page.size() - after_root_headers;
    auto memory = child.page.mutate(after_root_headers, memory_size);
    std::memcpy(memory, root.page.data() + after_root_headers, memory_size);

    // Copy the header and cell pointers. Doesn't copy the page LSN.
    memory_size = root.header.cell_count * sizeof(PageSize);
    memory = child.page.mutate(cell_slots_offset(child), memory_size);
    std::memcpy(memory, root.page.data() + cell_slots_offset(root), memory_size);
    child.header = root.header;

    CALICODB_EXPECT_TRUE(is_overflowing(root));
    std::swap(child.overflow, root.overflow);
    child.overflow_index = root.overflow_index;
    child.gap_size = root.gap_size;
    if (root.page.id().is_root()) {
        child.gap_size += FileHeader::kSize;
    }

    root.header = NodeHeader {};
    root.header.is_external = false;
    root.header.next_id = child.page.id();
    setup_node(root);

    CALICODB_TRY(fix_parent_id(child.page.id(), root.page.id(), PointerMap::kTreeNode));
    release(std::move(root));

    CALICODB_TRY(fix_links(child));
    out = std::move(child);
    return Status::ok();
}

auto Tree::transfer_left(Node &left, Node &right) -> Status
{
    CALICODB_EXPECT_EQ(left.header.is_external, right.header.is_external);
    const auto cell = read_cell(right, 0);
    CALICODB_TRY(insert_cell(left, left.header.cell_count, cell));
    CALICODB_EXPECT_FALSE(is_overflowing(left));
    erase_cell(right, 0, cell.size);
    return Status::ok();
}

auto Tree::split_non_root(Node right, Node &out) -> Status
{
    CALICODB_EXPECT_NE(right.page.id(), m_root_id);
    CALICODB_EXPECT_TRUE(is_overflowing(right));
    const auto &header = right.header;

    Id parent_id;
    CALICODB_TRY(find_parent_id(right.page.id(), parent_id));
    CALICODB_EXPECT_FALSE(parent_id.is_null());

    Node parent, left;
    CALICODB_TRY(acquire(parent_id, true, parent));
    CALICODB_TRY(allocate(header.is_external, left));

    const auto overflow_index = right.overflow_index;
    auto overflow = *right.overflow;
    right.overflow.reset();

    if (overflow_index == header.cell_count) {
        // Note the reversal of the "left" and "right" parameters. We are splitting the other way.
        // This can greatly improve the performance of sequential writes.
        return split_non_root_fast(
            std::move(parent),
            std::move(right),
            std::move(left),
            overflow,
            out);
    }

    // Fix the overflow. The overflow cell should fit in either "left" or "right". This routine
    // works by transferring cells, one-by-one, from "right" to "left", and trying to insert the
    // overflow cell. Where the overflow cell is written depends on how many cells we have already
    // transferred. If "overflow_index" is 0, we definitely have enough room in "left". Otherwise,
    // we transfer a cell and try to write the overflow cell to "right". If this isn't possible,
    // then the left node must have enough room, since the maximum cell size is limited to roughly
    // 1/4 of a page. If "right" is more than 3/4 full, then "left" must be less than 1/4 full, so
    // it must be able to accept the overflow cell without overflowing.
    for (std::size_t i {}, n = header.cell_count; i < n; ++i) {
        if (i == overflow_index) {
            CALICODB_TRY(insert_cell(left, left.header.cell_count, overflow));
            break;
        }
        CALICODB_TRY(transfer_left(left, right));

        if (usable_space(right) >= overflow.size + 2) {
            CALICODB_TRY(insert_cell(right, overflow_index - i - 1, overflow));
            break;
        }
        CALICODB_EXPECT_NE(i + 1, n);
    }
    CALICODB_EXPECT_FALSE(is_overflowing(left));
    CALICODB_EXPECT_FALSE(is_overflowing(right));

    auto separator = read_cell(right, 0);
    detach_cell(separator, cell_scratch());

    if (header.is_external) {
        if (!header.prev_id.is_null()) {
            Node left_sibling;
            CALICODB_TRY(acquire(header.prev_id, true, left_sibling));
            left_sibling.header.next_id = left.page.id();
            left.header.prev_id = left_sibling.page.id();
            release(std::move(left_sibling));
        }
        right.header.prev_id = left.page.id();
        left.header.next_id = right.page.id();
        CALICODB_TRY(PayloadManager::promote(
            *m_pager,
            m_freelist,
            nullptr,
            separator,
            parent_id));
    } else {
        left.header.next_id = read_child_id(separator);
        CALICODB_TRY(fix_parent_id(left.header.next_id, left.page.id(), PointerMap::kTreeNode));
        erase_cell(right, 0);
    }

    auto itr = node_iterator(parent);
    CALICODB_TRY(itr.seek(separator));

    // Post the separator into the parent node. This call will fix the sibling's parent pointer.
    write_child_id(separator, left.page.id());
    CALICODB_TRY(insert_cell(parent, itr.index(), separator));

    release(std::move(left));
    release(std::move(right));
    out = std::move(parent);
    return Status::ok();
}

auto Tree::split_non_root_fast(Node parent, Node left, Node right, const Cell &overflow, Node &out) -> Status
{
    const auto &header = left.header;
    CALICODB_TRY(insert_cell(right, 0, overflow));

    CALICODB_EXPECT_FALSE(is_overflowing(left));
    CALICODB_EXPECT_FALSE(is_overflowing(right));

    Cell separator;
    if (header.is_external) {
        if (!header.next_id.is_null()) {
            Node right_sibling;
            CALICODB_TRY(acquire(header.next_id, true, right_sibling));
            right_sibling.header.prev_id = right.page.id();
            right.header.next_id = right_sibling.page.id();
            release(std::move(right_sibling));
        }
        right.header.prev_id = left.page.id();
        left.header.next_id = right.page.id();

        separator = read_cell(right, 0);
        CALICODB_TRY(PayloadManager::promote(
            *m_pager,
            m_freelist,
            cell_scratch(),
            separator,
            parent.page.id()));
    } else {
        separator = read_cell(left, header.cell_count - 1);
        detach_cell(separator, cell_scratch());
        erase_cell(left, header.cell_count - 1);

        right.header.next_id = left.header.next_id;
        left.header.next_id = read_child_id(separator);
        CALICODB_TRY(fix_parent_id(right.header.next_id, right.page.id(), PointerMap::kTreeNode));
        CALICODB_TRY(fix_parent_id(left.header.next_id, left.page.id(), PointerMap::kTreeNode));
    }

    auto itr = node_iterator(parent);
    CALICODB_TRY(itr.seek(separator));

    // Post the separator into the parent node. This call will fix the sibling's parent pointer.
    write_child_id(separator, left.page.id());
    CALICODB_TRY(insert_cell(parent, itr.index(), separator));

    const auto offset = !is_overflowing(parent);
    write_child_id(parent, itr.index() + offset, right.page.id());
    CALICODB_TRY(fix_parent_id(right.page.id(), parent.page.id(), PointerMap::kTreeNode));

    release(std::move(left));
    release(std::move(right));
    out = std::move(parent);
    return Status::ok();
}

auto Tree::resolve_underflow(Node node, const Slice &anchor) -> Status
{
    while (is_underflowing(node)) {
        if (node.page.id() == m_root_id) {
            return fix_root(std::move(node));
        }
        Id parent_id;
        CALICODB_TRY(find_parent_id(node.page.id(), parent_id));
        CALICODB_EXPECT_FALSE(parent_id.is_null());

        Node parent;
        CALICODB_TRY(acquire(parent_id, true, parent));
        // NOTE: Searching for the anchor key from the node we took from should always give us the correct index
        //       due to the B+-tree ordering rules.
        bool exact;
        auto itr = node_iterator(parent);
        CALICODB_TRY(itr.seek(anchor, &exact));
        CALICODB_TRY(fix_non_root(std::move(node), parent, itr.index() + exact));
        node = std::move(parent);
    }
    release(std::move(node));
    return Status::ok();
}

auto Tree::merge_left(Node &left, Node right, Node &parent, std::size_t index) -> Status
{
    CALICODB_EXPECT_FALSE(parent.header.is_external);
    CALICODB_EXPECT_TRUE(is_underflowing(left));
    if (left.header.is_external) {
        CALICODB_EXPECT_TRUE(right.header.is_external);
        left.header.next_id = right.header.next_id;
        CALICODB_TRY(remove_cell(parent, index));

        while (right.header.cell_count) {
            CALICODB_TRY(transfer_left(left, right));
        }
        write_child_id(parent, index, left.page.id());

        if (!right.header.next_id.is_null()) {
            Node right_sibling;
            CALICODB_TRY(acquire(right.header.next_id, true, right_sibling));
            right_sibling.header.prev_id = left.page.id();
            release(std::move(right_sibling));
        }
    } else {
        CALICODB_EXPECT_FALSE(right.header.is_external);
        auto separator = read_cell(parent, index);
        write_cell(left, left.header.cell_count, separator);
        write_child_id(left, left.header.cell_count - 1, left.header.next_id);
        CALICODB_TRY(fix_parent_id(left.header.next_id, left.page.id(), PointerMap::kTreeNode));
        CALICODB_TRY(maybe_fix_overflow_chain(separator, left.page.id()));
        erase_cell(parent, index, separator.size);

        while (right.header.cell_count) {
            CALICODB_TRY(transfer_left(left, right));
        }
        left.header.next_id = right.header.next_id;
        write_child_id(parent, index, left.page.id());
    }
    CALICODB_TRY(fix_links(left));
    return destroy(std::move(right));
}

auto Tree::merge_right(Node &left, Node right, Node &parent, std::size_t index) -> Status
{
    CALICODB_EXPECT_FALSE(parent.header.is_external);
    CALICODB_EXPECT_TRUE(is_underflowing(right));
    if (left.header.is_external) {
        CALICODB_EXPECT_TRUE(right.header.is_external);

        left.header.next_id = right.header.next_id;
        CALICODB_EXPECT_EQ(read_child_id(parent, index + 1), right.page.id());
        write_child_id(parent, index + 1, left.page.id());
        CALICODB_TRY(remove_cell(parent, index));

        while (right.header.cell_count) {
            CALICODB_TRY(transfer_left(left, right));
        }
        if (!right.header.next_id.is_null()) {
            Node right_sibling;
            CALICODB_TRY(acquire(right.header.next_id, true, right_sibling));
            right_sibling.header.prev_id = left.page.id();
            release(std::move(right_sibling));
        }
    } else {
        CALICODB_EXPECT_FALSE(right.header.is_external);

        auto separator = read_cell(parent, index);
        write_cell(left, left.header.cell_count, separator);
        write_child_id(left, left.header.cell_count - 1, left.header.next_id);
        CALICODB_TRY(fix_parent_id(left.header.next_id, left.page.id(), PointerMap::kTreeNode));
        CALICODB_TRY(maybe_fix_overflow_chain(separator, left.page.id()));
        left.header.next_id = right.header.next_id;

        CALICODB_EXPECT_EQ(read_child_id(parent, index + 1), right.page.id());
        write_child_id(parent, index + 1, left.page.id());
        erase_cell(parent, index, separator.size);

        // Transfer the rest of the cells. left shouldn't overflow.
        while (right.header.cell_count) {
            CALICODB_TRY(transfer_left(left, right));
        }
    }
    CALICODB_TRY(fix_links(left));
    return destroy(std::move(right));
}

auto Tree::fix_non_root(Node node, Node &parent, std::size_t index) -> Status
{
    CALICODB_EXPECT_NE(node.page.id(), m_root_id);
    CALICODB_EXPECT_TRUE(is_underflowing(node));
    CALICODB_EXPECT_FALSE(is_overflowing(parent));

    if (index > 0) {
        Node left;
        CALICODB_TRY(acquire(read_child_id(parent, index - 1), true, left));
        if (left.header.cell_count == 1) {
            CALICODB_TRY(merge_right(left, std::move(node), parent, index - 1));
            release(std::move(left));
            CALICODB_EXPECT_FALSE(is_overflowing(parent));
            return Status::ok();
        }
        CALICODB_TRY(rotate_right(parent, left, node, index - 1));
        release(std::move(left));
    } else {
        Node right;
        CALICODB_TRY(acquire(read_child_id(parent, index + 1), true, right));
        if (right.header.cell_count == 1) {
            CALICODB_TRY(merge_left(node, std::move(right), parent, index));
            release(std::move(node));
            CALICODB_EXPECT_FALSE(is_overflowing(parent));
            return Status::ok();
        }
        CALICODB_TRY(rotate_left(parent, node, right, index));
        release(std::move(right));
    }

    CALICODB_EXPECT_FALSE(is_overflowing(node));
    release(std::move(node));

    if (is_overflowing(parent)) {
        const auto saved_id = parent.page.id();
        CALICODB_TRY(resolve_overflow(std::move(parent)));
        CALICODB_TRY(acquire(saved_id, true, parent));
    }
    return Status::ok();
}

auto Tree::fix_root(Node root) -> Status
{
    CALICODB_EXPECT_EQ(root.page.id(), m_root_id);

    // If the root is external here, the whole tree must be empty.
    if (!root.header.is_external) {
        Node child;
        CALICODB_TRY(acquire(root.header.next_id, true, child));

        // We don't have enough room to transfer the child contents into the root, due to the space occupied by
        // the file header. In this case, we'll just split the child and insert the median cell into the root.
        // Note that the child needs an overflow cell for the split routine to work. We'll just fake it by
        // extracting an arbitrary cell and making it the overflow cell.
        if (root.page.id().is_root() && usable_space(child) < FileHeader::kSize) {
            child.overflow_index = child.header.cell_count / 2;
            child.overflow = read_cell(child, child.overflow_index);
            detach_cell(*child.overflow, cell_scratch());
            erase_cell(child, child.overflow_index);
            release(std::move(root));
            Node parent;
            CALICODB_TRY(split_non_root(std::move(child), parent));
            release(std::move(parent));
            CALICODB_TRY(acquire(m_root_id, true, root));
        } else {
            merge_root(root, child);
            CALICODB_TRY(destroy(std::move(child)));
        }
        CALICODB_TRY(fix_links(root));
    }
    release(std::move(root));
    return Status::ok();
}

auto Tree::rotate_left(Node &parent, Node &left, Node &right, std::size_t index) -> Status
{
    CALICODB_EXPECT_FALSE(parent.header.is_external);
    CALICODB_EXPECT_GT(parent.header.cell_count, 0);
    CALICODB_EXPECT_GT(right.header.cell_count, 1);
    if (left.header.is_external) {
        CALICODB_EXPECT_TRUE(right.header.is_external);

        auto lowest = read_cell(right, 0);
        CALICODB_TRY(insert_cell(left, left.header.cell_count, lowest));
        CALICODB_EXPECT_FALSE(is_overflowing(left));
        erase_cell(right, 0);

        auto separator = read_cell(right, 0);
        CALICODB_TRY(PayloadManager::promote(
            *m_pager,
            m_freelist,
            cell_scratch(),
            separator,
            parent.page.id()));
        write_child_id(separator, left.page.id());

        CALICODB_TRY(remove_cell(parent, index));
        return insert_cell(parent, index, separator);
    } else {
        CALICODB_EXPECT_FALSE(right.header.is_external);

        Node child;
        CALICODB_TRY(acquire(read_child_id(right, 0), true, child));
        const auto saved_id = left.header.next_id;
        left.header.next_id = child.page.id();
        CALICODB_TRY(fix_parent_id(child.page.id(), left.page.id(), PointerMap::kTreeNode));
        release(std::move(child));

        const auto separator = read_cell(parent, index);
        CALICODB_TRY(insert_cell(left, left.header.cell_count, separator));
        CALICODB_EXPECT_FALSE(is_overflowing(left));
        write_child_id(left, left.header.cell_count - 1, saved_id);
        erase_cell(parent, index, separator.size);

        auto lowest = read_cell(right, 0);
        detach_cell(lowest, cell_scratch());
        erase_cell(right, 0);
        write_child_id(lowest, left.page.id());
        return insert_cell(parent, index, lowest);
    }
}

auto Tree::rotate_right(Node &parent, Node &left, Node &right, std::size_t index) -> Status
{
    CALICODB_EXPECT_FALSE(parent.header.is_external);
    CALICODB_EXPECT_GT(parent.header.cell_count, 0);
    CALICODB_EXPECT_GT(left.header.cell_count, 1);

    if (left.header.is_external) {
        CALICODB_EXPECT_TRUE(right.header.is_external);

        auto highest = read_cell(left, left.header.cell_count - 1);
        CALICODB_TRY(insert_cell(right, 0, highest));
        CALICODB_EXPECT_FALSE(is_overflowing(right));

        auto separator = highest;
        CALICODB_TRY(PayloadManager::promote(
            *m_pager,
            m_freelist,
            cell_scratch(),
            separator,
            parent.page.id()));
        write_child_id(separator, left.page.id());

        // Don't erase the cell until it has been detached.
        erase_cell(left, left.header.cell_count - 1);

        CALICODB_TRY(remove_cell(parent, index));
        CALICODB_TRY(insert_cell(parent, index, separator));
    } else {
        CALICODB_EXPECT_FALSE(right.header.is_external);

        Node child;
        CALICODB_TRY(acquire(left.header.next_id, true, child));
        const auto child_id = child.page.id();
        CALICODB_TRY(fix_parent_id(child.page.id(), right.page.id(), PointerMap::kTreeNode));
        left.header.next_id = read_child_id(left, left.header.cell_count - 1);
        release(std::move(child));

        auto separator = read_cell(parent, index);
        CALICODB_TRY(insert_cell(right, 0, separator));
        CALICODB_EXPECT_FALSE(is_overflowing(right));
        write_child_id(right, 0, child_id);
        erase_cell(parent, index, separator.size);

        auto highest = read_cell(left, left.header.cell_count - 1);
        detach_cell(highest, cell_scratch());
        write_child_id(highest, left.page.id());
        erase_cell(left, left.header.cell_count - 1, highest.size);
        CALICODB_TRY(insert_cell(parent, index, highest));
    }
    return Status::ok();
}

Tree::Tree(Pager &pager, Id root_id, Id &freelist_head)
    : m_node_scratch(pager.page_size(), '\0'),
      m_cell_scratch(pager.page_size(), '\0'),
      m_freelist {pager, freelist_head},
      m_pager {&pager},
      m_root_id {root_id}
{
}

auto Tree::cell_scratch() -> char *
{
    // Leave space for a child ID (maximum difference between the size of a varint and an Id).
    return m_cell_scratch.data() + sizeof(Id) - 1;
}

auto Tree::get(const Slice &key, std::string *value) const -> Status
{
    SearchResult slot;
    CALICODB_TRY(find_external(key, slot));
    auto [node, index, exact] = std::move(slot);

    if (!exact) {
        release(std::move(node));
        return Status::not_found("not found");
    }

    if (value != nullptr) {
        const auto cell = read_cell(node, index);
        Slice slice;
        CALICODB_TRY(PayloadManager::collect_value(*m_pager, *value, cell, &slice));
        value->resize(slice.size());
    }
    release(std::move(node));
    return Status::ok();
}

auto Tree::put(const Slice &key, const Slice &value, bool *exists) -> Status
{
    CALICODB_EXPECT_FALSE(key.is_empty());

    SearchResult slot;
    CALICODB_TRY(find_external(key, slot));
    auto [node, index, exact] = std::move(slot);
    upgrade(node);

    if (exact) {
        CALICODB_TRY(remove_cell(node, index));
    }

    CALICODB_TRY(PayloadManager::emplace(*m_pager, m_freelist, cell_scratch(), node, key, value, index));
    CALICODB_TRY(resolve_overflow(std::move(node)));
    if (exists != nullptr) {
        *exists = exact;
    }
    return Status::ok();
}

auto Tree::erase(const Slice &key) -> Status
{
    Tree internal {*this};
    SearchResult slot;

    CALICODB_TRY(internal.find_external(key, slot));
    auto [node, index, exact] = std::move(slot);

    if (exact) {
        Slice anchor;
        const auto cell = read_cell(node, index);
        CALICODB_TRY(PayloadManager::collect_key(*m_pager, m_anchor, cell, &anchor));

        internal.upgrade(node);
        CALICODB_TRY(internal.remove_cell(node, index));
        return internal.resolve_underflow(std::move(node), anchor);
    }
    internal.release(std::move(node));
    return Status::not_found("not found");
}

auto Tree::find_lowest(Node &out) const -> Status
{
    CALICODB_TRY(acquire(m_root_id, false, out));
    while (!out.header.is_external) {
        const auto next_id = read_child_id(out, 0);
        release(std::move(out));
        CALICODB_TRY(acquire(next_id, false, out));
    }
    return Status::ok();
}

auto Tree::find_highest(Node &out) const -> Status
{
    CALICODB_TRY(acquire(m_root_id, false, out));
    while (!out.header.is_external) {
        const auto next_id = out.header.next_id;
        release(std::move(out));
        CALICODB_TRY(acquire(next_id, false, out));
    }
    return Status::ok();
}

auto Tree::vacuum_step(Page &free, TableSet &tables, Id last_id) -> Status
{
    CALICODB_EXPECT_NE(free.id(), last_id);

    PointerMap::Entry entry;
    CALICODB_TRY(PointerMap::read_entry(*m_pager, last_id, entry));

    const auto fix_basic_link = [&entry, &free, this]() -> Status {
        Page parent;
        CALICODB_TRY(m_pager->acquire(entry.back_ptr, parent));
        m_pager->upgrade(parent);
        write_next_id(parent, free.id());
        m_pager->release(std::move(parent));
        return Status::ok();
    };

    switch (entry.type) {
        case PointerMap::kFreelistLink: {
            if (last_id == *m_freelist.m_head) {
                *m_freelist.m_head = free.id();
            } else if (last_id != free.id()) {
                // Back pointer points to another freelist page.
                CALICODB_EXPECT_FALSE(entry.back_ptr.is_null());
                CALICODB_TRY(fix_basic_link());
                Page last;
                CALICODB_TRY(m_pager->acquire(last_id, last));
                if (const auto next_id = read_next_id(last); !next_id.is_null()) {
                    CALICODB_TRY(fix_parent_id(next_id, free.id(), PointerMap::kFreelistLink));
                }
                m_pager->release(std::move(last));
            }
            break;
        }
        case PointerMap::kOverflowLink: {
            // Back pointer points to another overflow chain link, or the head of the chain.
            CALICODB_TRY(fix_basic_link());
            break;
        }
        case PointerMap::kOverflowHead: {
            // Back pointer points to the node that the overflow chain is rooted in. Search through that nodes cells
            // for the target overflowing cell.
            Node parent;
            CALICODB_TRY(acquire(entry.back_ptr, true, parent));
            bool found {};
            for (std::size_t i {}; i < parent.header.cell_count; ++i) {
                auto cell = read_cell(parent, i);
                found = cell.has_remote && read_overflow_id(cell) == last_id;
                if (found) {
                    write_overflow_id(cell, free.id());
                    break;
                }
            }
            CALICODB_EXPECT_TRUE(found);
            release(std::move(parent));
            break;
        }
        case PointerMap::kTreeRoot: {
            if (auto *state = tables.get(entry.back_ptr)) {
                if (state->tree != nullptr) {
                    // Open tables must have their in-memory root page ID changed.
                    state->root_id.page_id = free.id();
                    state->tree->m_root_id = free.id();
                }
            }
            // Tree root pages are also node pages (with no parent page). Handle them the same, but
            // note the guard against updating the parent page's child pointers below.
            [[fallthrough]];
        }
        case PointerMap::kTreeNode: {
            if (entry.type != PointerMap::kTreeRoot) {
                // Back pointer points to another node, i.e. this is not a root. Search through the
                // parent for the target child pointer and overwrite it with the new page ID.
                Node parent;
                CALICODB_TRY(acquire(entry.back_ptr, true, parent));
                CALICODB_EXPECT_FALSE(parent.header.is_external);
                bool found {};
                for (std::size_t i {}; !found && i <= parent.header.cell_count; ++i) {
                    const auto child_id = read_child_id(parent, i);
                    found = child_id == last_id;
                    if (found) {
                        write_child_id(parent, i, free.id());
                    }
                }
                CALICODB_EXPECT_TRUE(found);
                release(std::move(parent));
            }
            // Update references.
            Node last;
            CALICODB_TRY(acquire(last_id, true, last));
            for (std::size_t i {}; i < last.header.cell_count; ++i) {
                const auto cell = read_cell(last, i);
                CALICODB_TRY(maybe_fix_overflow_chain(cell, free.id()));
                if (!last.header.is_external) {
                    CALICODB_TRY(fix_parent_id(read_child_id(last, i), free.id(), PointerMap::kTreeNode));
                }
            }
            if (!last.header.is_external) {
                CALICODB_TRY(fix_parent_id(last.header.next_id, free.id(), PointerMap::kTreeNode));
            } else {
                if (!last.header.prev_id.is_null()) {
                    Node prev;
                    CALICODB_TRY(acquire(last.header.prev_id, true, prev));
                    prev.header.next_id = free.id();
                    release(std::move(prev));
                }
                if (!last.header.next_id.is_null()) {
                    Node next;
                    CALICODB_TRY(acquire(last.header.next_id, true, next));
                    next.header.prev_id = free.id();
                    release(std::move(next));
                }
            }
            release(std::move(last));
        }
    }
    CALICODB_TRY(PointerMap::write_entry(*m_pager, last_id, {}));
    CALICODB_TRY(PointerMap::write_entry(*m_pager, free.id(), entry));
    Page last;
    CALICODB_TRY(m_pager->acquire(last_id, last));
    // We need to upgrade the last node, even though we aren't writing to it. This causes a full image to be written,
    // if there isn't one already, which we will need if we crash during vacuum and need to roll back.
    m_pager->upgrade(last);
    const auto is_link =
        entry.type != PointerMap::kTreeNode &&
        entry.type != PointerMap::kTreeRoot;
    if (is_link) {
        if (const auto next_id = read_next_id(last); !next_id.is_null()) {
            PointerMap::Entry next_entry;
            CALICODB_TRY(PointerMap::read_entry(*m_pager, next_id, next_entry));
            next_entry.back_ptr = free.id();
            CALICODB_TRY(PointerMap::write_entry(*m_pager, next_id, next_entry));
        }
    }
    std::memcpy(free.mutate(sizeof(Lsn), free.size() - sizeof(Lsn)),
                last.data() + sizeof(Lsn), last.size() - sizeof(Lsn));
    m_pager->release(std::move(last));
    return Status::ok();
}

auto Tree::vacuum_one(Id target, TableSet &tables, bool *success) -> Status
{
    if (PointerMap::lookup(*m_pager, target) == target) {
        *success = true;
        return Status::ok();
    }
    if (target.is_root() || m_freelist.is_empty()) {
        *success = false;
        return Status::ok();
    }

    // Swap the head of the freelist with the last page in the file.
    Page head;
    CALICODB_TRY(m_freelist.pop(head));
    if (target != head.id()) {
        // Swap the last page with the freelist head.
        CALICODB_TRY(vacuum_step(head, tables, target));
    } else {
        CALICODB_TRY(fix_parent_id(target, Id::null(), {}));
    }
    m_pager->release(std::move(head));
    *success = true;
    return Status::ok();
}

static constexpr auto kLinkHeaderOffset = sizeof(Lsn);
static constexpr auto kLinkContentOffset = kLinkHeaderOffset + sizeof(Id);

[[nodiscard]] static auto get_readable_content(const Page &page, std::size_t size_limit) -> Slice
{
    return page.view(kLinkContentOffset, std::min(size_limit, page.size() - kLinkContentOffset));
}

Freelist::Freelist(Pager &pager, Id &head)
    : m_pager {&pager},
      m_head {&head}
{
}

[[nodiscard]] auto Freelist::is_empty() const -> bool
{
    return m_head->is_null();
}

auto Freelist::pop(Page &page) -> Status
{
    if (!m_head->is_null()) {
        CALICODB_TRY(m_pager->acquire(*m_head, page));
        m_pager->upgrade(page, kLinkContentOffset);
        *m_head = read_next_id(page);

        if (!m_head->is_null()) {
            // Only clear the back pointer for the new freelist head. Callers must make sure to update the returned
            // node's back pointer at some point.
            const PointerMap::Entry entry {Id::null(), PointerMap::kFreelistLink};
            CALICODB_TRY(PointerMap::write_entry(*m_pager, *m_head, entry));
        }
        return Status::ok();
    }
    return Status::not_supported("free list is empty");
}

auto Freelist::push(Page page) -> Status
{
    CALICODB_EXPECT_FALSE(page.id().is_root());
    write_next_id(page, *m_head);

    // Write the parent of the old head, if it exists.
    PointerMap::Entry entry {page.id(), PointerMap::kFreelistLink};
    if (!m_head->is_null()) {
        CALICODB_TRY(PointerMap::write_entry(*m_pager, *m_head, entry));
    }
    // Clear the parent of the new head.
    entry.back_ptr = Id::null();
    CALICODB_TRY(PointerMap::write_entry(*m_pager, page.id(), entry));

    *m_head = page.id();
    m_pager->release(std::move(page));
    return Status::ok();
}

// The first pointer map page is always on page 2, right after the root page.
static constexpr Id kFirstMapId {2};

static constexpr auto kEntrySize =
    sizeof(char) + // Type
    sizeof(Id);    // Back pointer

static auto entry_offset(Id map_id, Id pid) -> std::size_t
{
    CALICODB_EXPECT_GT(pid, map_id);

    // Account for the page LSN.
    return sizeof(Lsn) + (pid.value - map_id.value - 1) * kEntrySize;
}

static auto decode_entry(const char *data) -> PointerMap::Entry
{
    PointerMap::Entry entry;
    entry.type = PointerMap::Type {*data++};
    entry.back_ptr.value = get_u64(data);
    return entry;
}

auto PointerMap::read_entry(Pager &pager, Id pid, Entry &out) -> Status
{
    const auto mid = lookup(pager, pid);
    CALICODB_EXPECT_GE(mid, kFirstMapId);
    CALICODB_EXPECT_NE(mid, pid);

    const auto offset = entry_offset(mid, pid);
    CALICODB_EXPECT_LE(offset + kEntrySize, pager.page_size());

    Page map;
    CALICODB_TRY(pager.acquire(mid, map));
    out = decode_entry(map.data() + offset);
    pager.release(std::move(map));
    return Status::ok();
}

auto PointerMap::write_entry(Pager &pager, Id pid, Entry entry) -> Status
{
    const auto mid = lookup(pager, pid);
    CALICODB_EXPECT_GE(mid, kFirstMapId);
    CALICODB_EXPECT_NE(mid, pid);

    const auto offset = entry_offset(mid, pid);
    CALICODB_EXPECT_LE(offset + kEntrySize, pager.page_size());

    Page map;
    CALICODB_TRY(pager.acquire(mid, map));
    const auto [back_ptr, type] = decode_entry(map.data() + offset);
    if (entry.back_ptr != back_ptr || entry.type != type) {
        if (!map.is_writable()) {
            pager.upgrade(map);
        }
        auto data = map.mutate(offset, kEntrySize);
        *data++ = entry.type;
        put_u64(data, entry.back_ptr.value);
    }
    pager.release(std::move(map));
    return Status::ok();
}

auto PointerMap::lookup(const Pager &pager, Id pid) -> Id
{
    // Root page (1) has no parents, and page 2 is the first pointer map page. If "pid" is a pointer map
    // page, "pid" will be returned.
    if (pid < kFirstMapId) {
        return Id::null();
    }
    const auto usable_size = pager.page_size() - sizeof(Lsn);
    const auto inc = usable_size / kEntrySize + 1;
    const auto idx = (pid.value - kFirstMapId.value) / inc;
    return {idx * inc + kFirstMapId.value};
}

auto NodeManager::allocate(Pager &pager, Freelist &freelist, Node &out, std::string &scratch, bool is_external) -> Status
{
    const auto fetch_unused_page = [&freelist, &pager](Page &page) {
        if (freelist.is_empty()) {
            CALICODB_TRY(pager.allocate(page));
            // Since this is a fresh page from the end of the file, it could be a pointer map page. If so,
            // it is already blank, so just skip it and allocate another. It'll get filled in as the pages
            // following it are used.
            if (PointerMap::lookup(pager, page.id()) == page.id()) {
                pager.release(std::move(page));
                return pager.allocate(page);
            }
            return Status::ok();
        } else {
            return freelist.pop(page);
        }
    };
    CALICODB_TRY(fetch_unused_page(out.page));
    CALICODB_EXPECT_NE(PointerMap::lookup(pager, out.page.id()), out.page.id());

    out.header.is_external = is_external;
    out.scratch = scratch.data();
    setup_node(out);
    return Status::ok();
}

auto NodeManager::acquire(Pager &pager, Id page_id, Node &out, std::string &scratch, bool upgrade_node) -> Status
{
    auto s = pager.acquire(page_id, out.page);
    if (s.is_ok()) {
        out.scratch = scratch.data();
        out.header.read(out.page.data() + node_header_offset(out));
        setup_node(out);
        if (upgrade_node) {
            upgrade(pager, out);
        }
    }
    return s;
}

auto NodeManager::upgrade(Pager &pager, Node &node) -> void
{
    pager.upgrade(node.page);
}

auto NodeManager::release(Pager &pager, Node node) -> void
{
    pager.release(std::move(node).take());
}

auto NodeManager::destroy(Freelist &freelist, Node node) -> Status
{
    return freelist.push(std::move(node.page));
}

auto OverflowList::read(Pager &pager, Id head_id, std::size_t offset, std::size_t payload_size, char *buffer) -> Status
{
    while (payload_size != 0) {
        Page page;
        CALICODB_TRY(pager.acquire(head_id, page));
        auto content = get_readable_content(page, page.size());

        if (offset) {
            const auto max = std::min(offset, content.size());
            content.advance(max);
            offset -= max;
        }
        if (!content.is_empty()) {
            const auto size = std::min(payload_size, content.size());
            std::memcpy(buffer, content.data(), size);
            payload_size -= size;
            buffer += size;
        }
        head_id = read_next_id(page);
        pager.release(std::move(page));
    }
    return Status::ok();
}

auto OverflowList::write(Pager &pager, Freelist &freelist, Id &out, const Slice &first, const Slice &second) -> Status
{
    std::optional<Page> prev;
    auto head = Id::null();
    auto a = first;
    auto b = second;

    if (a.is_empty()) {
        a = b;
        b.clear();
    }

    while (!a.is_empty()) {
        Page page;
        auto s = freelist.pop(page);
        if (s.is_not_supported()) {
            s = pager.allocate(page);
            if (s.is_ok() && PointerMap::lookup(pager, page.id()) == page.id()) {
                pager.release(std::move(page));
                s = pager.allocate(page);
            }
        }
        CALICODB_TRY(s);

        auto content_size = std::min(a.size() + second.size(), page.size() - kLinkContentOffset);
        auto content_data = page.mutate(kLinkContentOffset, content_size);
        auto limit = std::min(a.size(), content_size);
        std::memcpy(content_data, a.data(), limit);
        a.advance(limit);

        if (a.is_empty()) {
            a = b;
            b.clear();

            if (!a.is_empty()) {
                content_data += limit;
                content_size -= limit;
                limit = std::min(a.size(), content_size);
                std::memcpy(content_data, a.data(), limit);
                a.advance(limit);
            }
        }
        if (prev) {
            write_next_id(*prev, page.id());
            const PointerMap::Entry entry {prev->id(), PointerMap::kOverflowLink};
            CALICODB_TRY(PointerMap::write_entry(pager, page.id(), entry));
            pager.release(std::move(*prev));
        } else {
            head = page.id();
        }
        prev.emplace(std::move(page));
    }
    if (prev) {
        // "prev" contains the last page in the chain.
        write_next_id(*prev, Id::null());
        pager.release(std::move(*prev));
    }
    out = head;
    return Status::ok();
}

auto OverflowList::copy(Pager &pager, Freelist &freelist, Id overflow_id, std::size_t size, Id &out) -> Status
{
    std::string scratch; // TODO: Copy page-by-page: no scratch is necessary.
    scratch.resize(size);

    CALICODB_TRY(read(pager, overflow_id, 0, size, scratch.data()));
    return write(pager, freelist, out, scratch);
}

auto OverflowList::erase(Pager &pager, Freelist &freelist, Id head_id) -> Status
{
    while (!head_id.is_null()) {
        Page page;
        CALICODB_TRY(pager.acquire(head_id, page));
        head_id = read_next_id(page);
        pager.upgrade(page);
        CALICODB_TRY(freelist.push(std::move(page)));
    }
    return Status::ok();
}

auto PayloadManager::emplace(Pager &pager, Freelist &freelist, char *scratch, Node &node, const Slice &key, const Slice &value, std::size_t index) -> Status
{
    CALICODB_EXPECT_TRUE(node.header.is_external);

    auto k = key.size();
    auto v = value.size();
    const auto local_size = compute_local_size(k, v, node.meta->min_local, node.meta->max_local);
    const auto has_remote = k + v > local_size;

    if (k > local_size) {
        k = local_size;
        v = 0;
    } else if (has_remote) {
        v = local_size - k;
    }

    CALICODB_EXPECT_EQ(k + v, local_size);
    auto total_size = local_size + varint_length(key.size()) + varint_length(value.size());

    Id overflow_id;
    if (has_remote) {
        CALICODB_TRY(OverflowList::write(pager, freelist, overflow_id, key.range(k), value.range(v)));
        PointerMap::Entry entry {node.page.id(), PointerMap::kOverflowHead};
        CALICODB_TRY(PointerMap::write_entry(pager, overflow_id, entry));
        total_size += sizeof(overflow_id);
    }

    const auto emplace = [&](auto *out) {
        ::calicodb::emplace_cell(out, key.size(), value.size(), key.range(0, k), value.range(0, v), overflow_id);
    };

    if (const auto offset = allocate_block(node, static_cast<PageSize>(index), static_cast<PageSize>(total_size))) {
        // Write directly into the node.
        emplace(node.page.data() + offset);
    } else {
        // The node has overflowed. Write the cell to scratch memory.
        emplace(scratch);
        node.overflow = parse_external_cell(*node.meta, scratch);
        node.overflow->is_free = true;
    }
    return Status::ok();
}

auto PayloadManager::promote(Pager &pager, Freelist &freelist, char *scratch, Cell &cell, Id parent_id) -> Status
{
    detach_cell(cell, scratch);

    // "scratch" should have enough room before its "m_data" member to write the left child ID.
    const auto header_size = sizeof(Id) + varint_length(cell.key_size);
    cell.ptr = cell.key - header_size;
    const auto *meta = lookup_meta(pager.page_size(), true);
    cell.local_size = compute_local_size(cell.key_size, 0, meta->min_local, meta->max_local);
    cell.size = header_size + cell.local_size;
    cell.has_remote = false;

    if (cell.key_size > cell.local_size) {
        // Part of the key is on an overflow page. No value is stored locally in this case, so the local size computation is still correct.
        Id overflow_id;
        CALICODB_TRY(OverflowList::copy(pager, freelist, read_overflow_id(cell), cell.key_size - cell.local_size, overflow_id));
        PointerMap::Entry entry {parent_id, PointerMap::kOverflowHead};
        CALICODB_TRY(PointerMap::write_entry(pager, overflow_id, entry));
        write_overflow_id(cell, overflow_id);
        cell.size += sizeof(Id);
        cell.has_remote = true;
    }
    return Status::ok();
}

auto PayloadManager::collect_key(Pager &pager, std::string &result, const Cell &cell, Slice *key) -> Status
{
    if (result.size() < cell.key_size) {
        result.resize(cell.key_size);
    }
    if (!cell.has_remote || cell.key_size <= cell.local_size) {
        std::memcpy(result.data(), cell.key, cell.key_size);
        if (key != nullptr) {
            *key = Slice {result.data(), cell.key_size};
        }
        return Status::ok();
    }
    std::memcpy(result.data(), cell.key, cell.local_size);

    CALICODB_TRY(OverflowList::read(
        pager,
        read_overflow_id(cell),
        0,
        cell.key_size - cell.local_size,
        result.data() + cell.local_size));
    if (key != nullptr) {
        *key = Slice {result}.truncate(cell.key_size);
    }
    return Status::ok();
}

auto PayloadManager::collect_value(Pager &pager, std::string &result, const Cell &cell, Slice *value) -> Status
{
    std::uint64_t value_size;
    decode_varint(cell.ptr, value_size);
    if (result.size() < value_size) {
        result.resize(value_size);
    }
    if (!cell.has_remote) {
        std::memcpy(result.data(), cell.key + cell.key_size, value_size);
        if (value != nullptr) {
            *value = Slice {result.data(), value_size};
        }
        return Status::ok();
    }
    std::size_t remote_key_size {};
    if (cell.key_size > cell.local_size) {
        remote_key_size = cell.key_size - cell.local_size;
    }
    std::size_t local_value_size {};
    if (remote_key_size == 0) {
        local_value_size = cell.local_size - cell.key_size;
        std::memcpy(result.data(), cell.key + cell.key_size, local_value_size);
    }

    CALICODB_TRY(OverflowList::read(
        pager,
        read_overflow_id(cell),
        remote_key_size,
        value_size - local_value_size,
        result.data() + local_value_size));
    if (value != nullptr) {
        *value = Slice {result}.truncate(value_size);
    }
    return Status::ok();
}

#if CALICODB_BUILD_TESTS

#define CHECK_OK(expr)                                                                     \
    do {                                                                                   \
        if (const auto check_s = (expr); !check_s.is_ok()) {                               \
            std::fprintf(stderr, "error: encountered %s status \"%s\" on line %d\n",       \
                         get_status_name(check_s), check_s.to_string().c_str(), __LINE__); \
            std::abort();                                                                  \
        }                                                                                  \
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
    std::vector<char> used(page.size());
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
    }
    // Gap space.
    {
        account(cell_area_offset(*this), gap_size);
    }
    // Free list blocks.
    {
        PageSize i {header.free_start};
        const char *data = page.data();
        std::size_t free_total {};
        while (i) {
            const auto size = get_u16(data + i + sizeof(PageSize));
            account(i, size);
            i = get_u16(data + i);
            free_total += size;
        }
        CHECK_EQ(free_total + header.frag_count, header.free_total);
    }
    // Cell bodies. Also makes sure the cells are in order.
    for (std::size_t n {}; n < header.cell_count; ++n) {
        const auto lhs_ptr = get_slot(n);
        const auto lhs_cell = read_cell_at(*this, lhs_ptr);
        CHECK_TRUE(lhs_cell.size >= 3);
        account(lhs_ptr, lhs_cell.size);

        if (n + 1 < header.cell_count) {
            const auto rhs_ptr = get_slot(n + 1);
            const auto rhs_cell = read_cell_at(*this, rhs_ptr);
            if (!lhs_cell.has_remote && !rhs_cell.has_remote) {
                const Slice lhs_key {lhs_cell.key, lhs_cell.key_size};
                const Slice rhs_key {rhs_cell.key, rhs_cell.key_size};
                CHECK_TRUE(lhs_key < rhs_key);
            }
        }
    }

    // Every byte should be accounted for, except for fragments.
    const auto total_bytes = std::accumulate(
        begin(used),
        end(used),
        int(header.frag_count),
        [](auto accum, auto next) {
            return accum + next;
        });
    CHECK_EQ(page.size(), std::size_t(total_bytes));
}

class TreeValidator
{
    using NodeCallback = std::function<void(Node &, std::size_t)>;
    using PageCallback = std::function<void(const Page &)>;

    struct PrinterData {
        std::vector<std::string> levels;
        std::vector<std::size_t> spaces;
    };

    static auto traverse_inorder_helper(const Tree &tree, Node node, const NodeCallback &callback) -> void
    {
        for (std::size_t index {}; index <= node.header.cell_count; ++index) {
            if (!node.header.is_external) {
                const auto saved_id = node.page.id();
                const auto next_id = read_child_id(node, index);

                // "node" must be released while we traverse, otherwise we are limited in how long of a traversal we can
                // perform by the number of pager frames.
                tree.release(std::move(node));

                Node next;
                CHECK_OK(tree.acquire(next_id, false, next));
                traverse_inorder_helper(tree, std::move(next), callback);
                CHECK_OK(tree.acquire(saved_id, false, node));
            }
            if (index < node.header.cell_count) {
                callback(node, index);
            }
        }
        tree.release(std::move(node));
    }

    static auto traverse_inorder(const Tree &tree, const NodeCallback &callback) -> void
    {
        Node root;
        CHECK_OK(tree.acquire(tree.m_root_id, false, root));
        traverse_inorder_helper(tree, std::move(root), callback);
    }

    static auto traverse_chain(Pager &pager, Page page, const PageCallback &callback) -> void
    {
        for (;;) {
            callback(page);

            const auto next_id = read_next_id(page);
            pager.release(std::move(page));
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
        std::size_t i {};

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

    static auto collect_levels(Tree &tree, PrinterData &data, Node node, std::size_t level) -> void
    {
        const auto &header = node.header;
        ensure_level_exists(data, level);
        for (std::size_t cid {}; cid < header.cell_count; ++cid) {
            const auto is_first = cid == 0;
            const auto not_last = cid + 1 < header.cell_count;
            auto cell = read_cell(node, cid);

            if (!header.is_external) {
                Node next;
                CHECK_OK(tree.acquire(read_child_id(cell), false, next));
                collect_levels(tree, data, std::move(next), level + 1);
            }

            if (is_first) {
                add_to_level(data, std::to_string(node.page.id().value) + ":[", level);
            }

            const auto key = Slice {cell.key, std::min<std::size_t>(3, cell.key_size)}.to_string();
            add_to_level(data, escape_string(key), level);
            if (cell.has_remote) {
                add_to_level(data, "(" + number_to_string(read_overflow_id(cell).value) + ")", level);
            }

            if (not_last) {
                add_to_level(data, ",", level);
            } else {
                add_to_level(data, "]", level);
            }
        }
        if (!node.header.is_external) {
            Node next;
            CHECK_OK(tree.acquire(node.header.next_id, false, next));
            collect_levels(tree, data, std::move(next), level + 1);
        }

        tree.release(std::move(node));
    }

public:
    static auto validate_freelist(Tree &tree, Id head) -> void
    {
        auto &pager = *tree.m_pager;
        auto &freelist = tree.m_freelist;
        if (freelist.is_empty()) {
            return;
        }
        CHECK_TRUE(!head.is_null());
        Page page;
        CHECK_OK(pager.acquire(head, page));

        Id parent_id;
        traverse_chain(pager, std::move(page), [&](const auto &link) {
            Id found_id;
            CHECK_OK(tree.find_parent_id(link.id(), found_id));
            CHECK_TRUE(found_id == parent_id);
            parent_id = link.id();
        });
    }

    static auto validate_tree(const Tree &tree) -> void
    {
        CHECK_EQ(lookup_meta(tree.m_pager->page_size(), true)->min_local, compute_min_local(tree.m_pager->page_size()));
        CHECK_EQ(lookup_meta(tree.m_pager->page_size(), true)->max_local, compute_max_local(tree.m_pager->page_size()));

        auto check_parent_child = [&tree](auto &node, auto index) -> void {
            Node child;
            CHECK_OK(tree.acquire(read_child_id(node, index), false, child));

            Id parent_id;
            CHECK_OK(tree.find_parent_id(child.page.id(), parent_id));
            CHECK_TRUE(parent_id == node.page.id());

            tree.release(std::move(child));
        };
        traverse_inorder(tree, [f = std::move(check_parent_child)](const auto &node, auto index) {
            const auto count = node.header.cell_count;
            CHECK_TRUE(index < count);

            if (!node.header.is_external) {
                f(node, index);
                // Rightmost child.
                if (index + 1 == count) {
                    f(node, index + 1);
                }
            }
        });

        traverse_inorder(tree, [&tree](auto &node, auto index) {
            const auto cell = read_cell(node, index);

            auto accumulated = cell.local_size;
            auto requested = cell.key_size;
            if (node.header.is_external) {
                std::uint64_t value_size;
                decode_varint(cell.ptr, value_size);
                requested += value_size;
            }

            if (cell.has_remote) {
                const auto overflow_id = read_overflow_id(cell);
                Page head;
                CHECK_OK(tree.m_pager->acquire(overflow_id, head));
                traverse_chain(*tree.m_pager, std::move(head), [&](auto &page) {
                    CHECK_TRUE(requested > accumulated);
                    const auto size_limit = std::min(page.size(), requested - accumulated);
                    accumulated += get_readable_content(page, size_limit).size();
                });
                CHECK_EQ(requested, accumulated);
            }

            if (index == 0) {
                node.TEST_validate();

                if (node.header.is_external && !node.header.next_id.is_null()) {
                    Node next;
                    CHECK_OK(tree.acquire(node.header.next_id, false, next));

                    tree.release(std::move(next));
                }
            }
        });

        // Find the leftmost external node.
        Node node;
        CHECK_OK(tree.acquire(tree.m_root_id, false, node));
        while (!node.header.is_external) {
            const auto id = read_child_id(node, 0);
            tree.release(std::move(node));
            CHECK_OK(tree.acquire(id, false, node));
        }
        while (!node.header.next_id.is_null()) {
            Node right;
            CHECK_OK(tree.acquire(node.header.next_id, false, right));
            std::string lhs_buffer, rhs_buffer;
            Slice lhs_key;
            CHECK_OK(PayloadManager::collect_key(*tree.m_pager, lhs_buffer, read_cell(node, 0), &lhs_key));
            Slice rhs_key;
            CHECK_OK(PayloadManager::collect_key(*tree.m_pager, rhs_buffer, read_cell(right, 0), &rhs_key));
            CHECK_TRUE(lhs_key < rhs_key);
            CHECK_TRUE(right.header.prev_id == node.page.id());
            tree.release(std::move(node));
            node = std::move(right);
        }
        tree.release(std::move(node));
    }

    [[nodiscard]] static auto to_string(Tree &tree) -> std::string
    {
        std::string repr;
        PrinterData data;

        Node root;
        CHECK_OK(tree.acquire(tree.m_root_id, false, root));
        collect_levels(tree, data, std::move(root), 0);
        for (const auto &level : data.levels) {
            repr.append(level + '\n');
        }
        return repr;
    }
};

auto Tree::TEST_validate() -> void
{
    TreeValidator::validate_freelist(*this, *m_freelist.m_head);
    TreeValidator::validate_tree(*this);
}

auto Tree::TEST_to_string() -> std::string
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

auto Tree::TEST_to_string() -> std::string
{
}

auto Tree::TEST_validate() -> void
{
}

#endif // CALICODB_BUILD_TESTS

[[nodiscard]] static auto default_cursor_status() -> Status
{
    return Status::not_found("cursor is invalid");
}

Cursor::Cursor() = default;
Cursor::~Cursor() = default;

auto CursorImpl::is_valid() const -> bool
{
    return m_status.is_ok();
}

auto CursorImpl::status() const -> Status
{
    return m_status;
}

auto CursorImpl::fetch_payload() -> Status
{
    CALICODB_EXPECT_EQ(m_key_size, 0);
    CALICODB_EXPECT_EQ(m_value_size, 0);

    Node node;
    CALICODB_TRY(m_tree->acquire(m_loc.pid, false, node));

    Slice key, value;
    auto cell = read_cell(node, m_loc.index);
    auto s = PayloadManager::collect_key(*m_tree->m_pager, m_key, cell, &key);
    m_key_size = key.size();
    if (s.is_ok()) {
        s = PayloadManager::collect_value(*m_tree->m_pager, m_value, cell, &value);
        m_value_size = value.size();
    }
    m_tree->release(std::move(node));
    return s;
}

auto CursorImpl::key() const -> Slice
{
    CALICODB_EXPECT_TRUE(is_valid());
    return Slice {m_key}.truncate(m_key_size);
}

auto CursorImpl::value() const -> Slice
{
    CALICODB_EXPECT_TRUE(is_valid());
    return Slice {m_value}.truncate(m_value_size);
}

auto CursorImpl::seek_first() -> void
{
    m_key_size = 0;
    m_value_size = 0;

    Node lowest;
    auto s = m_tree->find_lowest(lowest);
    if (!s.is_ok()) {
        m_status = s;
        return;
    }
    if (lowest.header.cell_count) {
        seek_to(std::move(lowest), 0);
    } else {
        m_tree->release(std::move(lowest));
        m_status = Status::not_found("database is empty");
    }
}

auto CursorImpl::seek_last() -> void
{
    m_key_size = 0;
    m_value_size = 0;

    Node highest;
    auto s = m_tree->find_highest(highest);
    if (!s.is_ok()) {
        m_status = s;
        return;
    }
    if (const auto count = highest.header.cell_count) {
        seek_to(std::move(highest), count - 1);
    } else {
        m_tree->release(std::move(highest));
        m_status = Status::not_found("database is empty");
    }
}

auto CursorImpl::next() -> void
{
    CALICODB_EXPECT_TRUE(is_valid());
    m_key_size = 0;
    m_value_size = 0;

    Node node;
    auto s = m_tree->acquire(m_loc.pid, false, node);
    if (!s.is_ok()) {
        m_status = s;
        return;
    }
    if (++m_loc.index < m_loc.count) {
        seek_to(std::move(node), m_loc.index);
        return;
    }
    const auto next_id = node.header.next_id;
    m_tree->release(std::move(node));

    if (next_id.is_null()) {
        m_status = default_cursor_status();
        return;
    }
    s = m_tree->acquire(next_id, false, node);
    if (!s.is_ok()) {
        m_status = s;
        return;
    }
    seek_to(std::move(node), 0);
}

auto CursorImpl::previous() -> void
{
    CALICODB_EXPECT_TRUE(is_valid());
    m_key_size = 0;
    m_value_size = 0;

    Node node;
    auto s = m_tree->acquire(m_loc.pid, false, node);
    if (!s.is_ok()) {
        m_status = s;
        return;
    }
    if (m_loc.index != 0) {
        seek_to(std::move(node), m_loc.index - 1);
        return;
    }
    const auto prev_id = node.header.prev_id;
    m_tree->release(std::move(node));

    if (prev_id.is_null()) {
        m_status = default_cursor_status();
        return;
    }
    s = m_tree->acquire(prev_id, false, node);
    if (!s.is_ok()) {
        m_status = s;
        return;
    }
    const auto count = node.header.cell_count;
    seek_to(std::move(node), count - 1);
}

auto CursorImpl::seek_to(Node node, std::size_t index) -> void
{
    const auto &header = node.header;
    CALICODB_EXPECT_TRUE(header.is_external);

    if (header.cell_count && index < header.cell_count) {
        m_loc.index = static_cast<PageSize>(index);
        m_loc.count = header.cell_count;
        m_loc.pid = node.page.id();
        m_status = fetch_payload();
    } else {
        m_status = default_cursor_status();
    }
    m_tree->release(std::move(node));
}

auto CursorImpl::seek(const Slice &key) -> void
{
    m_key_size = 0;
    m_value_size = 0;

    Tree::SearchResult slot;
    auto s = m_tree->find_external(key, slot);
    if (!s.is_ok()) {
        m_status = s;
        return;
    }
    seek_to(std::move(slot.node), slot.index);
}

auto CursorInternal::make_cursor(Tree &tree) -> Cursor *
{
    auto *cursor = new CursorImpl {tree};
    invalidate(*cursor, default_cursor_status());
    return cursor;
}

auto CursorInternal::invalidate(const Cursor &cursor, Status status) -> void
{
    CALICODB_EXPECT_FALSE(status.is_ok());
    reinterpret_cast<const CursorImpl &>(cursor).m_status = std::move(status);
}

} // namespace calicodb
