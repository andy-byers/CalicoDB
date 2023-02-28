#include "node.h"
#include "delta.h"
#include "encoding.h"
#include <algorithm>
#include <numeric>

namespace calicodb
{

static auto header_offset(const Node &node)
{
    return page_offset(node.page);
}

static auto cell_slots_offset(const Node &node)
{
    return header_offset(node) + NodeHeader::SIZE;
}

static auto cell_area_offset(const Node &node)
{
    return cell_slots_offset(node) + node.header.cell_count * sizeof(PageSize);
}

auto internal_cell_size(const NodeMeta &meta, const Byte *data) -> Size
{
    Size key_size;
    const auto *ptr = decode_varint(data + sizeof(Id), key_size);
    const auto local_size = compute_local_size(key_size, 0, meta.min_local, meta.max_local);
    const auto extra_size = (local_size < key_size) * sizeof(Id);
    const auto header_size = static_cast<Size>(ptr - data);
    return header_size + local_size + extra_size;
}

auto external_cell_size(const NodeMeta &meta, const Byte *data) -> Size
{
    Size key_size, value_size;
    const auto *ptr = decode_varint(data, value_size);
    ptr = decode_varint(ptr, key_size);
    const auto local_size = compute_local_size(key_size, value_size, meta.min_local, meta.max_local);
    const auto extra_size = (local_size < key_size + value_size) * sizeof(Id);
    const auto header_size = static_cast<Size>(ptr - data);
    return header_size + local_size + extra_size;
}

auto parse_external_cell(const NodeMeta &meta, Byte *data) -> Cell
{
    Size key_size, value_size;
    const auto *ptr = decode_varint(data, value_size);
    ptr = decode_varint(ptr, key_size);
    const auto header_size = static_cast<Size>(ptr - data);

    Cell cell;
    cell.ptr = data;
    cell.key = data + header_size;

    cell.key_size = key_size;
    cell.local_size = compute_local_size(key_size, value_size, meta.min_local, meta.max_local);
    cell.has_remote = cell.local_size < key_size + value_size;
    cell.size = header_size + cell.local_size + cell.has_remote * sizeof(Id);
    return cell;
}

auto parse_internal_cell(const NodeMeta &meta, Byte *data) -> Cell
{
    Size key_size;
    const auto *ptr = data + sizeof(Id);
    ptr = decode_varint(ptr, key_size);
    const auto header_size = static_cast<Size>(ptr - data);

    Cell cell;
    cell.ptr = data;
    cell.key = data + header_size;

    cell.key_size = key_size;
    cell.local_size = compute_local_size(key_size, 0, meta.min_local, meta.max_local);
    cell.has_remote = cell.local_size < key_size;
    cell.size = header_size + cell.local_size + cell.has_remote * sizeof(Id);
    return cell;
}

[[nodiscard]] static auto cell_size_direct(const Node &node, Size offset) -> Size
{
    return node.meta->cell_size(*node.meta, node.page.data() + offset);
}

class BlockAllocator
{
    Node *m_node {};

    [[nodiscard]] auto get_next_pointer(Size offset) -> PageSize
    {
        return get_u16(m_node->page.data() + offset);
    }

    [[nodiscard]] auto get_block_size(Size offset) -> PageSize
    {
        return get_u16(m_node->page.data() + offset + sizeof(PageSize));
    }

    auto set_next_pointer(Size offset, PageSize value) -> void
    {
        CDB_EXPECT_LT(value, m_node->page.size());
        return put_u16(m_node->page.span(offset, sizeof(PageSize)), value);
    }

    auto set_block_size(Size offset, PageSize value) -> void
    {
        CDB_EXPECT_GE(value, 4);
        CDB_EXPECT_LT(value, m_node->page.size());
        return put_u16(m_node->page.span(offset + sizeof(PageSize), sizeof(PageSize)), value);
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
    if (needed_size <= m_node->gap_size) {
        m_node->gap_size -= needed_size;
        return m_node->header.cell_start -= needed_size;
    }
    return 0;
}

auto BlockAllocator::take_free_space(PageSize ptr0, PageSize ptr1, PageSize needed_size) -> PageSize
{
    CDB_EXPECT_LT(ptr0, m_node->page.size());
    CDB_EXPECT_LT(ptr1, m_node->page.size());
    CDB_EXPECT_LT(needed_size, m_node->page.size());

    const auto is_first = !ptr0;
    const auto ptr2 = get_next_pointer(ptr1);
    const auto free_size = get_block_size(ptr1);
    auto &header = m_node->header;

    // Caller should make sure it isn't possible to overflow this byte.
    CDB_EXPECT_LE(header.frag_count + 3, 0xFF);

    CDB_EXPECT_GE(free_size, needed_size);
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
    CDB_EXPECT_GE(header.free_total, needed_size);
    header.free_total -= needed_size;
    return ptr1 + diff;
}

auto BlockAllocator::allocate(PageSize needed_size) -> PageSize
{
    CDB_EXPECT_LT(needed_size, m_node->page.size());

    if (const auto offset = allocate_from_gap(needed_size)) {
        return offset;
    }
    return allocate_from_free_list(needed_size);
}

auto BlockAllocator::free(PageSize ptr, PageSize size) -> void
{
    CDB_EXPECT_GE(ptr, cell_area_offset(*m_node));
    CDB_EXPECT_LE(ptr + size, m_node->page.size());
    auto &header = m_node->header;
    CDB_EXPECT_LE(header.frag_count + 3, 0xFF);

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

    for (Size index {}; index < n; ++index) {
        if (index == to_skip) {
            continue;
        }
        const auto offset = m_node->get_slot(index);
        const auto size = cell_size_direct(*m_node, offset);

        end -= PageSize(size);
        std::memcpy(m_node->scratch + end, ptr + offset, size);
        ptrs[index] = end;
    }
    for (Size index {}; index < n; ++index) {
        if (index == to_skip) {
            continue;
        }
        m_node->set_slot(index, ptrs[index]);
    }
    const auto offset = cell_area_offset(*m_node);
    const auto size = m_node->page.size() - offset;
    mem_copy(m_node->page.span(offset, size), {m_node->scratch + offset, size});

    header.cell_start = end;
    header.frag_count = 0;
    header.free_start = 0;
    header.free_total = 0;
    m_node->gap_size = static_cast<PageSize>(end - cell_area_offset(*m_node));
}

auto Node::initialize() -> void
{

    slots_offset = static_cast<PageSize>(page_offset(page) + NodeHeader::SIZE);

    if (header.cell_start == 0) {
        header.cell_start = static_cast<PageSize>(page.size());
    }

    const auto after_header = page_offset(page) + NodeHeader::SIZE;
    const auto bottom = after_header + header.cell_count * sizeof(PageSize);
    const auto top = header.cell_start;

    CDB_EXPECT_GE(top, bottom);
    gap_size = static_cast<PageSize>(top - bottom);
}

auto Node::get_slot(Size index) const -> Size
{
    CDB_EXPECT_LT(index, header.cell_count);
    return get_u16(page.data() + slots_offset + index * sizeof(PageSize));
}

auto Node::set_slot(Size index, Size pointer) -> void
{
    CDB_EXPECT_LT(index, header.cell_count);
    return put_u16(page.span(slots_offset + index * sizeof(PageSize), sizeof(PageSize)), static_cast<PageSize>(pointer));
}

auto Node::insert_slot(Size index, Size pointer) -> void
{
    CDB_EXPECT_LE(index, header.cell_count);
    CDB_EXPECT_GE(gap_size, sizeof(PageSize));
    const auto offset = slots_offset + index * sizeof(PageSize);
    const auto size = (header.cell_count - index) * sizeof(PageSize);
    auto *data = page.data() + offset;

    std::memmove(data + sizeof(PageSize), data, size);
    put_u16(data, static_cast<PageSize>(pointer));

    insert_delta(page.m_deltas, {offset, size + sizeof(PageSize)});
    gap_size -= static_cast<PageSize>(sizeof(PageSize));
    ++header.cell_count;
}

auto Node::remove_slot(Size index) -> void
{
    CDB_EXPECT_LT(index, header.cell_count);
    const auto offset = slots_offset + index * sizeof(PageSize);
    const auto size = (header.cell_count - index) * sizeof(PageSize);
    auto *data = page.data() + offset;

    std::memmove(data, data + sizeof(PageSize), size);

    insert_delta(page.m_deltas, {offset, size + sizeof(PageSize)});
    gap_size += sizeof(PageSize);
    --header.cell_count;
}

auto Node::take() && -> Page
{
    if (page.is_writable()) {
        header.write(page);
    }
    return std::move(page);
}

auto Node::TEST_validate() -> void
{
#if not NDEBUG
    CDB_EXPECT_LE(header.frag_count + 3, 0xFF);
    std::vector<Byte> used(page.size());
    const auto account = [&x = used](auto from, auto size) {
        auto lower = begin(x) + long(from);
        auto upper = begin(x) + long(from) + long(size);
        CDB_EXPECT_FALSE(std::any_of(lower, upper, [](auto byte) {
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
        const Byte *data = page.data();
        Size free_total {};
        while (i) {
            const auto size = get_u16(data + i + sizeof(PageSize));
            account(i, size);
            i = get_u16(data + i);
            free_total += size;
        }
        CDB_EXPECT_EQ(free_total + header.frag_count, header.free_total);
    }
    // Cell bodies. Also makes sure the cells are in order.
    for (Size n {}; n < header.cell_count; ++n) {
        const auto lhs_ptr = get_slot(n);
        const auto lhs_cell = read_cell_at(*this, lhs_ptr);
        account(lhs_ptr, lhs_cell.size);

        if (n + 1 < header.cell_count) {
            const auto rhs_ptr = get_slot(n + 1);
            const auto rhs_cell = read_cell_at(*this, rhs_ptr);
            if (!lhs_cell.has_remote && !rhs_cell.has_remote) {
                Slice lhs_key {lhs_cell.key, lhs_cell.key_size};
                Slice rhs_key {rhs_cell.key, rhs_cell.key_size};
                CDB_EXPECT_LT(lhs_key, rhs_key);
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
    CDB_EXPECT_EQ(page.size(), Size(total_bytes));
#endif // not NDEBUG
}

auto usable_space(const Node &node) -> Size
{
    return node.header.free_total + node.gap_size;
}

auto allocate_block(Node &node, PageSize index, PageSize size) -> Size
{
    CDB_EXPECT_LE(index, node.header.cell_count);

    if (size + sizeof(PageSize) > usable_space(node)) {
        node.overflow_index = index;
        return 0;
    }

    BlockAllocator alloc {node};

    // We don't have room to insert the cell pointer.
    if (node.gap_size < sizeof(PageSize)) {
        alloc.defragment(std::nullopt);
    }
    // insert a dummy cell pointer to save the slot.
    node.insert_slot(index, node.page.size() - 1);

    auto offset = alloc.allocate(size);
    if (offset == 0) {
        alloc.defragment(index);
        offset = alloc.allocate(size);
    }
    // We already made sure we had enough room to fulfill the request. If we had to defragment, the call
    // to allocate() following defragmentation should succeed.
    CDB_EXPECT_NE(offset, 0);
    node.set_slot(index, offset);

    // Signal that there will be a change here, but don't write anything yet.
    (void)node.page.span(offset, size);
    return offset;
}

static auto free_block(Node &node, PageSize index, PageSize size) -> void
{
    BlockAllocator alloc {node};
    alloc.free(static_cast<PageSize>(node.get_slot(index)), size);
    node.remove_slot(index);
}

auto read_cell_at(Node &node, Size offset) -> Cell
{
    return node.meta->parse_cell(*node.meta, node.page.data() + offset);
}

auto read_cell(Node &node, Size index) -> Cell
{
    return read_cell_at(node, node.get_slot(index));
}

auto write_cell(Node &node, Size index, const Cell &cell) -> Size
{
    if (const auto offset = allocate_block(node, static_cast<PageSize>(index), static_cast<PageSize>(cell.size))) {
        auto memory = node.page.span(offset, cell.size);
        std::memcpy(memory.data(), cell.ptr, cell.size);
        return offset;
    }
    node.overflow_index = PageSize(index);
    node.overflow = cell;
    return 0;
}

auto erase_cell(Node &node, Size index) -> void
{
    erase_cell(node, index, cell_size_direct(node, node.get_slot(index)));
}

auto erase_cell(Node &node, Size index, Size size_hint) -> void
{
    CDB_EXPECT_LT(index, node.header.cell_count);
    free_block(node, PageSize(index), PageSize(size_hint));
}

auto emplace_cell(Byte *out, Size key_size, Size value_size, const Slice &local_key, const Slice &local_value, Id overflow_id) -> Byte *
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

auto detach_cell(Cell &cell, Byte *backing) -> void
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

auto read_child_id_at(const Node &node, Size offset) -> Id
{
    return {get_u64(node.page.data() + offset)};
}

auto write_child_id_at(Node &node, Size offset, Id child_id) -> void
{
    put_u64(node.page.span(offset, sizeof(Id)), child_id.value);
}

auto read_child_id(const Node &node, Size index) -> Id
{
    const auto &header = node.header;
    CDB_EXPECT_LE(index, header.cell_count);
    CDB_EXPECT_FALSE(header.is_external);
    if (index == header.cell_count) {
        return header.next_id;
    }
    return read_child_id_at(node, node.get_slot(index));
}

auto read_child_id(const Cell &cell) -> Id
{
    return {get_u64(cell.ptr)};
}

auto read_overflow_id(const Cell &cell) -> Id
{
    return {get_u64(cell.key + cell.local_size)};
}

auto write_overflow_id(Cell &cell, Id overflow_id) -> void
{
    put_u64(cell.key + cell.local_size, overflow_id.value);
}

auto write_child_id(Node &node, Size index, Id child_id) -> void
{
    auto &header = node.header;
    CDB_EXPECT_LE(index, header.cell_count);
    CDB_EXPECT_FALSE(header.is_external);
    if (index == header.cell_count) {
        header.next_id = child_id;
    } else {
        write_child_id_at(node, node.get_slot(index), child_id);
    }
}

auto write_child_id(Cell &cell, Id child_id) -> void
{
    put_u64(cell.ptr, child_id.value);
}

auto merge_root(Node &root, Node &child) -> void
{
    CDB_EXPECT_EQ(root.header.next_id, child.page.id());
    const auto &header = child.header;
    if (header.free_total) {
        manual_defragment(child);
    }

    // Copy the cell content area.
    CDB_EXPECT_GE(header.cell_start, FileHeader::SIZE + NodeHeader::SIZE);
    auto memory = root.page.span(header.cell_start, child.page.size() - header.cell_start);
    std::memcpy(memory.data(), child.page.data() + header.cell_start, memory.size());

    // Copy the header and cell pointers.
    memory = root.page.span(FileHeader::SIZE + NodeHeader::SIZE, header.cell_count * sizeof(PageSize));
    std::memcpy(memory.data(), child.page.data() + cell_slots_offset(child), memory.size());
    root.header = header;
    root.meta = child.meta;
}

} // namespace calicodb
