#include "node.h"
#include "pager/delta.h"
#include "utils/encoding.h"

namespace Calico {

static auto header_offset(const Node &node)
{
    return FileHeader::SIZE * node.page.id().is_root();
}

static auto cell_slots_offset(const Node &node)
{
    return header_offset(node) + NodeHeader::SIZE;
}

static auto cell_area_offset(const Node &node)
{
    return cell_slots_offset(node) + node.header.cell_count*sizeof(PageSize);
}

static constexpr auto external_prefix_size() -> Size
{
    return sizeof(ValueSize) + sizeof(PageSize);
}

static constexpr auto internal_prefix_size() -> Size
{
    return sizeof(Id) + sizeof(PageSize);
}

static auto internal_payload_size(const Byte *data) -> Size
{
    return get_u16(data + sizeof(Id));
}

static auto external_payload_size(const Byte *data) -> Size
{
    return get_u32(data) + get_u16(data + sizeof(ValueSize));
}

auto external_cell_size(const NodeMeta &meta, const Byte *data) -> Size
{
    if (const auto ps = external_payload_size(data); ps <= meta.max_local)
        return external_prefix_size() + ps;
    return external_prefix_size() + std::max<Size>(get_u16(data + sizeof(ValueSize)), meta.min_local) + sizeof(Id);
}

auto internal_cell_size(const NodeMeta &, const Byte *data) -> Size
{
    return internal_prefix_size() + internal_payload_size(data);
}

auto read_external_key(const Byte *data) -> Slice
{
    return {data + external_prefix_size(), get_u16(data + sizeof(ValueSize))};
}

auto read_internal_key(const Byte *data) -> Slice
{
    return {data + internal_prefix_size(), get_u16(data + sizeof(Id))};
}

auto parse_external_cell(const NodeMeta &meta, Byte *data) -> Cell
{
    Cell cell;
    cell.ptr = data;
    cell.key = data + external_prefix_size();
    cell.total_ps = external_payload_size(data);
    cell.key_size = get_u16(data + sizeof(ValueSize));
    if (cell.total_ps > meta.max_local) {
        cell.local_ps = meta.min_local;
        // The entire key must be stored directly in the external node (none on an overflow page).
        if (cell.local_ps < cell.key_size)
            cell.local_ps = cell.key_size;
        cell.size = sizeof(Id);
    } else {
        cell.local_ps = cell.total_ps;
    }
    cell.size += cell.local_ps + external_prefix_size();
    return cell;
}

auto parse_internal_cell(const NodeMeta &, Byte *data) -> Cell
{
    Cell cell;
    cell.ptr = data;
    cell.key = data + internal_prefix_size();
    cell.key_size = internal_payload_size(data);
    cell.total_ps = cell.key_size;
    cell.local_ps = cell.key_size;
    cell.size = cell.key_size + internal_prefix_size();
    return cell;
}

[[nodiscard]]
static auto cell_size_direct(const Node &node, Size offset) -> Size
{
    return node.meta->cell_size(*node.meta, node.page.data() + offset);
}

class BlockAllocator {
    Node *m_node {};

    [[nodiscard]]
    auto get_next_pointer(Size offset) -> PageSize
    {
        return get_u16(m_node->page.data() + offset);
    }

    [[nodiscard]]
    auto get_block_size(Size offset) -> PageSize
    {
        return get_u16(m_node->page.data() + offset + sizeof(PageSize));
    }

    auto set_next_pointer(Size offset, PageSize value) -> void
    {
        CALICO_EXPECT_LT(value, m_node->page.size());
        return put_u16(m_node->page.data() + offset, value);
    }

    auto set_block_size(Size offset, PageSize value) -> void
    {
        CALICO_EXPECT_GE(value, 4);
        CALICO_EXPECT_LT(value, m_node->page.size());
        return put_u16(m_node->page.data() + offset + sizeof(PageSize), value);
    }

    [[nodiscard]] auto allocate_from_free_list(PageSize needed_size) -> PageSize;
    [[nodiscard]] auto allocate_from_gap(PageSize needed_size) -> PageSize;
    [[nodiscard]] auto take_free_space(PageSize ptr0, PageSize ptr1, PageSize needed_size) -> PageSize;

public:
    explicit BlockAllocator(Node &node)
        : m_node {&node}
    {}

    [[nodiscard]] auto allocate(PageSize needed_size) -> PageSize;
    auto free(PageSize ptr, PageSize size) -> void;
    auto defragment(std::optional<PageSize> skip_index) -> void;
};

auto BlockAllocator::allocate_from_free_list(PageSize needed_size) -> PageSize
{
    PageSize prev_ptr {};
    PageSize curr_ptr {m_node->header.free_start};

    while (curr_ptr) {
        if (needed_size <= get_block_size(curr_ptr))
            return take_free_space(prev_ptr, curr_ptr, needed_size);
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
    CALICO_EXPECT_LT(ptr0, m_node->page.size());
    CALICO_EXPECT_LT(ptr1, m_node->page.size());
    CALICO_EXPECT_LT(needed_size, m_node->page.size());
    const auto is_first = !ptr0;
    const auto ptr2 = get_next_pointer(ptr1);
    const auto free_size = get_block_size(ptr1);
    auto &header = m_node->header;

    CALICO_EXPECT_GE(free_size, needed_size);
    const auto diff = static_cast<PageSize>(free_size - needed_size);

    if (diff < 4) {
        header.frag_count += diff;

        if (is_first) {
            header.free_start = static_cast<PageSize>(ptr2);
        } else {
            set_next_pointer(ptr0, ptr2);
        }
    } else {
        set_block_size(ptr1, diff);
    }
    CALICO_EXPECT_GE(header.free_total, needed_size);
    header.free_total -= needed_size;
    return ptr1 + diff;
}

auto BlockAllocator::allocate(PageSize needed_size) -> PageSize
{
    CALICO_EXPECT_LT(needed_size, m_node->page.size());

    if (const auto offset = allocate_from_gap(needed_size))
        return offset;

    return allocate_from_free_list(needed_size);
}

auto BlockAllocator::free(PageSize ptr, PageSize size) -> void
{
    CALICO_EXPECT_GE(ptr, cell_area_offset(*m_node));
    CALICO_EXPECT_LE(ptr + size, m_node->page.size());
    auto &header = m_node->header;

    if (size < 4) {
        header.frag_count += size;
    } else {
        set_next_pointer(ptr, header.free_start);
        set_block_size(ptr, size);
        header.free_start = ptr;
    }
    header.free_total += size;
}

auto BlockAllocator::defragment(std::optional<PageSize> skip_index) -> void
{
    auto &header = m_node->header;
    const auto n = header.cell_count;
    const auto to_skip = skip_index ? *skip_index : n;
    auto end = static_cast<PageSize>(m_node->page.size());
    auto ptr = m_node->page.data();
    std::vector<PageSize> ptrs(n);

    for (Size index {}; index < n; ++index) {
        if (index == to_skip)
            continue;
        const auto offset = m_node->get_slot(index);
        const auto size = cell_size_direct(*m_node, offset);

        end -= PageSize(size);
        std::memcpy(m_node->scratch + end, ptr + offset, size);
        ptrs[index] = end;
    }
    for (Size index {}; index < n; ++index) {
        if (index == to_skip) continue;
        m_node->set_slot(index, ptrs[index]);
    }
    const auto offset = cell_area_offset(*m_node);
    const auto size = m_node->page.size() - offset;
    mem_copy(m_node->page.span(offset, size), {m_node->scratch + offset, size});

    header.cell_start = end;
    header.frag_count = 0;
    header.free_start = 0;
    header.free_total = 0;
    m_node->gap_size = PageSize(end - cell_area_offset(*m_node));
}

Node::Iterator::Iterator(Node &node)
    : m_node {&node}
{}

auto Node::Iterator::is_valid() const -> bool
{
    return m_index < m_node->header.cell_count;
}

auto Node::Iterator::index() const -> Size
{
    return m_index;
}

auto Node::Iterator::key() const -> Slice
{
    CALICO_EXPECT_TRUE(is_valid());
    return read_key(*m_node, m_index);
}

auto Node::Iterator::data() -> Byte *
{
    CALICO_EXPECT_TRUE(is_valid());
    return m_node->page.data() + m_node->get_slot(m_index);
}

auto Node::Iterator::data() const -> const Byte *
{
    CALICO_EXPECT_TRUE(is_valid());
    return m_node->page.data() + m_node->get_slot(m_index);
}

auto Node::Iterator::seek(const Slice &key) -> bool
{
    auto upper = static_cast<long>(m_node->header.cell_count);
    long lower {};

    while (lower < upper) {
        // Note that this cannot overflow since the page size is bounded by a 16-bit integer.
        const auto mid = (lower+upper) / 2;
        const auto rhs = read_key(*m_node, static_cast<Size>(mid));

        switch (compare_three_way(key, rhs)) {
            case ThreeWayComparison::EQ:
                m_index = static_cast<unsigned>(mid);
                return true;
            case ThreeWayComparison::LT:
                upper = mid;
                break;
            case ThreeWayComparison::GT:
                lower = mid + 1;
        }
    }
    m_index = static_cast<unsigned>(lower);
    return false;
}

auto Node::Iterator::next() -> void
{
    if (is_valid())
        m_index++;
}

Node::Node(Page inner, Byte *defragmentation_space)
    : page {std::move(inner)},
      scratch {defragmentation_space},
      header {page},
      slots_offset {NodeHeader::SIZE}
{
    if (page.id().is_root())
        slots_offset += FileHeader::SIZE;

    if (header.cell_start == 0)
        header.cell_start = static_cast<PageSize>(page.size());

    const auto after_header = page_offset(page) + NodeHeader::SIZE;
    const auto bottom = after_header + header.cell_count*sizeof(PageSize);
    const auto top = header.cell_start;

    CALICO_EXPECT_GE(top, bottom);
    gap_size = static_cast<PageSize>(top - bottom);
}

auto Node::get_slot(Size index) const -> Size
{
    CALICO_EXPECT_LT(index, header.cell_count);
    return get_u16(page.data() + slots_offset + index*sizeof(PageSize));
}

auto Node::set_slot(Size index, Size pointer) -> void
{
    CALICO_EXPECT_LT(index, header.cell_count);
    return put_u16(page.span(slots_offset + index*sizeof(PageSize), sizeof(PageSize)), static_cast<PageSize>(pointer));
}

auto Node::insert_slot(Size index, Size pointer) -> void
{
    CALICO_EXPECT_LE(index, header.cell_count);
    CALICO_EXPECT_GE(gap_size, sizeof(PageSize));
    const auto offset = slots_offset + index*sizeof(PageSize);
    const auto size = (header.cell_count-index) * sizeof(PageSize);
    auto *data = page.data() + offset;

    std::memmove(data + sizeof(PageSize), data, size);
    put_u16(data, static_cast<PageSize>(pointer));

    insert_delta(page.m_deltas, {offset, size + sizeof(PageSize)});
    gap_size -= PageSize(sizeof(PageSize));
    header.cell_count++;
}

auto Node::remove_slot(Size index) -> void
{
    CALICO_EXPECT_LT(index, header.cell_count);
    const auto offset = slots_offset + index*sizeof(PageSize);
    const auto size = (header.cell_count-index) * sizeof(PageSize);
    auto *data = page.data() + offset;

    std::memmove(data, data + sizeof(PageSize), size);

    insert_delta(page.m_deltas, {offset, size + sizeof(PageSize)});
    gap_size += sizeof(PageSize);
    header.cell_count--;
}

auto Node::take() && -> Page
{
    if (page.is_writable())
        header.write(page);
    return std::move(page);
}

auto Node::TEST_validate() const -> void
{
    std::vector<Byte> used(page.size());
    const auto account = [&x = used](auto from, auto size) {
        auto lower = begin(x) + long(from);
        auto upper = begin(x) + long(from) + long(size);
        CALICO_EXPECT_FALSE(std::any_of(lower, upper, [](auto byte) {
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
        CALICO_EXPECT_EQ(free_total + header.frag_count, header.free_total);
    }
    // Cell bodies. Also makes sure the cells are in order.
    for (Size n {}; n < header.cell_count; ++n) {
        const auto lhs_ptr = get_slot(n);
        const auto lhs_size = cell_size_direct(*this, lhs_ptr);
        const auto lhs_key = read_key_at(*this, lhs_ptr);
        account(lhs_ptr, lhs_size);

        if (n + 1 < header.cell_count) {
            const auto rhs_ptr = get_slot(n + 1);
            const auto rhs_key = read_key_at(*this, rhs_ptr);
            CALICO_EXPECT_LT(lhs_key, rhs_key);
        }
    }

    // Every byte should be accounted for, except for fragments.
    const auto total_bytes = std::accumulate(
        begin(used),
        end(used),
        header.frag_count,
        [](auto accum, auto next) {
            return accum + next;
        });
    CALICO_EXPECT_EQ(page.size(), total_bytes);
}

auto usable_space(const Node &node) -> Size
{
    return node.header.free_total + node.gap_size;
}

auto max_usable_space(const Node &node) -> Size
{
    return node.page.size() - cell_slots_offset(node);
}

auto allocate_block(Node &node, PageSize index, PageSize size) -> Size
{
    const auto &header = node.header;
    const auto can_allocate = size + sizeof(PageSize) <= usable_space(node);
    BlockAllocator alloc {node};

    CALICO_EXPECT_FALSE(node.overflow.has_value());
    CALICO_EXPECT_LE(index, header.cell_count);

    // We don't have room to insert the cell pointer.
    if (cell_area_offset(node) + sizeof(PageSize) > header.cell_start) {
        if (!can_allocate) {
            node.overflow_index = index;
            return 0;
        }
        alloc.defragment(std::nullopt);
    }
    // insert a dummy cell pointer to save the slot.
    node.insert_slot(index, node.page.size() - 1);

    auto offset = alloc.allocate(size);
    if (!offset && can_allocate) {
        alloc.defragment(index);
        offset = alloc.allocate(size);
    }

    if (!offset) {
        node.overflow_index = index;
        node.remove_slot(index);
        return 0;
    }
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
    if (const auto offset = allocate_block(node, PageSize(index), PageSize(cell.size))) {
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
    CALICO_EXPECT_LT(index, node.header.cell_count);
    free_block(node, PageSize(index), PageSize(size_hint));
}

auto emplace_cell(Byte *out, Size value_size, const Slice &key, const Slice &local_value, Id overflow_id) -> void
{
    put_u32(out, static_cast<ValueSize>(value_size));
    out += sizeof(ValueSize);

    put_u16(out, static_cast<PageSize>(key.size()));
    out += sizeof(PageSize);

    std::memcpy(out, key.data(), key.size());
    out += key.size();

    std::memcpy(out, local_value.data(), local_value.size());

    if (!overflow_id.is_null())
        put_u64(out + local_value.size(), overflow_id.value);
}

auto determine_cell_size(Size key_size, Size &value_size, const NodeMeta &meta) -> Size
{
    CALICO_EXPECT_NE(key_size, 0);
    CALICO_EXPECT_LE(key_size, meta.max_local);

    auto total_size = key_size + value_size;
    if (total_size > meta.max_local) {
        const auto remote_size = total_size - std::max(key_size, meta.min_local);
        total_size = total_size - remote_size + sizeof(Id);
        value_size -= remote_size;
    }
    return external_prefix_size() + total_size;
}

auto manual_defragment(Node &node) -> void
{
    BlockAllocator alloc {node};
    alloc.defragment(std::nullopt);
}

auto detach_cell(Cell &cell, Byte *backing) -> void
{
    std::memcpy(backing, cell.ptr, cell.size);
    const auto diff = cell.key - cell.ptr;
    cell.ptr = backing;
    cell.key = backing + diff;
    cell.is_free = true;
}

auto promote_cell(Cell &cell) -> void
{
    // Pretend like there is a left child ID field. Now, when this cell is inserted into an internal node,
    // it can be copied over in one chunk. The caller will need to set the actual ID value later.
    cell.ptr -= EXTERNAL_SHIFT;
    cell.size = cell.key_size + internal_prefix_size();
    cell.total_ps = cell.key_size;
    cell.local_ps = cell.key_size;
}

auto read_key_at(const Node &node, Size offset) -> Slice
{
    return node.meta->read_key(node.page.data() + offset);
}

auto read_child_id_at(const Node &node, Size offset) -> Id
{
    return {get_u64(node.page.data() + offset)};
}

auto write_child_id_at(Node &node, Size offset, Id child_id) -> void
{
    put_u64(node.page.data() + offset, child_id.value);
}

auto read_key(const Node &node, Size index) -> Slice
{
    CALICO_EXPECT_LT(index, node.header.cell_count);
    return read_key_at(node, node.get_slot(index));
}

auto read_key(const Cell &cell) -> Slice
{
    return {cell.key, cell.key_size};
}

auto read_child_id(const Node &node, Size index) -> Id
{
    const auto &header = node.header;
    CALICO_EXPECT_LE(index, header.cell_count);
    CALICO_EXPECT_FALSE(header.is_external);
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
    return {get_u64(cell.key + cell.local_ps)};
}

auto write_child_id(Node &node, Size index, Id child_id) -> void
{
    auto &header = node.header;
    CALICO_EXPECT_LE(index, header.cell_count);
    CALICO_EXPECT_FALSE(header.is_external);
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
    CALICO_EXPECT_EQ(root.header.next_id, child.page.id());
    if (child.header.free_total) {
        manual_defragment(child);
    }

    // Copy the cell content area.
    const auto offset = cell_area_offset(child);
    auto memory = root.page.span(offset, child.page.size() - offset);
    std::memcpy(memory.data(), child.page.data() + offset, memory.size());

    // Copy the header and cell pointers.
    memory = root.page.span(FileHeader::SIZE + NodeHeader::SIZE, child.header.cell_count * sizeof(PageSize));
    std::memcpy(memory.data(), child.page.data() + cell_slots_offset(child), memory.size());
    root.header = child.header;
}

} // namespace Calico

