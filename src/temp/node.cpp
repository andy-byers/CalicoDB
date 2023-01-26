#include "node.h"
#include "page/delta.h"
#include "utils/encoding.h"

namespace Calico {

using PagePtr = std::uint16_t;
using PageSize = std::uint16_t;
using ValueSize = std::uint32_t;

static auto header_offset(const Node_ &node)
{
    return FileHeader_::SIZE * node.page.id().is_root();
}

static auto cell_slots_offset(const Node_ &node)
{
    return header_offset(node) + NodeHeader_::SIZE;
}

static auto cell_area_offset(const Node_ &node)
{
    return cell_slots_offset(node) + node.header.cell_count*sizeof(PagePtr);
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
    return external_prefix_size() + meta.min_local + sizeof(Id);
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

auto parse_external_cell(const NodeMeta &meta, Byte *data) -> Cell_
{
    Cell_ cell;
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

auto parse_internal_cell(const NodeMeta &, Byte *data) -> Cell_
{
    Cell_ cell;
    cell.ptr = data;
    cell.key = data + internal_prefix_size();
    cell.key_size = internal_payload_size(data);
    cell.total_ps = cell.key_size;
    cell.local_ps = cell.key_size;
    cell.size = cell.key_size + internal_prefix_size();
    return cell;
}

Node_::Iterator::Iterator(Node_ &node)
    : m_node {&node}
{}

auto Node_::Iterator::is_valid() const -> bool
{
    return m_index < m_node->header.cell_count;
}

auto Node_::Iterator::index() const -> Size
{
    return m_index;
}

auto Node_::Iterator::key() const -> Slice
{
    CALICO_EXPECT_TRUE(is_valid());
    return m_node->read_key(m_node->get_slot(m_index));
}

auto Node_::Iterator::data() -> Byte *
{
    CALICO_EXPECT_TRUE(is_valid());
    return m_node->page.data() + m_node->get_slot(m_index);
}

auto Node_::Iterator::data() const -> const Byte *
{
    CALICO_EXPECT_TRUE(is_valid());
    return m_node->page.data() + m_node->get_slot(m_index);
}

auto Node_::Iterator::seek(const Slice &key) -> bool
{
    auto upper = static_cast<long>(m_node->header.cell_count);
    long lower {};

    while (lower < upper) {
        // Note that this cannot overflow since the page size is bounded by a 16-bit integer.
        const auto mid = (lower+upper) / 2;
        const auto ptr = m_node->get_slot(static_cast<Size>(mid));
        const auto rhs = m_node->read_key(ptr);

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

auto Node_::Iterator::next() -> void
{
    if (is_valid())
        m_index++;
}

Node_::Node_(Page inner, Byte *defragmentation_space)
    : page {std::move(inner)},
      scratch {defragmentation_space},
      header {page},
      slots_offset {NodeHeader_::SIZE}
{
    CALICO_EXPECT_NE(scratch, nullptr);

    if (page.id().is_root())
        slots_offset += FileHeader_::SIZE;

    if (header.cell_start == 0)
        header.cell_start = static_cast<PageSize>(page.size());

    const auto after_header = page_offset(page) + NodeHeader_::SIZE;
    const auto bottom = after_header + header.cell_count*sizeof(PagePtr);
    const auto top = header.cell_start;

    CALICO_EXPECT_GE(top, bottom);
    gap_size = static_cast<PagePtr>(top - bottom);
}

auto Node_::get_slot(Size index) const -> Size
{
    return get_u16(page.data() + slots_offset + index*sizeof(PagePtr));
}

auto Node_::set_slot(Size index, Size pointer) -> void
{
    return put_u16(page.span(slots_offset + index*sizeof(PagePtr), sizeof(PagePtr)), static_cast<PagePtr>(pointer));
}

auto Node_::insert_slot(Size index, Size pointer) -> void
{
    const auto offset = slots_offset + index*sizeof(PagePtr);
    const auto size = (header.cell_count-index) * sizeof(PagePtr);
    auto *data = page.data() + offset;

    std::memmove(data + sizeof(PagePtr), data, size);
    put_u16(data, static_cast<PagePtr>(pointer));

    insert_delta(page.m_deltas, {offset, size + sizeof(PagePtr)});
    header.cell_count++;
}

auto Node_::remove_slot(Size index) -> void
{
    const auto offset = slots_offset + index*sizeof(PagePtr);
    const auto size = (header.cell_count-index) * sizeof(PagePtr);
    auto *data = page.data() + offset;

    std::memmove(data, data + sizeof(PagePtr), size);

    insert_delta(page.m_deltas, {offset, size + sizeof(PagePtr)});
    header.cell_count--;
}


auto Node_::take() && -> Page
{
    if (page.is_writable())
        header.write(page);
    return std::move(page);
}

class BlockAllocator {
    Node_ *m_node {};

    [[nodiscard]]
    auto get_next_pointer(Size offset) -> PagePtr
    {
        return get_u16(m_node->page.data() + offset);
    }

    [[nodiscard]]
    auto get_block_size(Size offset) -> PagePtr
    {
        return get_u16(m_node->page.data() + offset + sizeof(PagePtr));
    }

    auto set_next_pointer(Size offset, PagePtr value) -> void
    {
        CALICO_EXPECT_LT(value, m_node->page.size());
        return put_u16(m_node->page.data() + offset, value);
    }

    auto set_block_size(Size offset, PagePtr value) -> void
    {
        CALICO_EXPECT_GE(value, 4);
        CALICO_EXPECT_LT(value, m_node->page.size());
        return put_u16(m_node->page.data() + offset + sizeof(PagePtr), value);
    }

    [[nodiscard]]
    auto allocate_from_free_list(PagePtr needed_size) -> PagePtr
    {
        PagePtr prev_ptr {};
        PagePtr curr_ptr {m_node->header.free_start};

        while (curr_ptr) {
            if (needed_size <= get_block_size(curr_ptr))
                return take_free_space(prev_ptr, curr_ptr, needed_size);
            prev_ptr = curr_ptr;
            curr_ptr = get_next_pointer(curr_ptr);
        }
        return 0;
    }

    [[nodiscard]]
    auto allocate_from_gap(PagePtr needed_size) -> PagePtr
    {
        if (needed_size <= m_node->gap_size) {
            m_node->header.cell_start -= needed_size;
            m_node->gap_size -= needed_size;
            return m_node->header.cell_start;
        }
        return 0;
    }

    [[nodiscard]]
    auto take_free_space(PagePtr ptr0, PagePtr ptr1, PagePtr needed_size) -> PagePtr
    {
        CALICO_EXPECT_LT(ptr0, m_node->page.size());
        CALICO_EXPECT_LT(ptr1, m_node->page.size());
        CALICO_EXPECT_LT(needed_size, m_node->page.size());
        const auto is_first = !ptr0;
        const auto ptr2 = get_next_pointer(ptr1);
        const auto free_size = get_block_size(ptr1);
        auto &header = m_node->header;

        CALICO_EXPECT_GE(free_size, needed_size);
        const auto diff = static_cast<PagePtr>(free_size - needed_size);

        if (diff < 4) {
            header.frag_count += diff;

            if (is_first) {
                header.free_start = static_cast<PagePtr>(ptr2);
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

public:
    explicit BlockAllocator(Node_ &node)
        : m_node {&node}
    {}

    [[nodiscard]]
    auto allocate(PageSize needed_size) -> PagePtr
    {
        CALICO_EXPECT_LT(needed_size, m_node->page.size());

        if (const auto offset = allocate_from_gap(needed_size))
            return offset;

        return allocate_from_free_list(needed_size);
    }

    auto free(PagePtr ptr, PagePtr size) -> void
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

    auto defragment(std::optional<PagePtr> skip_index) -> void
    {
        auto &header = m_node->header;
        const auto n = header.cell_count;
        const auto to_skip = skip_index ? *skip_index : n;
        auto end = static_cast<PagePtr>(m_node->page.size());
        auto ptr = m_node->page.data();
        std::vector<PagePtr> ptrs(n);

        for (Size index {}; index < n; ++index) {
            if (index == to_skip)
                continue;
            const auto offset = m_node->get_slot(index);
            const auto size = m_node->cell_size(offset);

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
    }
};

auto usable_space(const Node_ &node) -> bool
{
    return node.header.free_total + node.gap_size;
}

auto allocate_block(Node_ &node, PagePtr index, PagePtr size) -> Size
{
    const auto &header = node.header;
    const auto can_allocate = size + sizeof(PagePtr) <= usable_space(node);
    BlockAllocator alloc {node};

    CALICO_EXPECT_FALSE(node.overflow.has_value());
    CALICO_EXPECT_LE(index, header.cell_count);

    // We don't have room to insert the cell pointer.
    if (cell_area_offset(node) + sizeof(PagePtr) > header.cell_start) {
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

static auto free_block(Node_ &node, PagePtr index, PagePtr size) -> void
{
    BlockAllocator alloc {node};
    alloc.free(static_cast<PagePtr>(node.get_slot(index)), size);
    node.remove_slot(index);
}

auto read_cell(Node_ &node, Size index) -> Cell_
{
    return node.parse_cell(node.get_slot(index));
}

auto write_cell(Node_ &node, Size index, const Cell_ &cell) -> void
{
    if (const auto offset = allocate_block(node, PagePtr(index), PageSize(cell.size))) {
        auto memory = node.page.span(offset, cell.size);
        std::memcpy(memory.data(), cell.ptr, cell.size);
    } else {
        node.overflow_index = PagePtr(index);
        node.overflow = cell;
    }
}

auto erase_cell(Node_ &node, Size index) -> void
{
    erase_cell(node, index, node.cell_size(node.get_slot(index)));
}

auto erase_cell(Node_ &node, Size index, Size size_hint) -> void
{
    free_block(node, PagePtr(index), PageSize(size_hint));
}

auto emplace_cell(Byte *out, Size value_size, const Slice &key, const Slice &local_value, Id overflow_id) -> void
{
    put_u32(out, static_cast<std::uint32_t>(value_size));
    out += sizeof(std::uint32_t);

    put_u16(out, static_cast<std::uint16_t>(key.size()));
    out += sizeof(std::uint16_t);

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
    return sizeof(ValueSize) + sizeof(PageSize) + total_size;
}

auto manual_defragment(Node_ &node) -> void
{
    BlockAllocator alloc {node};
    alloc.defragment(std::nullopt);
}

auto detach_cell(Cell_ &cell, Byte *backing) -> void
{
    std::memcpy(backing, cell.ptr, cell.size);
    const auto diff = cell.key - cell.ptr;
    cell.ptr = backing;
    cell.key = backing + diff;
    cell.is_free = true;
}

auto promote_cell(Cell_ &cell) -> void
{
    // Pretend like there is a left child ID field. Now, when this cell is inserted into an internal node,
    // it can be copied over in one chunk. The caller will need to set the actual ID value later.
    cell.ptr -= EXTERNAL_SHIFT;
    cell.size = cell.key_size + internal_prefix_size();
    cell.total_ps = cell.key_size;
    cell.local_ps = cell.key_size;
}

auto read_child_id(const Node_ &node, Size index) -> Id
{
    const auto &header = node.header;
    CALICO_EXPECT_FALSE(header.is_external);
    if (index == header.cell_count) {
        return header.next_id;
    }
    return {get_u64(node.page.data() + node.get_slot(index))};
}

auto read_child_id(const Cell_ &cell) -> Id
{
    return {get_u64(cell.ptr)};
}

auto read_overflow_id(const Cell_ &cell) -> Id
{
    return {get_u64(cell.key + cell.local_ps)};
}

auto write_child_id(Node_ &node, Size index, Id child_id) -> void
{
    auto &header = node.header;
    CALICO_EXPECT_FALSE(header.is_external);
    if (index == header.cell_count) {
        header.next_id = child_id;
    }
    put_u64(node.page.data() + node.get_slot(index), child_id.value);
}

auto write_child_id(Cell_ &cell, Id child_id) -> void
{
    put_u64(cell.ptr, child_id.value);
}

} // namespace Calico

