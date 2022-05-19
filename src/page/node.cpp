
#include "node.h"

namespace cub {

NodeHeader::NodeHeader(PID id, MutBytes data)
    : m_header{data.range(NodeLayout::header_offset(id), NodeLayout::HEADER_SIZE)} {}

auto NodeHeader::parent_id() const -> PID
{
    return PID{get_uint32(m_header.range(NodeLayout::PARENT_ID_OFFSET))};
}

auto NodeHeader::right_sibling_id() const -> PID
{
    return PID{get_uint32(m_header.range(NodeLayout::RIGHT_SIBLING_ID_OFFSET))};
}

auto NodeHeader::rightmost_child_id() const -> PID
{
    return PID{get_uint32(m_header.range(NodeLayout::RIGHTMOST_CHILD_ID_OFFSET))};
}

auto NodeHeader::cell_count() const -> Size
{
    return get_uint16(m_header.range(NodeLayout::CELL_COUNT_OFFSET));
}

auto NodeHeader::free_count() const -> Size
{
    return get_uint16(m_header.range(NodeLayout::FREE_COUNT_OFFSET));
}

auto NodeHeader::cell_start() const -> Index
{
    return get_uint16(m_header.range(NodeLayout::CELL_START_OFFSET));
}

auto NodeHeader::free_start() const -> Index
{
    return get_uint16(m_header.range(NodeLayout::FREE_START_OFFSET));
}

auto NodeHeader::frag_count() const -> Size
{
    return get_uint16(m_header.range(NodeLayout::FRAG_COUNT_OFFSET));
}

auto NodeHeader::set_parent_id(PID parent_id) -> void
{
    put_uint32(m_header.range(NodeLayout::PARENT_ID_OFFSET), parent_id.value);
}

auto NodeHeader::set_right_sibling_id(PID right_sibling_id) -> void
{
    put_uint32(m_header.range(NodeLayout::RIGHT_SIBLING_ID_OFFSET), right_sibling_id.value);
}

auto NodeHeader::set_rightmost_child_id(PID rightmost_child_id) -> void
{
    put_uint32(m_header.range(NodeLayout::RIGHTMOST_CHILD_ID_OFFSET), rightmost_child_id.value);
}

auto NodeHeader::set_cell_count(Size cell_count) -> void
{
    CUB_EXPECT_BOUNDED_BY(uint16_t, cell_count);
    put_uint16(m_header.range(NodeLayout::CELL_COUNT_OFFSET), static_cast<uint16_t>(cell_count));
}

auto NodeHeader::set_free_count(Size free_count) -> void
{
    CUB_EXPECT_BOUNDED_BY(uint16_t, free_count);
    put_uint16(m_header.range(NodeLayout::FREE_COUNT_OFFSET), static_cast<uint16_t>(free_count));
}

auto NodeHeader::set_cell_start(Index cell_start) -> void
{
    CUB_EXPECT_BOUNDED_BY(uint16_t, cell_start);
    put_uint16(m_header.range(NodeLayout::CELL_START_OFFSET), static_cast<uint16_t>(cell_start));
}

auto NodeHeader::set_free_start(Index free_start) -> void
{
    CUB_EXPECT_BOUNDED_BY(uint16_t, free_start);
    put_uint16(m_header.range(NodeLayout::FREE_START_OFFSET), static_cast<uint16_t>(free_start));
}

auto NodeHeader::set_frag_count(Size frag_count) -> void
{
    CUB_EXPECT_BOUNDED_BY(uint16_t, frag_count);
    put_uint16(m_header.range(NodeLayout::FRAG_COUNT_OFFSET), static_cast<uint16_t>(frag_count));
}

auto Node::usable_space() const -> Size
{
    return m_usable_space;
}

Node::Node(Page page, bool reset_header)
    : m_header{page.id(), page.raw_data()}
    , m_page{std::move(page)}
{
    reset(reset_header);
    recompute_usable_space();
}

auto Node::is_external() const -> bool
{
    return m_page.type() == PageType::EXTERNAL_NODE;
}

auto Node::cell_count() const -> Size
{
    return m_header.cell_count();
}

auto Node::parent_id() const -> PID
{
    return m_header.parent_id();
}

auto Node::right_sibling_id() const -> PID
{
    CUB_EXPECT_TRUE(is_external());
    return m_header.right_sibling_id();
}

auto Node::rightmost_child_id() const -> PID
{
    CUB_EXPECT_FALSE(is_external());
    return m_header.rightmost_child_id();
}

auto Node::child_id(Index index) const -> PID
{
    CUB_EXPECT_FALSE(is_external());
    CUB_EXPECT_LE(index, cell_count());
    if (index < cell_count())
        return read_cell(index).left_child_id();
    return rightmost_child_id();
}

auto Node::set_parent_id(PID id) -> void
{
    m_header.set_parent_id(id);
}

auto Node::set_right_sibling_id(PID id) -> void
{
    m_header.set_right_sibling_id(id);
}

auto Node::set_rightmost_child_id(PID id) -> void
{
    m_header.set_rightmost_child_id(id);
}

auto Node::read_key(Index index) const -> RefBytes
{
    CUB_EXPECT_LT(index, cell_count());
    return read_cell(index).key();
}

auto Node::read_cell(Index index) const -> Cell
{
    CUB_EXPECT_LT(index, cell_count());
    return CellReader{m_page.type(), m_page.range(0)}.read(cell_pointer(index));
}

auto Node::detach_cell(Index index, Scratch scratch) const -> Cell
{
    CUB_EXPECT_LT(index, cell_count());
    auto cell = read_cell(index);
    cell.detach(std::move(scratch));
    return cell;
}

auto Node::extract_cell(Index index, Scratch scratch) -> Cell
{
    CUB_EXPECT_LT(index, cell_count());
    auto cell = detach_cell(index, std::move(scratch));
    remove_at(index, cell.size());
    return cell;
}

auto Node::find_ge(RefBytes key) const -> SearchResult
{
    long lower{};
    auto upper = static_cast<long>(cell_count()) - 1;
    
    while (lower <= upper) {
        const auto middle = (lower + upper) / 2;
        switch (compare_three_way(key, read_key(static_cast<Index>(middle)))) {
            case ThreeWayComparison::EQ:
                return {static_cast<Index>(middle), true};
            case ThreeWayComparison::LT:
                upper = middle - 1;
                break;
            case ThreeWayComparison::GT:
                lower = middle + 1;
        }
    }
    return {static_cast<Index>(lower), false};
}

auto Node::cell_pointers_offset() const -> Size
{
    return NodeLayout::content_offset(m_page.id());
}

auto Node::cell_area_offset() const -> Size
{
    return cell_pointers_offset() + CELL_POINTER_SIZE*cell_count();
}

auto Node::header_offset() const -> Index
{
    return NodeLayout::header_offset(m_page.id());
}

auto Node::recompute_usable_space() -> void
{
    auto usable_space = gap_size() + m_header.frag_count();
    for (Index i{}, ptr{m_header.free_start()}; i < m_header.free_count(); ++i) {
        usable_space += m_page.get_u16(ptr + CELL_POINTER_SIZE);
        ptr = m_page.get_u16(ptr);
    }
    CUB_EXPECT_LE(usable_space, m_page.size() - cell_pointers_offset());
    m_usable_space = usable_space;
}

auto Node::gap_size() const -> Size
{
    const auto top = m_header.cell_start();
    const auto bottom = cell_area_offset();
    CUB_EXPECT_GE(top, bottom);
    return top - bottom;
}

auto Node::cell_pointer(Index index) const -> Index
{
    CUB_EXPECT_LT(index, cell_count());
    return m_page.get_u16(cell_pointers_offset() + index*CELL_POINTER_SIZE);
}

auto Node::set_cell_pointer(Index index, Index cell_pointer) -> void
{
    CUB_EXPECT_LT(index, m_header.cell_count());
    CUB_EXPECT_LE(cell_pointer, m_page.size());
    m_page.put_u16(cell_pointers_offset() + index*CELL_POINTER_SIZE, static_cast<uint16_t>(cell_pointer));
}

auto Node::is_overflowing() const -> bool
{
    return m_overflow != std::nullopt;
}

auto Node::is_underflowing() const -> bool
{
    return !cell_count();
}

auto Node::overflow_cell() const -> const Cell&
{
    CUB_EXPECT_TRUE(is_overflowing());
    return *m_overflow;
}

auto Node::set_overflow_cell(Cell cell) -> void
{
    m_overflow = std::move(cell);
}

auto Node::take_overflow_cell() -> Cell
{
    auto cell = std::move(*m_overflow);
    CUB_EXPECT_EQ(m_overflow, std::nullopt); // TODO: Will this work? If so, move out in the return statement and avoid the temporary.
    m_overflow.reset(); // TODO: Or just move out like above?? probably will be necessary honestly...
    return cell;
}

auto Node::insert_cell_pointer(Index cid, Index cell_pointer) -> void
{
    CUB_EXPECT_GE(cell_pointer, cell_area_offset());
    CUB_EXPECT_LT(cell_pointer, m_page.size());
    CUB_EXPECT_LE(cid, m_header.cell_count());
    const auto start = NodeLayout::content_offset(m_page.id());
    const auto offset = start + CELL_POINTER_SIZE*cid;
    const auto size = (cell_count()-cid) * CELL_POINTER_SIZE;
    auto chunk = m_page.mut_range(offset, size + CELL_POINTER_SIZE);
    mem_move(chunk.range(CELL_POINTER_SIZE), chunk, size);//todo:changed checkme
    m_header.set_cell_count(cell_count() + 1);
    set_cell_pointer(cid, cell_pointer);
    m_usable_space -= CELL_POINTER_SIZE;
}

auto Node::remove_cell_pointer(Index cid) -> void
{
    CUB_EXPECT_GT(cell_count(), 0);
    CUB_EXPECT_LT(cid, m_header.cell_count());
    const auto start = NodeLayout::header_offset(m_page.id()) + NodeLayout::HEADER_SIZE;
    const auto offset = start + CELL_POINTER_SIZE*cid;
    const auto size = (cell_count()-cid-1) * CELL_POINTER_SIZE;
    auto chunk = m_page.mut_range(offset, size + CELL_POINTER_SIZE);
    mem_move(chunk, chunk.range(CELL_POINTER_SIZE), size);//todo:changed checkme
    m_header.set_cell_count(cell_count() - 1);
    m_usable_space += CELL_POINTER_SIZE;
}

auto Node::set_child_id(Index index, PID child_id) -> void
{
    CUB_EXPECT_FALSE(is_external());
    CUB_EXPECT_LE(index, m_header.cell_count());
    if (index < m_header.cell_count()) {
        m_page.put_u32(cell_pointer(index), child_id.value);
    } else {
        set_rightmost_child_id(child_id);
    }
}

auto Node::allocate_from_free(Size needed_size) -> Index
{
    // NOTE: We use a value of zero to indicate that there is no previous pointer.
    Index prev_ptr{};
    auto curr_ptr = m_header.free_start();

    for (Index i{}; i < m_header.free_count(); ++i) {
        if (needed_size <= m_page.get_u16(curr_ptr + sizeof(uint16_t)))
            return take_free_space(prev_ptr, curr_ptr, needed_size);
        prev_ptr = curr_ptr;
        curr_ptr = m_page.get_u16(curr_ptr);
    }
    return 0;
}

auto Node::allocate_from_gap(Size needed_size) -> Index
{
    if (needed_size <= gap_size()) {
        m_usable_space -= needed_size;
        const auto top = m_header.cell_start() - needed_size;
        m_header.set_cell_start(top);
        return top;
    }
    return 0;
}

auto Node::allocate(Size needed_size, std::optional<Index> skipped_cid) -> Index
{
    CUB_EXPECT_LT(needed_size, m_page.size() - NodeLayout::content_offset(m_page.id()));

    if (needed_size > m_usable_space)
        return 0;
    if (auto cell_ptr{allocate_from_free(needed_size)})
        return cell_ptr;
    if (auto cell_ptr{allocate_from_gap(needed_size)})
        return cell_ptr;
    defragment(skipped_cid);
    return allocate_from_gap(needed_size);
}

/* Free block layout:
 *     .--------------------.----------.-----------------.
 *     |  next_offset (2B)  |  n (2B)  |   payload (nB)  |
 *     '--------------------'----------'-----------------'
 */
auto Node::take_free_space(Index ptr0, Index ptr1, Size needed_size) -> Index
{
    CUB_EXPECT_LT(ptr0, m_page.size());
    CUB_EXPECT_LT(ptr1, m_page.size());
    CUB_EXPECT_LT(needed_size, m_page.size());
    const auto is_first = !ptr0;
    const auto ptr2 = m_page.get_u16(ptr1);
    const auto free_size = m_page.get_u16(ptr1 + sizeof(uint16_t));
    const auto diff = free_size - needed_size;
    CUB_EXPECT_BOUNDED_BY(uint16_t, diff);

    if (diff < 4) {
        m_header.set_frag_count(m_header.frag_count() + diff);
        m_header.set_free_count(m_header.free_count() - 1);
        if (is_first) {
            m_header.set_free_start(ptr2);
        } else {
            m_page.put_u16(ptr0, ptr2);
        }
    } else {
        // Adjust the size of the free block.
        m_page.put_u16(ptr1 + sizeof(uint16_t), static_cast<uint16_t>(diff));
    }
    m_usable_space -= needed_size;
    return ptr1 + diff;
}

auto Node::give_free_space(Index ptr, Size size) -> void
{
    CUB_EXPECT_LE(ptr + size, m_page.size());
    CUB_EXPECT_GE(ptr, NodeLayout::content_offset(m_page.id()));
    if (size < 4) {
        m_header.set_frag_count(m_header.frag_count() + size);
    } else {
        m_page.put_u16(ptr, uint16_t(m_header.free_start()));
        m_page.put_u16(ptr + CELL_POINTER_SIZE, uint16_t(size));
        m_header.set_free_count(m_header.free_count() + 1);
        m_header.set_free_start(ptr);
    }
    m_usable_space += size;
}

auto Node::defragment() -> void
{
    defragment(std::nullopt);
}

auto Node::defragment(std::optional<Index> skipped_cid) -> void
{
    const auto n = m_header.cell_count();
    const auto to_skip = skipped_cid ? *skipped_cid : n;
    auto end = m_page.size();
    std::string temp(end, '\x00');
    std::vector<Size> ptrs(n);

    for (Index index{}; index < n; ++index) {
        if (index == to_skip)
            continue;
        const auto cell = read_cell(index);
        end -= cell.size();
        cell.write({temp.data() + end, temp.size() - end});
        ptrs.at(index) = end;
    }
    for (Index cid{}; cid < n; ++cid) {
        if (cid != to_skip)
            set_cell_pointer(cid, ptrs.at(cid));
    }
    const auto offset = cell_area_offset();
    m_page.write(to_bytes(temp).range(offset, m_page.size() - offset), offset);
    m_header.set_cell_start(end);
    m_header.set_frag_count(0);
    m_header.set_free_count(0);
}

auto Node::insert(Cell cell) -> void
{
    const auto [index, should_be_false] = find_ge(cell.key());
    // Keys should be unique.
    CUB_EXPECT_FALSE(should_be_false);
    insert_at(index, std::move(cell));
}

auto Node::insert_at(Index index, Cell cell) -> void
{
    CUB_EXPECT_FALSE(is_overflowing());
    CUB_EXPECT_LE(index, m_header.cell_count());

    const auto local_size = cell.size();

    // We don't have room to insert the cell pointer.
    if (cell_area_offset() + CELL_POINTER_SIZE > m_header.cell_start()) {
        if (m_usable_space >= local_size + CELL_POINTER_SIZE) {
            defragment(std::nullopt);
            return insert_at(index, std::move(cell));
        }
        set_overflow_cell(std::move(cell));
        return;
    }
    // insert a dummy cell pointer to save the slot.
    insert_cell_pointer(index, m_page.size() - 1);

    // allocate space for the cell. This call may defragment the node.
    const auto offset = allocate(local_size, index);

    // We don't have room to insert the cell.
    if (!offset) {
        set_overflow_cell(std::move(cell));
        remove_cell_pointer(index);
        return;
    }
    // Now we can fill in the dummy cell pointer and write_all the cell.
    set_cell_pointer(index, offset);
    cell.write(m_page.mut_range(offset, cell.size()));

    // Adjust the start of the cell content area.
    if (offset < m_header.cell_start())
        m_header.set_cell_start(offset);
}

auto Node::remove(RefBytes key) -> bool
{
    if (auto [index, found_eq] = find_ge(key); found_eq) {
        remove_at(index, read_cell(index).size());
        return true;
    }
    return false;
}

auto Node::remove_at(Index index, Size local_size) -> void
{
    // TODO: Allow keys of zero size? Current comparison routine should still work. Assertion allows this currently.
    CUB_EXPECT_GE(local_size, Cell::MIN_HEADER_SIZE);
    CUB_EXPECT_LE(local_size, max_local(m_page.size()) + Cell::MAX_HEADER_SIZE);
    CUB_EXPECT_LT(index, m_header.cell_count());
    CUB_EXPECT_FALSE(is_overflowing());
    give_free_space(cell_pointer(index), local_size);
    remove_cell_pointer(index);
}

auto Node::reset(bool reset_header) -> void
{
    if (reset_header) {
        auto chunk = m_page.mut_range(header_offset(), NodeLayout::HEADER_SIZE);
        mem_clear(chunk, chunk.size());
        m_header.set_cell_start(m_page.size());
    }
    m_overflow = std::nullopt;
    recompute_usable_space();
}

} // cub