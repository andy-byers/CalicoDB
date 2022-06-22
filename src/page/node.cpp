#include "node.h"

#include "utils/crc.h"
#include "utils/encoding.h"
#include "utils/layout.h"

namespace calico {

auto NodeHeader::header_crc() const -> Index
{
    return m_page->get_u32(header_offset() + NodeLayout::HEADER_CRC_OFFSET);
}

auto NodeHeader::parent_id() const -> PID
{
    return PID {get_uint32(m_page->range(header_offset() + NodeLayout::PARENT_ID_OFFSET))};
}

auto NodeHeader::right_sibling_id() const -> PID
{
    CALICO_EXPECT_EQ(m_page->type(), PageType::EXTERNAL_NODE);
    return PID {get_uint32(m_page->range(header_offset() + NodeLayout::RIGHT_SIBLING_ID_OFFSET))};
}

auto NodeHeader::rightmost_child_id() const -> PID
{
    CALICO_EXPECT_NE(m_page->type(), PageType::EXTERNAL_NODE);
    return PID {get_uint32(m_page->range(header_offset() + NodeLayout::RIGHTMOST_CHILD_ID_OFFSET))};
}

auto NodeHeader::cell_count() const -> Size
{
    return m_page->get_u16(header_offset() + NodeLayout::CELL_COUNT_OFFSET);
}

auto NodeHeader::free_count() const -> Size
{
    return m_page->get_u16(header_offset() + NodeLayout::FREE_COUNT_OFFSET);
}

auto NodeHeader::cell_start() const -> Index
{
    return m_page->get_u16(header_offset() + NodeLayout::CELL_START_OFFSET);
}

auto NodeHeader::free_start() const -> Index
{
    return m_page->get_u16(header_offset() + NodeLayout::FREE_START_OFFSET);
}

auto NodeHeader::frag_count() const -> Size
{
    return m_page->get_u16(header_offset() + NodeLayout::FRAG_COUNT_OFFSET);
}

auto NodeHeader::update_header_crc() -> void
{
    const auto offset = header_offset() + NodeLayout::HEADER_CRC_OFFSET;
    // This includes the old crc value in the new one.
    m_page->put_u32(offset, crc_32(m_page->range(header_offset(), NodeLayout::HEADER_SIZE)));
}

auto NodeHeader::set_parent_id(PID parent_id) -> void
{
    CALICO_EXPECT_NE(m_page->id(), PID::root());
    const auto offset = header_offset() + NodeLayout::PARENT_ID_OFFSET;
    m_page->put_u32(offset, parent_id.value);
}

auto NodeHeader::set_right_sibling_id(PID right_sibling_id) -> void
{
    CALICO_EXPECT_EQ(m_page->type(), PageType::EXTERNAL_NODE);
    const auto offset = header_offset() + NodeLayout::RIGHT_SIBLING_ID_OFFSET;
    m_page->put_u32(offset, right_sibling_id.value);
}

auto NodeHeader::set_rightmost_child_id(PID rightmost_child_id) -> void
{
    CALICO_EXPECT_NE(m_page->type(), PageType::EXTERNAL_NODE);
    const auto offset = header_offset() + NodeLayout::RIGHTMOST_CHILD_ID_OFFSET;
    m_page->put_u32(offset, rightmost_child_id.value);
}

auto NodeHeader::set_cell_count(Size cell_count) -> void
{
    CALICO_EXPECT_BOUNDED_BY(uint16_t, cell_count);
    m_page->put_u16(header_offset() + NodeLayout::CELL_COUNT_OFFSET, static_cast<uint16_t>(cell_count));
}

auto NodeHeader::set_free_count(Size free_count) -> void
{
    CALICO_EXPECT_BOUNDED_BY(uint16_t, free_count);
    m_page->put_u16(header_offset() + NodeLayout::FREE_COUNT_OFFSET, static_cast<uint16_t>(free_count));
}

auto NodeHeader::set_cell_start(Index cell_start) -> void
{
    CALICO_EXPECT_BOUNDED_BY(uint16_t, cell_start);
    m_page->put_u16(header_offset() + NodeLayout::CELL_START_OFFSET, static_cast<uint16_t>(cell_start));
}

auto NodeHeader::set_free_start(Index free_start) -> void
{
    CALICO_EXPECT_BOUNDED_BY(uint16_t, free_start);
    m_page->put_u16(header_offset() + NodeLayout::FREE_START_OFFSET, static_cast<uint16_t>(free_start));
}

auto NodeHeader::set_frag_count(Size frag_count) -> void
{
    CALICO_EXPECT_BOUNDED_BY(uint16_t, frag_count);
    m_page->put_u16(header_offset() + NodeLayout::FRAG_COUNT_OFFSET, static_cast<uint16_t>(frag_count));
}

auto NodeHeader::cell_pointers_offset() const -> Size
{
    return NodeLayout::content_offset(m_page->id());
}

auto NodeHeader::cell_area_offset() const -> Size
{
    const auto cell_count = m_page->get_u16(header_offset() + NodeLayout::CELL_COUNT_OFFSET);
    return cell_pointers_offset() + CELL_POINTER_SIZE*cell_count;
}

auto NodeHeader::header_offset() const -> Index
{
    return NodeLayout::header_offset(m_page->id());
}

auto NodeHeader::gap_size() const -> Size
{
    const auto top = cell_start();
    const auto bottom = cell_area_offset();
    CALICO_EXPECT_GE(top, bottom);
    return top - bottom;
}

auto NodeHeader::max_usable_space() const -> Size
{
    return m_page->size() - cell_pointers_offset();
}

auto CellDirectory::get_pointer(Index index) const -> Pointer
{
    CALICO_EXPECT_LT(index, m_header->cell_count());
    return {m_page->get_u16(m_header->cell_pointers_offset() + index * CELL_POINTER_SIZE)};
}

auto CellDirectory::set_pointer(Index index, Pointer pointer) -> void
{
    CALICO_EXPECT_LT(index, m_header->cell_count());
    CALICO_EXPECT_LE(pointer.value, m_page->size());
    m_page->put_u16(m_header->cell_pointers_offset() + index*CELL_POINTER_SIZE, static_cast<uint16_t>(pointer.value));
}

auto CellDirectory::insert_pointer(Index index, Pointer pointer) -> void
{
    CALICO_EXPECT_GE(pointer.value, m_header->cell_area_offset());
    CALICO_EXPECT_LT(pointer.value, m_page->size());
    CALICO_EXPECT_LE(index, m_header->cell_count());
    const auto start = NodeLayout::content_offset(m_page->id());
    const auto offset = start + CELL_POINTER_SIZE*index;
    const auto size = (m_header->cell_count()-index) * CELL_POINTER_SIZE;
    auto chunk = m_page->mut_range(offset, size + CELL_POINTER_SIZE);
    mem_move(chunk.range(CELL_POINTER_SIZE), chunk, size);
    m_header->set_cell_count(m_header->cell_count() + 1);
    set_pointer(index, pointer);
}

auto CellDirectory::remove_pointer(Index index) -> void
{
    CALICO_EXPECT_GT(m_header->cell_count(), 0);
    CALICO_EXPECT_LT(index, m_header->cell_count());
    const auto start = NodeLayout::header_offset(m_page->id()) + NodeLayout::HEADER_SIZE;
    const auto offset = start + CELL_POINTER_SIZE*index;
    const auto size = (m_header->cell_count()-index-1) * CELL_POINTER_SIZE;
    auto chunk = m_page->mut_range(offset, size + CELL_POINTER_SIZE);
    mem_move(chunk, chunk.range(CELL_POINTER_SIZE), size);
    m_header->set_cell_count(m_header->cell_count() - 1);
}

BlockAllocator::BlockAllocator(NodeHeader &header)
    : m_page {&header.page()}
      , m_header {&header}
{
    // Don't compute the free block total yet. If the page is from the freelist, the header may contain junk.
}

auto BlockAllocator::reset() -> void
{
    m_header->set_frag_count(0);
    m_header->set_free_count(0);
    recompute_usable_space();
}

auto BlockAllocator::compute_free_total() const -> Size
{
    auto usable_space = m_header->frag_count();
    for (Index i {}, ptr {m_header->free_start()}; i < m_header->free_count(); ++i) {
        usable_space += get_block_size(ptr);
        ptr = get_next_pointer(ptr);
    }
    CALICO_EXPECT_LE(usable_space, m_page->size() - m_header->cell_pointers_offset());
    return usable_space;
}

auto BlockAllocator::recompute_usable_space() -> void
{
    m_free_total = compute_free_total();
}

auto BlockAllocator::usable_space() const -> Size
{
    return m_free_total + m_header->gap_size();
}

auto BlockAllocator::get_next_pointer(Index offset) const -> Index
{
    return m_page->get_u16(offset);
}

auto BlockAllocator::get_block_size(Index offset) const -> Size
{
    return m_page->get_u16(offset + CELL_POINTER_SIZE);
}

auto BlockAllocator::set_next_pointer(Index offset, Index next_pointer) -> void
{
    CALICO_EXPECT_LT(next_pointer, m_page->size());
    return m_page->put_u16(offset, static_cast<uint16_t>(next_pointer));
}

auto BlockAllocator::set_block_size(Index offset, Size block_size) -> void
{
    CALICO_EXPECT_GE(block_size, CELL_POINTER_SIZE + sizeof(uint16_t));
    CALICO_EXPECT_LT(block_size, m_header->max_usable_space());
    return m_page->put_u16(offset + CELL_POINTER_SIZE, static_cast<uint16_t>(block_size));
}

auto BlockAllocator::allocate_from_free(Size needed_size) -> Index
{
    // NOTE: We use a value of zero to indicate that there is no previous pointer.
    Index prev_ptr {};
    auto curr_ptr = m_header->free_start();

    for (Index i {}; i < m_header->free_count(); ++i) {
        if (needed_size <= get_block_size(curr_ptr))
            return take_free_space(prev_ptr, curr_ptr, needed_size);
        prev_ptr = curr_ptr;
        curr_ptr = get_next_pointer(curr_ptr);
    }
    return 0;
}

auto BlockAllocator::allocate_from_gap(Size needed_size) -> Index
{
    if (needed_size <= m_header->gap_size()) {
        const auto top = m_header->cell_start() - needed_size;
        m_header->set_cell_start(top);
        return top;
    }
    return 0;
}

auto BlockAllocator::allocate(Size needed_size) -> Index
{
    CALICO_EXPECT_LT(needed_size, m_page->size() - NodeLayout::content_offset(m_page->id()));

    if (needed_size > usable_space())
        return 0;
    if (auto ptr = allocate_from_free(needed_size))
        return ptr;
    return allocate_from_gap(needed_size);
}

/* Free block layout:
 *     .---------------------.-------------.---------------------------.
 *     |  Next Pointer (2B)  |  Size (2B)  |   Free Memory (Size-4 B)  |
 *     '---------------------'-------------'---------------------------'
 */
auto BlockAllocator::take_free_space(Index ptr0, Index ptr1, Size needed_size) -> Index
{
    CALICO_EXPECT_LT(ptr0, m_page->size());
    CALICO_EXPECT_LT(ptr1, m_page->size());
    CALICO_EXPECT_LT(needed_size, m_page->size());
    const auto is_first = !ptr0;
    const auto ptr2 = get_next_pointer(ptr1);
    const auto free_size = get_block_size(ptr1);
    CALICO_EXPECT_GE(free_size, needed_size);
    const auto diff = free_size - needed_size;

    if (diff < 4) {
        m_header->set_frag_count(m_header->frag_count() + diff);
        m_header->set_free_count(m_header->free_count() - 1);
        if (is_first) {
            m_header->set_free_start(ptr2);
        } else {
            set_next_pointer(ptr0, ptr2);
        }
    } else {
        set_block_size(ptr1, diff);
    }
    m_free_total -= needed_size;
    return ptr1 + diff;
}

auto BlockAllocator::free(Index ptr, Size size) -> void
{
    CALICO_EXPECT_LE(ptr + size, m_page->size());
    CALICO_EXPECT_GE(ptr, NodeLayout::content_offset(m_page->id()));
    if (size < 4) {
        m_header->set_frag_count(m_header->frag_count() + size);
    } else {
        set_next_pointer(ptr, m_header->free_start());
        set_block_size(ptr, size);
        m_header->set_free_count(m_header->free_count() + 1);
        m_header->set_free_start(ptr);
    }
    m_free_total += size;
}

auto Node::header_crc() const -> Index
{
    return m_header.header_crc();
}

auto Node::parent_id() const -> PID
{
    return m_header.parent_id();
}

auto Node::right_sibling_id() const -> PID
{
    return m_header.right_sibling_id();
}

auto Node::rightmost_child_id() const -> PID
{
    return m_header.rightmost_child_id();
}

auto Node::cell_count() const -> Size
{
    return m_header.cell_count();
}

auto Node::update_header_crc() -> void
{
    m_header.update_header_crc();
}

auto Node::set_parent_id(PID parent_id) -> void
{
    m_header.set_parent_id(parent_id);
}

auto Node::set_right_sibling_id(PID right_sibling_id) -> void
{
    m_header.set_right_sibling_id(right_sibling_id);
}

auto Node::set_rightmost_child_id(PID rightmost_child_id) -> void
{
    m_header.set_rightmost_child_id(rightmost_child_id);
}

auto Node::usable_space() const -> Size
{
    return m_allocator.usable_space();
}

auto Node::is_external() const -> bool
{
    return m_page.type() == PageType::EXTERNAL_NODE;
}

auto Node::child_id(Index index) const -> PID
{
    CALICO_EXPECT_FALSE(is_external());
    CALICO_EXPECT_LE(index, cell_count());
    if (index < cell_count())
        return read_cell(index).left_child_id();
    return rightmost_child_id();
}

auto Node::read_key(Index index) const -> BytesView
{
    CALICO_EXPECT_LT(index, cell_count());
    return read_cell(index).key();
}

auto Node::read_cell(Index index) const -> Cell
{
    CALICO_EXPECT_LT(index, cell_count());
    return Cell::read_at(*this, m_directory.get_pointer(index).value);
}

auto Node::detach_cell(Index index, Scratch scratch) const -> Cell
{
    CALICO_EXPECT_LT(index, cell_count());
    auto cell = read_cell(index);
    cell.detach(std::move(scratch));
    return cell;
}

auto Node::extract_cell(Index index, Scratch scratch) -> Cell
{
    CALICO_EXPECT_LT(index, cell_count());
    auto cell = detach_cell(index, std::move(scratch));
    remove_at(index, cell.size());
    return cell;
}

auto Node::validate() const -> void
{
    // The usable space is the total of all the free blocks, fragments, and the gap space.
    [[maybe_unused]] const auto usable_space = m_allocator.usable_space();

    // The used space is the total of the header, cell pointers list, and the cells.
    [[maybe_unused]] auto used_space = cell_area_offset();
    for (Index i {}; i < cell_count(); ++i) {
        const auto lhs = read_cell(i);
        used_space += lhs.size();
        if (i < cell_count() - 1) {
            [[maybe_unused]] const auto rhs = read_cell(i + 1);
            CALICO_EXPECT_TRUE(lhs.key() < rhs.key());
        }
    }
    CALICO_EXPECT_EQ(used_space + usable_space, size());
}

auto Node::find_ge(BytesView key) const -> SearchResult
{
    long lower {};
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
    return m_header.cell_pointers_offset();
}

auto Node::cell_area_offset() const -> Size
{
    return m_header.cell_area_offset();
}

auto Node::header_offset() const -> Index
{
    return m_header.header_offset();
}

auto Node::max_usable_space() const -> Size
{
    return m_header.max_usable_space();
}

auto Node::is_overflowing() const -> bool
{
    return m_overflow != std::nullopt;
}

auto Node::is_underflowing() const -> bool
{
    if (id().is_root())
        return cell_count() == 0;
    return m_allocator.usable_space() > max_usable_space() / 2;
}

auto Node::overflow_cell() const -> const Cell&
{
    CALICO_EXPECT_TRUE(is_overflowing());
    return *m_overflow;
}

auto Node::set_overflow_cell(Cell cell) -> void
{
    m_overflow = std::move(cell);
}

auto Node::take_overflow_cell() -> Cell
{
    auto cell = std::move(*m_overflow);
    m_overflow.reset();
    return cell;
}

auto Node::set_child_id(Index index, PID child_id) -> void
{
    CALICO_EXPECT_FALSE(is_external());
    CALICO_EXPECT_LE(index, cell_count());
    if (index < cell_count()) {
        m_page.put_u32(m_directory.get_pointer(index).value, child_id.value);
    } else {
        set_rightmost_child_id(child_id);
    }
}

auto Node::allocate(Size needed_size, std::optional<Index> skipped_cid) -> Index
{
    if (const auto ptr = m_allocator.allocate(needed_size))
        return ptr;
    defragment(skipped_cid);
    return m_allocator.allocate(needed_size);
}

auto Node::defragment() -> void
{
    defragment(std::nullopt);
}

auto Node::defragment(std::optional<Index> skipped_cid) -> void
{
    const auto n = cell_count();
    const auto to_skip = skipped_cid ? *skipped_cid : n;
    auto end = m_page.size();
    std::string temp(end, '\x00');
    std::vector<Size> ptrs(n);

    for (Index index {}; index < n; ++index) {
        if (index == to_skip)
            continue;
        const auto cell = read_cell(index);
        end -= cell.size();
        cell.write({temp.data() + end, temp.size() - end});
        ptrs.at(index) = end;
    }
    for (Index index {}; index < n; ++index) {
        if (index != to_skip)
            m_directory.set_pointer(index, {ptrs.at(index)});
    }
    const auto offset = cell_area_offset();
    m_page.write(stob(temp).range(offset, m_page.size() - offset), offset);
    m_header.set_cell_start(end);
    m_allocator.reset();
}

auto Node::insert(Cell cell) -> void
{
    const auto [index, should_be_false] = find_ge(cell.key());
    // Keys should be unique.
    CALICO_EXPECT_FALSE(should_be_false);
    insert_at(index, std::move(cell));
}

auto Node::insert_at(Index index, Cell cell) -> void
{
    CALICO_EXPECT_FALSE(is_overflowing());
    CALICO_EXPECT_LE(index, cell_count());
    CALICO_EXPECT_EQ(is_external(), cell.left_child_id().is_null());

    const auto local_size = cell.size();

    // We don't have room to insert the cell pointer.
    if (cell_area_offset() + CELL_POINTER_SIZE > m_header.cell_start()) {
        if (m_allocator.usable_space() >= local_size + CELL_POINTER_SIZE) {
            defragment();
            return insert_at(index, std::move(cell));
        }
        set_overflow_cell(std::move(cell));
        return;
    }
    // insert a dummy cell pointer to save the slot.
    m_directory.insert_pointer(index, {m_page.size() - 1});

    // allocate space for the cell. This call may defragment the node.
    const auto offset = allocate(local_size, index);

    // We don't have room to insert the cell.
    if (!offset) {
        set_overflow_cell(std::move(cell));
        m_directory.remove_pointer(index);
        return;
    }
    // Now we can fill in the dummy cell pointer and write_all the cell.
    m_directory.set_pointer(index, {offset});
    cell.write(m_page.mut_range(offset, cell.size()));

    // Adjust the start of the cell content area.
    if (offset < m_header.cell_start())
        m_header.set_cell_start(offset);

    CALICO_VALIDATE(validate());
}

auto Node::remove(BytesView key) -> bool
{
    if (auto [index, found_eq] = find_ge(key); found_eq) {
        remove_at(index, read_cell(index).size());
        return true;
    }
    return false;
}

auto Node::remove_at(Index index, Size local_size) -> void
{
    CALICO_EXPECT_GE(local_size, MIN_CELL_HEADER_SIZE);
    CALICO_EXPECT_LE(local_size, get_max_local(m_page.size()) + MAX_CELL_HEADER_SIZE);
    CALICO_EXPECT_LT(index, cell_count());
    CALICO_EXPECT_FALSE(is_overflowing());
    m_allocator.free(m_directory.get_pointer(index).value, local_size);
    m_directory.remove_pointer(index);
    CALICO_VALIDATE(validate());
}

auto Node::reset(bool reset_header) -> void
{
    if (reset_header) {
        auto chunk = m_page.mut_range(header_offset(), NodeLayout::HEADER_SIZE);
        mem_clear(chunk, chunk.size());
        m_header.set_cell_start(m_page.size());
    }
    m_overflow.reset();
    m_allocator.recompute_usable_space();
}

auto transfer_cell(Node &src, Node &dst, Index index) -> void
{
    CALICO_EXPECT_EQ(src.type(), dst.type());
    auto cell = src.read_cell(index);
    const auto cell_size = cell.size();
    dst.insert(std::move(cell));
    src.remove_at(index, cell_size);
}

auto can_merge_siblings(const Node &Ln, const Node &rn, const Cell &separator) -> bool
{
    CALICO_EXPECT_FALSE(Ln.is_overflowing());
    CALICO_EXPECT_FALSE(rn.is_overflowing());
    CALICO_EXPECT_FALSE(Ln.id().is_root());
    CALICO_EXPECT_FALSE(rn.id().is_root());
    const auto page_size = Ln.size();
    CALICO_EXPECT_EQ(page_size, rn.size());
    CALICO_EXPECT_EQ(Ln.type(), rn.type());

    Size total {};

    // Size contributed by the separator cell and cell pointer. Note that the separator must be from an internal
    // node, so it has a left child ID. If Ln/rn are external, the left child ID will be discarded.
    total += separator.size() - Ln.is_external()*PAGE_ID_SIZE + CELL_POINTER_SIZE;

    // Occupied space in each node, including the headers.
    total += page_size - Ln.usable_space();
    total += page_size - rn.usable_space();

    // Disregard one of the sets of headers.
    total -= PageLayout::HEADER_SIZE + NodeLayout::HEADER_SIZE;
    return total <= page_size;
}

auto merge_left(Node &Lc, Node &rc, Node &parent, Index index) -> void
{
    if (Lc.is_external())
        Lc.set_right_sibling_id(rc.right_sibling_id());

    // Move the separator from the parent to the left child node.
    auto separator = parent.read_cell(index);
    const auto separator_size = separator.size();
    if (!Lc.is_external()) {
        separator.set_left_child_id(Lc.rightmost_child_id());
    } else {
        separator.set_left_child_id(PID::null());
    }
    Lc.insert(std::move(separator));
    parent.remove_at(index, separator_size);

    // Transfer the rest of the cells. Lc shouldn't overflow.
    while (rc.cell_count()) {
        transfer_cell(rc, Lc, 0);
        CALICO_EXPECT_FALSE(Lc.is_overflowing());
    }
    if (!Lc.is_external())
        Lc.set_rightmost_child_id(rc.rightmost_child_id());
    parent.set_child_id(index, Lc.id());
    if (parent.rightmost_child_id() == rc.id())
        parent.set_rightmost_child_id(Lc.id());
}

auto merge_right(Node &Lc, Node &rc, Node &parent, Index index) -> void
{
    if (Lc.is_external())
        Lc.set_right_sibling_id(rc.right_sibling_id());

    // Move the separator from the source to the left child node.
    auto separator = parent.read_cell(index);
    const auto separator_size = separator.size();
    if (!Lc.is_external()) {
        separator.set_left_child_id(Lc.rightmost_child_id());
        Lc.set_rightmost_child_id(rc.rightmost_child_id());
    } else {
        separator.set_left_child_id(PID::null());
    }
    Lc.insert(std::move(separator));
    CALICO_EXPECT_EQ(parent.child_id(index + 1), rc.id());
    parent.set_child_id(index + 1, Lc.id());
    parent.remove_at(index, separator_size);

    // Transfer the rest of the cells. Lc shouldn't overflow.
    while (rc.cell_count()) {
        transfer_cell(rc, Lc, 0);
        CALICO_EXPECT_FALSE(Lc.is_overflowing());
    }
}

auto split_root(Node &root, Node &child) -> void
{
    // Copy the cells.
    auto offset = root.cell_area_offset();
    auto size = root.size() - offset;
    child.page().write(root.page().range(offset).truncate(size), offset);

    // Copy the header and cell pointers.
    offset = child.header_offset();
    size = NodeLayout::HEADER_SIZE + root.cell_count()*CELL_POINTER_SIZE;
    child.page().write(root.page().range(root.header_offset()).truncate(size), offset);

    CALICO_EXPECT_TRUE(root.is_overflowing());
    child.set_overflow_cell(root.take_overflow_cell());

    root.reset(true);
    root.page().set_type(PageType::INTERNAL_NODE);
    root.set_rightmost_child_id(child.id());
    child.set_parent_id(PID::root());
}

auto merge_root(Node &root, Node &child) -> void
{
    CALICO_EXPECT(root.rightmost_child_id() == child.id());
    child.defragment();

    // Copy the cell content area.
    auto offset = child.cell_area_offset();
    auto size = child.size() - offset;
    root.page().write(child.page().range(offset).truncate(size), offset);

    // Copy the header and cell pointers.
    offset = root.header_offset();
    size = NodeLayout::HEADER_SIZE + child.cell_count()*CELL_POINTER_SIZE;
    root.page().write(child.page().range(child.header_offset()).truncate(size), offset);
    root.page().set_type(child.type());
}

auto split_non_root(Node &Ln, Node &rn, Scratch scratch) -> Cell
{
    // get the overflow cell. The caller should make sure the node is overflowing
    // before calling this method.
    CALICO_EXPECT_TRUE(Ln.is_overflowing());
    auto overflow = Ln.take_overflow_cell();

    // Include the overflow cell in our count.
    const auto n = Ln.cell_count() + 1;

    // Figure out where the overflow cell should go.
    const auto [overflow_idx, should_be_false] = Ln.find_ge(overflow.key());
    CALICO_EXPECT_FALSE(should_be_false);
    auto where = ThreeWayComparison::EQ;
    auto median_index = n / 2;

    const auto select_median = [&](Index index) {
        if (index == median_index) {
            return std::move(overflow);
        } else {
            const auto is_right_of_median = index > median_index;
            median_index -= !is_right_of_median;
            // Since `is_left_of_median` is either 0 or 1, this will produce either -1 or 1, i.e. LT or
            // GT to indicate the position of the overflow cell relative to the median.
            where = static_cast<ThreeWayComparison>(2*is_right_of_median - 1);
            return Ln.extract_cell(median_index, std::move(scratch));
        }
    };
    auto median = select_median(overflow_idx);

    if (Ln.is_external()) {
        rn.set_right_sibling_id(Ln.right_sibling_id());
        Ln.set_right_sibling_id(rn.id());
    } else {
        rn.set_rightmost_child_id(Ln.rightmost_child_id());
        Ln.set_rightmost_child_id(median.left_child_id());
    }
    rn.set_parent_id(Ln.parent_id());
    median.set_left_child_id(Ln.id());

    // Transfer cells after the median cell to the right sibling node.
    const auto cell_count = Ln.cell_count();
    for (auto index = median_index; index < cell_count; ++index) {
        auto cell = Ln.read_cell(median_index);
        const auto cell_size = cell.size();
        rn.insert_at(rn.cell_count(), std::move(cell));
        Ln.remove_at(median_index, cell_size);
    }
    const auto do_transfer = [](Node &target, Cell cell) {
        const auto [index, found_eq]{target.find_ge(cell.key())};
        CALICO_EXPECT_FALSE(found_eq);
        target.insert_at(index, std::move(cell));
    };
    switch (where) {
        case ThreeWayComparison::LT:
            do_transfer(Ln, std::move(overflow));
            break;
        case ThreeWayComparison::GT:
            do_transfer(rn, std::move(overflow));
            break;
        case ThreeWayComparison::EQ:
            // No transfer. The median/overflow cell is returned.
            break;
    }
    return median;
}

} // calico