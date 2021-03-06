#include "node.h"
#include <spdlog/fmt/fmt.h>
#include "file_header.h"
#include "utils/crc.h"
#include "utils/layout.h"

namespace cco {


auto NodeHeader::parent_id() const -> PID
{
    return PID {get_u32(*m_page, header_offset() + NodeLayout::PARENT_ID_OFFSET)};
}

auto NodeHeader::right_sibling_id() const -> PID
{
    CCO_EXPECT_EQ(m_page->type(), PageType::EXTERNAL_NODE);
    return PID {get_u32(*m_page, header_offset() + NodeLayout::RIGHT_SIBLING_ID_OFFSET)};
}

auto NodeHeader::reserved() const -> uint32_t
{
    CCO_EXPECT_EQ(m_page->type(), PageType::INTERNAL_NODE);
    return get_u32(*m_page, header_offset() + NodeLayout::RESERVED_OFFSET);
}

auto NodeHeader::left_sibling_id() const -> PID
{
    CCO_EXPECT_EQ(m_page->type(), PageType::EXTERNAL_NODE);
    return PID {get_u32(*m_page, header_offset() + NodeLayout::LEFT_SIBLING_ID_OFFSET)};
}

auto NodeHeader::rightmost_child_id() const -> PID
{
    CCO_EXPECT_NE(m_page->type(), PageType::EXTERNAL_NODE);
    return PID {get_u32(*m_page, header_offset() + NodeLayout::RIGHTMOST_CHILD_ID_OFFSET)};
}

auto NodeHeader::cell_count() const -> Size
{
    return get_u16(*m_page, header_offset() + NodeLayout::CELL_COUNT_OFFSET);
}

auto NodeHeader::free_count() const -> Size
{
    return get_u16(*m_page, header_offset() + NodeLayout::FREE_COUNT_OFFSET);
}

auto NodeHeader::cell_start() const -> Index
{
    return get_u16(*m_page, header_offset() + NodeLayout::CELL_START_OFFSET);
}

auto NodeHeader::free_start() const -> Index
{
    return get_u16(*m_page, header_offset() + NodeLayout::FREE_START_OFFSET);
}

auto NodeHeader::frag_count() const -> Size
{
    return get_u16(*m_page, header_offset() + NodeLayout::FRAG_TOTAL_OFFSET);
}

auto NodeHeader::free_total() const -> Size
{
    return get_u16(*m_page, header_offset() + NodeLayout::FREE_TOTAL_OFFSET);
}

auto NodeHeader::set_parent_id(PID parent_id) -> void
{
    CCO_EXPECT_NE(m_page->id(), PID::root());
    const auto offset = header_offset() + NodeLayout::PARENT_ID_OFFSET;
    put_u32(*m_page, offset, parent_id.value);
}

auto NodeHeader::set_right_sibling_id(PID right_sibling_id) -> void
{
    CCO_EXPECT_EQ(m_page->type(), PageType::EXTERNAL_NODE);
    const auto offset = header_offset() + NodeLayout::RIGHT_SIBLING_ID_OFFSET;
    put_u32(*m_page, offset, right_sibling_id.value);
}

auto NodeHeader::set_left_sibling_id(PID left_sibling_id) -> void
{
    CCO_EXPECT_EQ(m_page->type(), PageType::EXTERNAL_NODE);
    const auto offset = header_offset() + NodeLayout::LEFT_SIBLING_ID_OFFSET;
    put_u32(*m_page, offset, left_sibling_id.value);
}

auto NodeHeader::set_rightmost_child_id(PID rightmost_child_id) -> void
{
    CCO_EXPECT_NE(m_page->type(), PageType::EXTERNAL_NODE);
    const auto offset = header_offset() + NodeLayout::RIGHTMOST_CHILD_ID_OFFSET;
    put_u32(*m_page, offset, rightmost_child_id.value);
}

auto NodeHeader::set_cell_count(Size cell_count) -> void
{
    CCO_EXPECT_BOUNDED_BY(uint16_t, cell_count);
    put_u16(*m_page, header_offset() + NodeLayout::CELL_COUNT_OFFSET, static_cast<uint16_t>(cell_count));
}

auto NodeHeader::set_free_count(Size free_count) -> void
{
    CCO_EXPECT_BOUNDED_BY(uint16_t, free_count);
    put_u16(*m_page, header_offset() + NodeLayout::FREE_COUNT_OFFSET, static_cast<uint16_t>(free_count));
}

auto NodeHeader::set_cell_start(Index cell_start) -> void
{
    CCO_EXPECT_BOUNDED_BY(uint16_t, cell_start);
    put_u16(*m_page, header_offset() + NodeLayout::CELL_START_OFFSET, static_cast<uint16_t>(cell_start));
}

auto NodeHeader::set_free_start(Index free_start) -> void
{
    CCO_EXPECT_BOUNDED_BY(uint16_t, free_start);
    put_u16(*m_page, header_offset() + NodeLayout::FREE_START_OFFSET, static_cast<uint16_t>(free_start));
}

auto NodeHeader::set_frag_count(Size frag_count) -> void
{
    CCO_EXPECT_BOUNDED_BY(uint16_t, frag_count);
    put_u16(*m_page, header_offset() + NodeLayout::FRAG_TOTAL_OFFSET, static_cast<uint16_t>(frag_count));
}

auto NodeHeader::set_free_total(Size free_total) -> void
{
    CCO_EXPECT_BOUNDED_BY(uint16_t, free_total);
    put_u16(*m_page, header_offset() + NodeLayout::FREE_TOTAL_OFFSET, static_cast<uint16_t>(free_total));
}

auto NodeHeader::cell_directory_offset() const -> Size
{
    return NodeLayout::content_offset(m_page->id());
}

auto NodeHeader::cell_area_offset() const -> Size
{
    const auto cell_count = get_u16(*m_page, header_offset() + NodeLayout::CELL_COUNT_OFFSET);
    return cell_directory_offset() + CELL_POINTER_SIZE * cell_count;
}

auto NodeHeader::header_offset() const -> Index
{
    return NodeLayout::header_offset(m_page->id());
}

auto NodeHeader::gap_size() const -> Size
{
    const auto top = cell_start();
    const auto bottom = cell_area_offset();
    CCO_EXPECT_GE(top, bottom);
    return top - bottom;
}

auto NodeHeader::max_usable_space() const -> Size
{
    return m_page->size() - cell_directory_offset();
}

auto CellDirectory::get_pointer(Index index) const -> Pointer
{
    CCO_EXPECT_LT(index, m_header->cell_count());
    return {get_u16(*m_page, m_header->cell_directory_offset() + index * CELL_POINTER_SIZE)};
}

auto CellDirectory::set_pointer(Index index, Pointer pointer) -> void
{
    CCO_EXPECT_LT(index, m_header->cell_count());
    CCO_EXPECT_LE(pointer.value, m_page->size());
    put_u16(*m_page, m_header->cell_directory_offset() + index * CELL_POINTER_SIZE, static_cast<uint16_t>(pointer.value));
}

auto CellDirectory::insert_pointer(Index index, Pointer pointer) -> void
{
    CCO_EXPECT_GE(pointer.value, m_header->cell_area_offset());
    CCO_EXPECT_LT(pointer.value, m_page->size());
    CCO_EXPECT_LE(index, m_header->cell_count());
    const auto start = NodeLayout::content_offset(m_page->id());
    const auto offset = start + CELL_POINTER_SIZE * index;
    const auto size = (m_header->cell_count() - index) * CELL_POINTER_SIZE;
    auto chunk = m_page->bytes(offset, size + CELL_POINTER_SIZE);
    mem_move(chunk.range(CELL_POINTER_SIZE), chunk, size);
    m_header->set_cell_count(m_header->cell_count() + 1);
    set_pointer(index, pointer);
}

auto CellDirectory::remove_pointer(Index index) -> void
{
    CCO_EXPECT_GT(m_header->cell_count(), 0);
    CCO_EXPECT_LT(index, m_header->cell_count());
    const auto start = NodeLayout::header_offset(m_page->id()) + NodeLayout::HEADER_SIZE;
    const auto offset = start + CELL_POINTER_SIZE * index;
    const auto size = (m_header->cell_count() - index - 1) * CELL_POINTER_SIZE;
    auto chunk = m_page->bytes(offset, size + CELL_POINTER_SIZE);
    mem_move(chunk, chunk.range(CELL_POINTER_SIZE), size);
    m_header->set_cell_count(m_header->cell_count() - 1);
}

BlockAllocator::BlockAllocator(NodeHeader &header)
    : m_page {&header.page()},
      m_header {&header}
{}

auto BlockAllocator::usable_space() const -> Size
{
    return m_header->free_total() + m_header->gap_size();
}

auto BlockAllocator::reset() -> void
{
    CCO_EXPECT_TRUE(m_page->is_writable());
    m_header->set_frag_count(0);
    m_header->set_free_count(0);
    m_header->set_free_total(0);
}

auto BlockAllocator::compute_free_total() const -> Size
{
    auto free_total = m_header->frag_count();
    for (Index i {}, ptr {m_header->free_start()}; i < m_header->free_count(); ++i) {
        free_total += get_block_size(ptr);
        ptr = get_next_pointer(ptr);
    }
    CCO_EXPECT_LE(free_total, m_page->size() - m_header->cell_directory_offset());
    return free_total;
}

auto BlockAllocator::get_next_pointer(Index offset) const -> Index
{
    return get_u16(*m_page, offset);
}

auto BlockAllocator::get_block_size(Index offset) const -> Size
{
    return get_u16(*m_page, offset + CELL_POINTER_SIZE);
}

auto BlockAllocator::set_next_pointer(Index offset, Index next_pointer) -> void
{
    CCO_EXPECT_LT(next_pointer, m_page->size());
    return put_u16(*m_page, offset, static_cast<uint16_t>(next_pointer));
}

auto BlockAllocator::set_block_size(Index offset, Size block_size) -> void
{
    CCO_EXPECT_GE(block_size, CELL_POINTER_SIZE + sizeof(uint16_t));
    CCO_EXPECT_LT(block_size, m_header->max_usable_space());
    return put_u16(*m_page, offset + CELL_POINTER_SIZE, static_cast<uint16_t>(block_size));
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
    CCO_EXPECT_TRUE(m_page->is_writable());
    CCO_EXPECT_LT(needed_size, m_page->size() - NodeLayout::content_offset(m_page->id()));

    if (needed_size > usable_space())
        return 0;
    if (auto ptr = allocate_from_free(needed_size))
        return ptr;
    return allocate_from_gap(needed_size);
}

/* Free block layout:
 *     .---------------------.-------------.---------------------------.
 *     |  Next Pointer (2B)  |  Size (2B)  |   Free FakeFile (Size-4 B)  |
 *     '---------------------'-------------'---------------------------'
 */
auto BlockAllocator::take_free_space(Index ptr0, Index ptr1, Size needed_size) -> Index
{
    CCO_EXPECT_LT(ptr0, m_page->size());
    CCO_EXPECT_LT(ptr1, m_page->size());
    CCO_EXPECT_LT(needed_size, m_page->size());
    const auto is_first = !ptr0;
    const auto ptr2 = get_next_pointer(ptr1);
    const auto free_size = get_block_size(ptr1);
    CCO_EXPECT_GE(free_size, needed_size);
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
    const auto free_total = m_header->free_total();
    CCO_EXPECT_GE(free_total, needed_size);
    m_header->set_free_total(free_total - needed_size);
    return ptr1 + diff;
}

auto BlockAllocator::free(Index ptr, Size size) -> void
{
    CCO_EXPECT_LE(ptr + size, m_page->size());
    CCO_EXPECT_GE(ptr, NodeLayout::content_offset(m_page->id()));
    if (size < FREE_BLOCK_HEADER_SIZE) {
        m_header->set_frag_count(m_header->frag_count() + size);
    } else {
        set_next_pointer(ptr, m_header->free_start());
        set_block_size(ptr, size);
        m_header->set_free_count(m_header->free_count() + 1);
        m_header->set_free_start(ptr);
    }
    m_header->set_free_total(m_header->free_total() + size);
}

auto Node::parent_id() const -> PID
{
    return m_header.parent_id();
}

auto Node::right_sibling_id() const -> PID
{
    return m_header.right_sibling_id();
}

auto Node::left_sibling_id() const -> PID
{
    return m_header.left_sibling_id();
}

auto Node::rightmost_child_id() const -> PID
{
    return m_header.rightmost_child_id();
}

auto Node::cell_count() const -> Size
{
    return m_header.cell_count();
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
    CCO_EXPECT_FALSE(is_external());
    CCO_EXPECT_LE(index, cell_count());
    if (index < cell_count())
        return read_cell(index).left_child_id();
    return rightmost_child_id();
}

auto Node::read_key(Index index) const -> BytesView
{
    CCO_EXPECT_LT(index, cell_count());
    return read_cell(index).key();
}

auto Node::read_cell(Index index) const -> Cell
{
    CCO_EXPECT_LT(index, cell_count());
    return Cell::read_at(*this, m_directory.get_pointer(index).value);
}

auto Node::detach_cell(Index index, Scratch scratch) const -> Cell
{
    CCO_EXPECT_LT(index, cell_count());
    auto cell = read_cell(index);
    cell.detach(scratch);
    return cell;
}

auto Node::extract_cell(Index index, Scratch scratch) -> Cell
{
    CCO_EXPECT_LT(index, cell_count());
    auto cell = detach_cell(index, scratch);
    remove_at(index, cell.size());
    return cell;
}

auto Node::TEST_validate() const -> void
{
    const auto label = fmt::format("node {}: ", m_page.id().value);

    // The usable space is the total of all the free blocks, fragments, and the gap space.
    const auto usable_space = m_allocator.usable_space();

    // The used space is the total of the header, cell pointers list, and the cells.
    auto used_space = cell_area_offset();
    for (Index i {}; i < cell_count(); ++i) {
        const auto lhs = read_cell(i);
        used_space += lhs.size();
        if (i < cell_count() - 1) {
            const auto rhs = read_cell(i + 1);
            if (lhs.key() >= rhs.key()) {
                fmt::print("(1/2) {}: keys are out of order\n", label);
                fmt::print("(2/2) {}: {} should be less than {}\n", label, btos(lhs.key()), btos(rhs.key()));
                std::exit(EXIT_FAILURE);
            }
        }
    }
    if (used_space + usable_space != size()) {
        fmt::print("(1/2) {}: memory is unaccounted for\n", label);
        fmt::print("(2/2) {}: {} bytes were lost\n", label, int(size()) - int(used_space + usable_space));
        std::exit(EXIT_FAILURE);
    }
}

auto Node::find_ge(BytesView key) const -> FindGeResult
{
    long lower {};
    auto upper = static_cast<long>(cell_count());

    while (lower < upper) {
        // Note that this cannot overflow since the page size is bounded by a 16-bit integer.
        const auto middle = (lower + upper) / 2;
        switch (compare_three_way(key, read_key(static_cast<Index>(middle)))) {
            case ThreeWayComparison::EQ:
                return {static_cast<Index>(middle), true};
            case ThreeWayComparison::LT:
                upper = middle;
                break;
            case ThreeWayComparison::GT:
                lower = middle + 1;
        }
    }
    return {static_cast<Index>(lower), false};
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

auto Node::overflow_cell() const -> const Cell &
{
    CCO_EXPECT_TRUE(is_overflowing());
    return *m_overflow;
}

auto Node::set_overflow_cell(Cell cell) -> void
{
    m_overflow = cell;
}

auto Node::take_overflow_cell() -> Cell
{
    auto cell = *m_overflow;
    m_overflow.reset();
    return cell;
}

auto Node::set_child_id(Index index, PID child_id) -> void
{
    CCO_EXPECT_FALSE(is_external());
    CCO_EXPECT_LE(index, cell_count());
    if (index < cell_count()) {
        put_u32(m_page, m_directory.get_pointer(index).value, child_id.value);
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
    CCO_EXPECT_FALSE(should_be_false);
    insert_at(index, cell);
}

auto Node::insert_at(Index index, Cell cell) -> void
{
    CCO_EXPECT_FALSE(is_overflowing());
    CCO_EXPECT_LE(index, cell_count());
    CCO_EXPECT_EQ(is_external(), cell.is_external());

    const auto local_size = cell.size();

    // We don't have room to insert the cell pointer.
    if (cell_area_offset() + CELL_POINTER_SIZE > m_header.cell_start()) {
        if (m_allocator.usable_space() >= local_size + CELL_POINTER_SIZE) {
            defragment();
            return insert_at(index, cell);
        }
        set_overflow_cell(cell);
        return;
    }
    // insert a dummy cell pointer to save the slot.
    m_directory.insert_pointer(index, {m_page.size() - 1});

    // allocate space for the cell. This call may defragment the node.
    const auto offset = allocate(local_size, index);

    // We don't have room to insert the cell.
    if (!offset) {
        CCO_EXPECT_LT(m_allocator.usable_space(), local_size + CELL_POINTER_SIZE);
        set_overflow_cell(cell);
        m_directory.remove_pointer(index);
        return;
    }
    // Now we can fill in the dummy cell pointer and write_all the cell.
    m_directory.set_pointer(index, {offset});
    cell.write(m_page.bytes(offset, cell.size()));

    // Adjust the start of the cell content area.
    if (offset < m_header.cell_start())
        m_header.set_cell_start(offset);

    //    CCO_VALIDATE(validate());
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
    CCO_EXPECT_GE(local_size, MIN_CELL_HEADER_SIZE);
    CCO_EXPECT_LE(local_size, get_max_local(m_page.size()) + MAX_CELL_HEADER_SIZE);
    CCO_EXPECT_LT(index, cell_count());
    CCO_EXPECT_FALSE(is_overflowing());
    m_allocator.free(m_directory.get_pointer(index).value, local_size);
    m_directory.remove_pointer(index);
    //    CCO_VALIDATE(validate());
}

auto Node::reset(bool reset_header) -> void
{
    if (reset_header) {
        CCO_EXPECT_TRUE(m_page.is_writable());
        auto chunk = m_page.bytes(header_offset(), NodeLayout::HEADER_SIZE);
        mem_clear(chunk, chunk.size());
        m_header.set_cell_start(m_page.size());
    }
    m_overflow.reset();
}

auto transfer_cell(Node &src, Node &dst, Index index) -> void
{
    CCO_EXPECT_EQ(src.type(), dst.type());
    auto cell = src.read_cell(index);
    const auto cell_size = cell.size();
    dst.insert(cell);
    src.remove_at(index, cell_size);
}

auto accumulate_occupied_space(const Node &Ln, const Node &rn)
{
    const auto page_size = Ln.size();
    CCO_EXPECT_EQ(page_size, rn.size());
    CCO_EXPECT_EQ(Ln.type(), rn.type());
    CCO_EXPECT_FALSE(Ln.is_overflowing());
    CCO_EXPECT_FALSE(rn.is_overflowing());
    CCO_EXPECT_FALSE(Ln.id().is_root());
    CCO_EXPECT_FALSE(rn.id().is_root());
    Size total {};

    // Occupied space in each node, including the headers.
    total += page_size - Ln.usable_space();
    total += page_size - rn.usable_space();

    // Disregard one of the sets of headers.
    return total - PageLayout::HEADER_SIZE + NodeLayout::HEADER_SIZE;
}

auto can_merge_internal_siblings(const Node &Ln, const Node &rn, const Cell &separator) -> bool
{
    const auto total = accumulate_occupied_space(Ln, rn) +
                       separator.size() + CELL_POINTER_SIZE;
    return total <= Ln.size();
}

auto can_merge_external_siblings(const Node &Ln, const Node &rn) -> bool
{
    return accumulate_occupied_space(Ln, rn) <= Ln.size();
}

auto can_merge_siblings(const Node &Ln, const Node &rn, const Cell &separator) -> bool
{
    if (Ln.is_external())
        return can_merge_external_siblings(Ln, rn);
    return can_merge_internal_siblings(Ln, rn, separator);
}

auto internal_merge_left(Node &Lc, Node &rc, Node &parent, Index index) -> void
{
    // Move the separator from the parent to the left child node.
    auto separator = parent.read_cell(index);
    const auto separator_size = separator.size();
    separator.set_left_child_id(Lc.rightmost_child_id());
    Lc.insert(separator);
    parent.remove_at(index, separator_size);

    // Transfer the rest of the cells. Lc shouldn't overflow.
    while (rc.cell_count())
        transfer_cell(rc, Lc, 0);
    CCO_EXPECT_FALSE(Lc.is_overflowing());

    Lc.set_rightmost_child_id(rc.rightmost_child_id());
    parent.set_child_id(index, Lc.id());
    if (parent.rightmost_child_id() == rc.id()) // TODO: Necessary???
        parent.set_rightmost_child_id(Lc.id());
}

auto external_merge_left(Node &Lc, Node &rc, Node &parent, Index index) -> void
{
    Lc.set_right_sibling_id(rc.right_sibling_id());

    // Move the separator from the parent to the left child node.
    auto separator = parent.read_cell(index);
    parent.remove_at(index, separator.size());

    while (rc.cell_count())
        transfer_cell(rc, Lc, 0);
    CCO_EXPECT_FALSE(Lc.is_overflowing());
    parent.set_child_id(index, Lc.id());
    if (parent.rightmost_child_id() == rc.id()) // TODO: Necessary???
        parent.set_rightmost_child_id(Lc.id());
}

auto merge_left(Node &Lc, Node &rc, Node &parent, Index index) -> void
{
    if (Lc.is_external()) {
        external_merge_left(Lc, rc, parent, index);
    } else {
        internal_merge_left(Lc, rc, parent, index);
    }
}

auto internal_merge_right(Node &Lc, Node &rc, Node &parent, Index index) -> void
{
    // Move the separator from the source to the left child node.
    auto separator = parent.read_cell(index);
    const auto separator_size = separator.size();
    separator.set_left_child_id(Lc.rightmost_child_id());
    Lc.set_rightmost_child_id(rc.rightmost_child_id());
    Lc.insert(separator);
    CCO_EXPECT_EQ(parent.child_id(index + 1), rc.id());
    parent.set_child_id(index + 1, Lc.id());
    parent.remove_at(index, separator_size);

    // Transfer the rest of the cells. Lc shouldn't overflow.
    while (rc.cell_count()) {
        transfer_cell(rc, Lc, 0);
        CCO_EXPECT_FALSE(Lc.is_overflowing());
    }
}

auto external_merge_right(Node &Lc, Node &rc, Node &parent, Index index) -> void
{
    Lc.set_right_sibling_id(rc.right_sibling_id());

    auto separator = parent.read_cell(index);
    CCO_EXPECT_EQ(parent.child_id(index + 1), rc.id());
    parent.set_child_id(index + 1, Lc.id());
    parent.remove_at(index, separator.size());

    while (rc.cell_count())
        transfer_cell(rc, Lc, 0);
    CCO_EXPECT_FALSE(Lc.is_overflowing());
}

auto merge_right(Node &Lc, Node &rc, Node &parent, Index index) -> void
{
    if (Lc.is_external()) {
        external_merge_right(Lc, rc, parent, index);
    } else {
        internal_merge_right(Lc, rc, parent, index);
    }
}

auto split_root(Node &root, Node &child) -> void
{
    // Copy the cells.
    auto offset = root.cell_area_offset();
    auto size = root.size() - offset;
    child.page().write(root.page().view(offset).truncate(size), offset);

    // Copy the header and cell pointers.
    offset = child.header_offset();
    size = NodeLayout::HEADER_SIZE + root.cell_count() * CELL_POINTER_SIZE;
    child.page().write(root.page().view(root.header_offset()).truncate(size), offset);

    CCO_EXPECT_TRUE(root.is_overflowing());
    child.set_overflow_cell(root.take_overflow_cell());

    root.reset(true);
    root.page().set_type(PageType::INTERNAL_NODE);
    root.set_rightmost_child_id(child.id());
    child.set_parent_id(PID::root());
}

auto merge_root(Node &root, Node &child) -> void
{
    CCO_EXPECT(root.rightmost_child_id() == child.id());
    child.defragment();

    // Copy the cell content area.
    auto offset = child.cell_area_offset();
    auto size = child.size() - offset;
    root.page().write(child.page().view(offset).truncate(size), offset);

    // Copy the header and cell pointers.
    offset = root.header_offset();
    size = NodeLayout::HEADER_SIZE + child.cell_count() * CELL_POINTER_SIZE;
    root.page().write(child.page().view(child.header_offset()).truncate(size), offset);
    root.page().set_type(child.type());
    root.page().set_lsn(child.page().lsn());
}

template<class Predicate>
auto transfer_cells_right_while(Node &src, Node &dst, Predicate &&predicate) -> void
{
    Index counter {};
    while (predicate(src, dst, counter++)) {
        const auto last = src.cell_count() - 1;
        auto cell = src.read_cell(last);
        const auto cell_size = cell.size();
        dst.insert_at(0, cell);
        CCO_EXPECT_FALSE(dst.is_overflowing());
        src.remove_at(last, cell_size);
    }
}

auto split_non_root_fast_internal(Node &Ln, Node &rn, Cell overflow, Index overflow_index, Scratch scratch) -> Cell
{
    transfer_cells_right_while(Ln, rn, [overflow_index](const Node &src, const Node &, Index) {
        return src.cell_count() > overflow_index;
    });

    if (overflow.is_attached())
        overflow.detach(scratch, true);
    overflow.set_is_external(false);
    overflow.set_left_child_id(Ln.id());
    return overflow;
}

auto split_non_root_fast_external(Node &Ln, Node &rn, Cell overflow, Index overflow_index, Scratch scratch) -> Cell
{
    // Note that we need to insert the overflow cell into either Ln or rn no matter what, even if it ends up being the separator.
    transfer_cells_right_while(Ln, rn, [&overflow, overflow_index](const Node &src, const Node &, Index counter) {
        const auto goes_in_src = src.cell_count() > overflow_index;
        const auto has_no_room = src.usable_space() < overflow.size() + CELL_POINTER_SIZE;
        return !counter || (goes_in_src && has_no_room);
    });

    if (Ln.cell_count() > overflow_index) {
        Ln.insert_at(overflow_index, overflow);
        CCO_EXPECT_FALSE(Ln.is_overflowing());
    } else {
        rn.insert_at(0, overflow);
        CCO_EXPECT_FALSE(rn.is_overflowing());
    }
    auto separator = rn.read_cell(0);
    separator.detach(scratch, true);
    separator.set_left_child_id(Ln.id());
    return separator;
}

auto split_external_non_root(Node &Ln, Node &rn, Scratch scratch) -> Cell
{
    auto overflow = Ln.take_overflow_cell();

    // Figure out where the overflow cell should go.
    const auto [overflow_idx, should_be_false] = Ln.find_ge(overflow.key());
    CCO_EXPECT_FALSE(should_be_false);

    // Warning: We don't have access to the former right sibling of Ln, but we need to set its left child ID.
    //          We need to make sure to do that in the caller.
    rn.set_right_sibling_id(Ln.right_sibling_id());
    Ln.set_right_sibling_id(rn.id());
    rn.set_left_sibling_id(Ln.id());
    rn.set_parent_id(Ln.parent_id());

    if (overflow_idx > 0 && overflow_idx < Ln.cell_count()) {
        return split_non_root_fast_external(Ln, rn, overflow, overflow_idx, scratch);

    } else if (overflow_idx == 0) {
        // We need the `!counter` because the condition following it may not be true if we got here from split_root().
        transfer_cells_right_while(Ln, rn, [&overflow](const Node &src, const Node &, Index counter) {
            return !counter || src.usable_space() < overflow.size() + CELL_POINTER_SIZE;
        });
        Ln.insert_at(0, overflow);
        CCO_EXPECT_FALSE(Ln.is_overflowing());

    } else if (overflow_idx == Ln.cell_count()) {
        // Just transfer a single cell in this case. This should reduce the number of splits during a sequential write, which seems to be
        // a common use case.
        transfer_cells_right_while(Ln, rn, [](const Node &, const Node &, Index counter) {
            return !counter;
        });
        rn.insert_at(rn.cell_count(), overflow);
        CCO_EXPECT_FALSE(rn.is_overflowing());
    }

    auto separator = rn.detach_cell(0, scratch);
    separator.set_is_external(false);
    separator.set_left_child_id(Ln.id());
    return separator;
}

auto split_internal_non_root(Node &Ln, Node &rn, Scratch scratch) -> Cell
{
    auto overflow = Ln.take_overflow_cell();

    // Figure out where the overflow cell should go.
    const auto [overflow_idx, falsy] = Ln.find_ge(overflow.key());
    CCO_EXPECT_FALSE(falsy);

    rn.set_rightmost_child_id(Ln.rightmost_child_id());
    rn.set_parent_id(Ln.parent_id());

    if (overflow_idx > 0 && overflow_idx < Ln.cell_count()) {
        Ln.set_rightmost_child_id(overflow.left_child_id());
        return split_non_root_fast_internal(Ln, rn, overflow, overflow_idx, scratch);

    } else if (overflow_idx == 0) {
        // TODO: Split the other way in this case, as we are possibly inserting reverse sequentially?
        transfer_cells_right_while(Ln, rn, [&overflow](const Node &src, const Node &, Index counter) {
            return !counter || src.usable_space() < overflow.size() + CELL_POINTER_SIZE;
        });
        Ln.insert_at(0, overflow);
        CCO_EXPECT_FALSE(Ln.is_overflowing());

    } else if (overflow_idx == Ln.cell_count()) {
        // Just transfer a single cell in this case. This should reduce the number of splits during a sequential write, which seems to be
        // a common use case. If we want to change this behavior, we just need to make sure that rn still has room for the overflow cell.
        transfer_cells_right_while(Ln, rn, [](const Node &, const Node &, Index counter) {
            return !counter;
        });
        rn.insert_at(rn.cell_count(), overflow);
        CCO_EXPECT_FALSE(rn.is_overflowing());
    }

    auto separator = Ln.extract_cell(Ln.cell_count() - 1, scratch);
    Ln.set_rightmost_child_id(separator.left_child_id());
    separator.set_left_child_id(Ln.id());
    return separator;
}

auto split_non_root(Node &Ln, Node &rn, Scratch scratch) -> Cell
{
    CCO_EXPECT_TRUE(Ln.is_overflowing());
    CCO_EXPECT_EQ(Ln.is_external(), rn.is_external());
    if (Ln.is_external())
        return split_external_non_root(Ln, rn, scratch);
    return split_internal_non_root(Ln, rn, scratch);
}

auto get_file_header_reader(const Node &node) -> FileHeaderReader
{
    return get_file_header_reader(node.page());
}

auto get_file_header_writer(Node &node) -> FileHeaderWriter
{
    return get_file_header_writer(node.page());
}

} // namespace cco