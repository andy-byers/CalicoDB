#include "node.h"
#include "utils/crc.h"
#include "utils/encoding.h"
#include "utils/layout.h"

namespace cub {

auto Node::header_crc() const -> Index
{
    return m_page.get_u32(header_offset() + NodeLayout::HEADER_CRC_OFFSET);
}

auto Node::parent_id() const -> PID
{
    return PID{get_uint32(m_page.range(header_offset() + NodeLayout::PARENT_ID_OFFSET))};
}

auto Node::right_sibling_id() const -> PID
{
    CUB_EXPECT_TRUE(is_external());
    return PID {get_uint32(m_page.range(header_offset() + NodeLayout::RIGHT_SIBLING_ID_OFFSET))};
}

auto Node::rightmost_child_id() const -> PID
{
    CUB_EXPECT_FALSE(is_external());
    return PID {get_uint32(m_page.range(header_offset() + NodeLayout::RIGHTMOST_CHILD_ID_OFFSET))};
}

auto Node::cell_count() const -> Size
{
    return m_page.get_u16(header_offset() + NodeLayout::CELL_COUNT_OFFSET);
}

auto Node::free_count() const -> Size
{
    return m_page.get_u16(header_offset() + NodeLayout::FREE_COUNT_OFFSET);
}

auto Node::cell_start() const -> Index
{
    return m_page.get_u16(header_offset() + NodeLayout::CELL_START_OFFSET);
}

auto Node::free_start() const -> Index
{
    return m_page.get_u16(header_offset() + NodeLayout::FREE_START_OFFSET);
}

auto Node::frag_count() const -> Size
{
    return m_page.get_u16(header_offset() + NodeLayout::FRAG_COUNT_OFFSET);
}

auto Node::update_header_crc() -> void
{
    const auto offset = header_offset() + NodeLayout::HEADER_CRC_OFFSET;
    // This includes the old crc value in the new one.
    m_page.put_u32(offset, crc_32(m_page.range(header_offset(), NodeLayout::HEADER_SIZE)));
}

auto Node::set_parent_id(PID parent_id) -> void
{
    CUB_EXPECT_NE(id(), PID::root());
    const auto offset = header_offset() + NodeLayout::PARENT_ID_OFFSET;
    m_page.put_u32(offset, parent_id.value);
}

auto Node::set_right_sibling_id(PID right_sibling_id) -> void
{
    CUB_EXPECT_TRUE(is_external());
    const auto offset = header_offset() + NodeLayout::RIGHT_SIBLING_ID_OFFSET;
    m_page.put_u32(offset, right_sibling_id.value);
}

auto Node::set_rightmost_child_id(PID rightmost_child_id) -> void
{
    CUB_EXPECT_FALSE(is_external());
    const auto offset = header_offset() + NodeLayout::RIGHTMOST_CHILD_ID_OFFSET;
    m_page.put_u32(offset, rightmost_child_id.value);
}

auto Node::set_cell_count(Size cell_count) -> void
{
    CUB_EXPECT_BOUNDED_BY(uint16_t, cell_count);
    m_page.put_u16(header_offset() + NodeLayout::CELL_COUNT_OFFSET, static_cast<uint16_t>(cell_count));
}

auto Node::set_free_count(Size free_count) -> void
{
    CUB_EXPECT_BOUNDED_BY(uint16_t, free_count);
    m_page.put_u16(header_offset() + NodeLayout::FREE_COUNT_OFFSET, static_cast<uint16_t>(free_count));
}

auto Node::set_cell_start(Index cell_start) -> void
{
    CUB_EXPECT_BOUNDED_BY(uint16_t, cell_start);
    m_page.put_u16(header_offset() + NodeLayout::CELL_START_OFFSET, static_cast<uint16_t>(cell_start));
}

auto Node::set_free_start(Index free_start) -> void
{
    CUB_EXPECT_BOUNDED_BY(uint16_t, free_start);
    m_page.put_u16(header_offset() + NodeLayout::FREE_START_OFFSET, static_cast<uint16_t>(free_start));
}

auto Node::set_frag_count(Size frag_count) -> void
{
    CUB_EXPECT_BOUNDED_BY(uint16_t, frag_count);
    m_page.put_u16(header_offset() + NodeLayout::FRAG_COUNT_OFFSET, static_cast<uint16_t>(frag_count));
}

auto Node::usable_space() const -> Size
{
    return m_usable_space;
}

Node::Node(Page page, bool reset_header)
    : m_page {std::move(page)}
{
    reset(reset_header);
    recompute_usable_space();
}

auto Node::is_external() const -> bool
{
    return m_page.type() == PageType::EXTERNAL_NODE;
}

auto Node::child_id(Index index) const -> PID
{
    CUB_EXPECT_FALSE(is_external());
    CUB_EXPECT_LE(index, cell_count());
    if (index < cell_count())
        return read_cell(index).left_child_id();
    return rightmost_child_id();
}

auto Node::read_key(Index index) const -> BytesView
{
    CUB_EXPECT_LT(index, cell_count());
    return read_cell(index).key();
}

auto Node::read_cell(Index index) const -> Cell
{
    CUB_EXPECT_LT(index, cell_count());
    return Cell::read_at(*this, cell_pointer(index));
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
    auto usable_space = gap_size() + frag_count();
    for (Index i {}, ptr {free_start()}; i < free_count(); ++i) {
        usable_space += m_page.get_u16(ptr + CELL_POINTER_SIZE);
        ptr = m_page.get_u16(ptr);
    }
    CUB_EXPECT_LE(usable_space, m_page.size() - cell_pointers_offset());
    m_usable_space = usable_space;
}

auto Node::gap_size() const -> Size
{
    const auto top = cell_start();
    const auto bottom = cell_area_offset();
    CUB_EXPECT_GE(top, bottom);
    return top - bottom;
}

auto Node::cell_pointer(Index index) const -> Index
{
    CUB_EXPECT_LT(index, cell_count());
    return m_page.get_u16(cell_pointers_offset() + index*CELL_POINTER_SIZE);
}

auto Node::max_usable_space() const -> Size
{
    return size() - NodeLayout::HEADER_SIZE - PageLayout::HEADER_SIZE;
}

auto Node::set_cell_pointer(Index index, Index cell_pointer) -> void
{
    CUB_EXPECT_LT(index, cell_count());
    CUB_EXPECT_LE(cell_pointer, m_page.size());
    m_page.put_u16(cell_pointers_offset() + index*CELL_POINTER_SIZE, static_cast<uint16_t>(cell_pointer));
}

auto Node::is_overflowing() const -> bool
{
    return m_overflow != std::nullopt;
}

auto Node::is_underflowing() const -> bool
{
    if (id().is_root())
        return cell_count() == 0;
    // Note that the maximally-sized cell has no overflow, which is why we subtract PAGE_ID_SIZE.
    const auto max_cell_size = MAX_CELL_HEADER_SIZE +
                               get_max_local(size()) +
                               CELL_POINTER_SIZE -
                               PAGE_ID_SIZE;
    return m_usable_space >= max_usable_space()/2 + max_cell_size;
}

auto Node::is_underflowing_() const -> bool
{
    if (id().is_root())
        return !cell_count();
    return m_usable_space >= max_usable_space() / 2; // TODO: removeme
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
    m_overflow.reset();
    return cell;
}

auto Node::insert_cell_pointer(Index cid, Index cell_pointer) -> void
{
    CUB_EXPECT_GE(cell_pointer, cell_area_offset());
    CUB_EXPECT_LT(cell_pointer, m_page.size());
    CUB_EXPECT_LE(cid, cell_count());
    const auto start = NodeLayout::content_offset(m_page.id());
    const auto offset = start + CELL_POINTER_SIZE*cid;
    const auto size = (cell_count()-cid) * CELL_POINTER_SIZE;
    auto chunk = m_page.mut_range(offset, size + CELL_POINTER_SIZE);
    mem_move(chunk.range(CELL_POINTER_SIZE), chunk, size);//todo:changed checkme
    set_cell_count(cell_count() + 1);
    set_cell_pointer(cid, cell_pointer);
    m_usable_space -= CELL_POINTER_SIZE;
}

auto Node::remove_cell_pointer(Index cid) -> void
{
    CUB_EXPECT_GT(cell_count(), 0);
    CUB_EXPECT_LT(cid, cell_count());
    const auto start = NodeLayout::header_offset(m_page.id()) + NodeLayout::HEADER_SIZE;
    const auto offset = start + CELL_POINTER_SIZE*cid;
    const auto size = (cell_count()-cid-1) * CELL_POINTER_SIZE;
    auto chunk = m_page.mut_range(offset, size + CELL_POINTER_SIZE);
    mem_move(chunk, chunk.range(CELL_POINTER_SIZE), size);//todo:changed checkme
    set_cell_count(cell_count() - 1);
    m_usable_space += CELL_POINTER_SIZE;
}

auto Node::set_child_id(Index index, PID child_id) -> void
{
    CUB_EXPECT_FALSE(is_external());
    CUB_EXPECT_LE(index, cell_count());
    if (index < cell_count()) {
        m_page.put_u32(cell_pointer(index), child_id.value);
    } else {
        set_rightmost_child_id(child_id);
    }
}

auto Node::allocate_from_free(Size needed_size) -> Index
{
    // NOTE: We use a value of zero to indicate that there is no previous pointer.
    Index prev_ptr {};
    auto curr_ptr = free_start();

    for (Index i {}; i < free_count(); ++i) {
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
        const auto top = cell_start() - needed_size;
        set_cell_start(top);
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
        set_frag_count(frag_count() + diff);
        set_free_count(free_count() - 1);
        if (is_first) {
            set_free_start(ptr2);
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
        set_frag_count(frag_count() + size);
    } else {
        m_page.put_u16(ptr, uint16_t(free_start()));
        m_page.put_u16(ptr + CELL_POINTER_SIZE, uint16_t(size));
        set_free_count(free_count() + 1);
        set_free_start(ptr);
    }
    m_usable_space += size;
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
    for (Index cid{}; cid < n; ++cid) {
        if (cid != to_skip)
            set_cell_pointer(cid, ptrs.at(cid));
    }
    const auto offset = cell_area_offset();
    m_page.write(_b(temp).range(offset, m_page.size() - offset), offset);
    set_cell_start(end);
    set_frag_count(0);
    set_free_count(0);
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
    CUB_EXPECT_LE(index, cell_count());

    const auto local_size = cell.size();

    // We don't have room to insert the cell pointer.
    if (cell_area_offset() + CELL_POINTER_SIZE > cell_start()) {
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
    if (offset < cell_start())
        set_cell_start(offset);
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
    // TODO: Allow keys of zero size? Current comparison routine should still work. Assertion allows this currently, although we could
    //       only have 1 zero-length key in the DB.
    CUB_EXPECT_GE(local_size, MIN_CELL_HEADER_SIZE);
    CUB_EXPECT_LE(local_size, get_max_local(m_page.size()) + MAX_CELL_HEADER_SIZE);
    CUB_EXPECT_LT(index, cell_count());
    CUB_EXPECT_FALSE(is_overflowing());
    give_free_space(cell_pointer(index), local_size);
    remove_cell_pointer(index);
}

auto Node::reset(bool reset_header) -> void
{
    if (reset_header) {
        auto chunk = m_page.mut_range(header_offset(), NodeLayout::HEADER_SIZE);
        mem_clear(chunk, chunk.size());
        set_cell_start(m_page.size());
    }
    m_overflow.reset();
    recompute_usable_space();
}

auto transfer_cell(Node &src, Node &dst, Index index) -> void
{
    CUB_EXPECT_EQ(src.type(), dst.type());
    auto cell = src.read_cell(index);
    const auto cell_size = cell.size();
    dst.insert(std::move(cell));
    src.remove_at(index, cell_size);
}

auto can_merge_siblings(const Node &Ln, const Node &rn, const Cell &separator) -> bool
{
    CUB_EXPECT_FALSE(Ln.is_overflowing());
    CUB_EXPECT_FALSE(rn.is_overflowing());
    CUB_EXPECT_FALSE(Ln.id().is_root());
    CUB_EXPECT_FALSE(rn.id().is_root());
    const auto page_size = Ln.size();
    CUB_EXPECT_EQ(page_size, rn.size());
    CUB_EXPECT_EQ(Ln.type(), rn.type());

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
    /* Transformation:
     *      1:[x..,        b,        y..]   -->   1:[x..,                 y..]
     *     X       3:[a..]    2:[c..]    Y  -->  X       3:[a.., b.., c..]    Y
     *            A       B  C       D      -->         A       B    C    D
     */

    if (Lc.is_external())
        Lc.set_right_sibling_id(rc.right_sibling_id());

    // Move the separator from the parent to the left child node.
    auto separator = parent.read_cell(index);
    if (!Lc.is_external()) {
        separator.set_left_child_id(Lc.rightmost_child_id()); // (1)
    } else {
        // TODO: NULL-ing out the left child ID causes it to not be written. See cell.h
        //       for another TOD0 concerning this weirdness. It's not causing any bugs
        //       that I know of, it's just an awful API that will likely cause bugs if
        //       anything is changed. Should fix.
        separator.set_left_child_id(PID::null());
    }
    const auto separator_size = separator.size();
    Lc.insert(std::move(separator));
    parent.remove_at(index, separator_size);

    // Transfer the rest of the cells. Lc shouldn't overflow.
    while (rc.cell_count()) {
        transfer_cell(rc, Lc, 0);
        CUB_EXPECT_FALSE(Lc.is_overflowing());
    }
    if (!Lc.is_external())
        Lc.set_rightmost_child_id(rc.rightmost_child_id()); // (2)
    parent.set_child_id(index, Lc.id());
    if (parent.rightmost_child_id() == rc.id())
        parent.set_rightmost_child_id(Lc.id()); // (3)

    /* Transformation:
     *         1:[a]        -->   1:[]
     *     3:[]     2:[b]   -->       3:[a, b]
     *         A   B     C  -->      A     B  C
     *
     * Pointers:
     *     (1) a.left_child_id      : 3 --> A  (Internal nodes only)
     *     (2) 3.rightmost_child_id : A --> C  (Internal nodes only)
     *     (3) 1.rightmost_child_id : 2 --> 3
     */

//    if (Lc.is_external())
//        Lc.set_right_sibling_id(rc.right_sibling_id());
//
//    // Move the separator from the parent to the left child node.
//    auto separator = parent.read_cell(index);
//    if (!Lc.is_external()) {
//        separator.set_left_child_id(Lc.rightmost_child_id()); // (1)
//    } else {
//        // TODO: NULL-ing out the left child ID causes it to not be written. See cell.h
//        //       for another TOD0 concerning this weirdness. It's not causing any bugs
//        //       that I know of, it's just an awful API that will likely cause bugs if
//        //       anything is changed. Should fix.
//        separator.set_left_child_id(PID::null());
//    }
//    const auto separator_size = separator.size();
//    Lc.insert(std::move(separator));
//    parent.remove_at(index, separator_size);
//
//    // Transfer the rest of the cells. Lc shouldn't overflow.
//    while (rc.cell_count()) {
//        transfer_cell(rc, Lc, 0);
//        CUB_EXPECT_FALSE(Lc.is_overflowing());
//    }
//    if (!Lc.is_external())
//        Lc.set_rightmost_child_id(rc.rightmost_child_id()); // (2)
//    parent.set_child_id(index, Lc.id());
//    if (parent.rightmost_child_id() == rc.id())
//        parent.set_rightmost_child_id(Lc.id()); // (3)
}

auto merge_right(Node &Lc, Node &rc, Node &parent, Index index) -> void
{
    /* Transformation:
     *      1:[x..,        b,        y..]   -->   1:[x..,                 y..]
     *     X       3:[a..]    2:[c..]    Y  -->  X       3:[a.., b.., c..]    Y
     *            A       B  C       D      -->         A       B    C    D
     */

    /* Transformation:
     *         1:[a]        -->   1:[]
     *     3:[]     2:[b]   -->       3:[a, b]
     *         A   B     C  -->      A     B  C
     */

    if (Lc.is_external())
        Lc.set_right_sibling_id(rc.right_sibling_id());

    // Move the separator from the source to the left child node.
    auto separator = parent.read_cell(index);
    if (!Lc.is_external()) {
        separator.set_left_child_id(Lc.rightmost_child_id());
        Lc.set_rightmost_child_id(rc.rightmost_child_id());
    } else {
        separator.set_left_child_id(PID::null());
    }
    const auto separator_size = separator.size();
    Lc.insert(std::move(separator));
    CUB_EXPECT_EQ(parent.child_id(index + 1), rc.id());
    parent.set_child_id(index + 1, Lc.id());
    parent.remove_at(index, separator_size);

    // Transfer the rest of the cells. Lc shouldn't overflow.
    while (rc.cell_count()) {
        transfer_cell(rc, Lc, 0);
        CUB_EXPECT_FALSE(Lc.is_overflowing());
    }

//    /* Transformation:
//     *         1:[a]        -->   1:[]
//     *     3:[]     2:[b]   -->       3:[a, b]
//     *         A   B     C  -->      A     B  C
//     *
//     * Pointers:
//     *     (1) a.left_child_id      : 3 --> A  (Internal nodes only)
//     *     (2) 3.rightmost_child_id : A --> C  (Internal nodes only)
//     *     (3) 1.rightmost_child_id : 2 --> 3
//     */
//
//    if (Lc.is_external())
//        Lc.set_right_sibling_id(rc.right_sibling_id());
//
//    // Move the separator from the source to the left child node.
//    auto separator = parent.read_cell(index - 1);
//    if (!Lc.is_external()) {
//        separator.set_left_child_id(Lc.rightmost_child_id());
//        Lc.set_rightmost_child_id(rc.rightmost_child_id());
//    } else {
//        separator.set_left_child_id(PID::null());
//    }
//    const auto separator_size = separator.size();
//    Lc.insert(std::move(separator));
//    CUB_EXPECT_EQ(parent.child_id(index), rc.id());
//    parent.set_child_id(index, Lc.id());
//    parent.remove_at(index - 1, separator_size);
//
//    // Transfer the rest of the cells. Lc shouldn't overflow.
//    while (rc.cell_count()) {
//        transfer_cell(rc, Lc, 0);
//        CUB_EXPECT_FALSE(Lc.is_overflowing());
//    }
}

} // cub