#include "node.h"
#include "utils/crc.h"
#include "utils/layout.h"
#include <spdlog/fmt/fmt.h>

namespace calico {

auto NodeHeader::parent_id(const Page &page) -> PageId
{
    return PageId {get_u64(page, header_offset(page) + NodeLayout::PARENT_ID_OFFSET)};
}

auto NodeHeader::right_sibling_id(const Page &page) -> PageId
{
    CALICO_EXPECT_EQ(page.type(), PageType::EXTERNAL_NODE);
    return PageId {get_u64(page, header_offset(page) + NodeLayout::RIGHT_SIBLING_ID_OFFSET)};
}

auto NodeHeader::reserved(const Page &page) -> std::uint64_t
{
    CALICO_EXPECT_EQ(page.type(), PageType::INTERNAL_NODE);
    return get_u64(page, header_offset(page) + NodeLayout::RESERVED_OFFSET);
}

auto NodeHeader::left_sibling_id(const Page &page) -> PageId
{
    CALICO_EXPECT_EQ(page.type(), PageType::EXTERNAL_NODE);
    return PageId {get_u64(page, header_offset(page) + NodeLayout::LEFT_SIBLING_ID_OFFSET)};
}

auto NodeHeader::rightmost_child_id(const Page &page) -> PageId
{
    CALICO_EXPECT_NE(page.type(), PageType::EXTERNAL_NODE);
    return PageId {get_u64(page, header_offset(page) + NodeLayout::RIGHTMOST_CHILD_ID_OFFSET)};
}

auto NodeHeader::cell_count(const Page &page) -> Size
{
    return get_u16(page, header_offset(page) + NodeLayout::CELL_COUNT_OFFSET);
}

auto NodeHeader::cell_start(const Page &page) -> Size
{
    return get_u16(page, header_offset(page) + NodeLayout::CELL_START_OFFSET);
}

auto NodeHeader::free_start(const Page &page) -> Size
{
    return get_u16(page, header_offset(page) + NodeLayout::FREE_START_OFFSET);
}

auto NodeHeader::frag_count(const Page &page) -> Size
{
    return get_u16(page, header_offset(page) + NodeLayout::FRAG_TOTAL_OFFSET);
}

auto NodeHeader::free_total(const Page &page) -> Size
{
    return get_u16(page, header_offset(page) + NodeLayout::FREE_TOTAL_OFFSET);
}

auto NodeHeader::set_parent_id(Page &page, PageId parent_id) -> void
{
    CALICO_EXPECT_NE(page.id(), PageId::root());
    const auto offset = header_offset(page) + NodeLayout::PARENT_ID_OFFSET;
    put_u64(page, offset, parent_id.value);
}

auto NodeHeader::set_right_sibling_id(Page &page, PageId right_sibling_id) -> void
{
    CALICO_EXPECT_EQ(page.type(), PageType::EXTERNAL_NODE);
    const auto offset = header_offset(page) + NodeLayout::RIGHT_SIBLING_ID_OFFSET;
    put_u64(page, offset, right_sibling_id.value);
}

auto NodeHeader::set_left_sibling_id(Page &page, PageId left_sibling_id) -> void
{
    CALICO_EXPECT_EQ(page.type(), PageType::EXTERNAL_NODE);
    const auto offset = header_offset(page) + NodeLayout::LEFT_SIBLING_ID_OFFSET;
    put_u64(page, offset, left_sibling_id.value);
}

auto NodeHeader::set_rightmost_child_id(Page &page, PageId rightmost_child_id) -> void
{
    CALICO_EXPECT_NE(page.type(), PageType::EXTERNAL_NODE);
    const auto offset = header_offset(page) + NodeLayout::RIGHTMOST_CHILD_ID_OFFSET;
    put_u64(page, offset, rightmost_child_id.value);
}

auto NodeHeader::set_cell_count(Page &page, Size cell_count) -> void
{
    put_u16(page, header_offset(page) + NodeLayout::CELL_COUNT_OFFSET, static_cast<uint16_t>(cell_count));
}

auto NodeHeader::set_cell_start(Page &page, Size cell_start) -> void
{
    put_u16(page, header_offset(page) + NodeLayout::CELL_START_OFFSET, static_cast<uint16_t>(cell_start));
}

auto NodeHeader::set_free_start(Page &page, Size free_start) -> void
{
    put_u16(page, header_offset(page) + NodeLayout::FREE_START_OFFSET, static_cast<uint16_t>(free_start));
}

auto NodeHeader::set_frag_count(Page &page, Size frag_count) -> void
{
    put_u16(page, header_offset(page) + NodeLayout::FRAG_TOTAL_OFFSET, static_cast<uint16_t>(frag_count));
}

auto NodeHeader::set_free_total(Page &page, Size free_total) -> void
{
    put_u16(page, header_offset(page) + NodeLayout::FREE_TOTAL_OFFSET, static_cast<uint16_t>(free_total));
}

auto NodeHeader::cell_directory_offset(const Page &page) -> Size
{
    return NodeLayout::content_offset(page.id());
}

auto NodeHeader::cell_area_offset(const Page &page) -> Size
{
    const auto cell_count = get_u16(page, header_offset(page) + NodeLayout::CELL_COUNT_OFFSET);
    return cell_directory_offset(page) + CELL_POINTER_SIZE * cell_count;
}

auto NodeHeader::header_offset(const Page &page) -> Size
{
    return NodeLayout::header_offset(page.id());
}

auto NodeHeader::gap_size(const Page &page) -> Size
{
    const auto top = cell_start(page);
    const auto bottom = cell_area_offset(page);
    CALICO_EXPECT_GE(top, bottom);
    return top - bottom;
}

auto NodeHeader::max_usable_space(const Page &page) -> Size
{
    return page.size() - cell_directory_offset(page);
}

auto CellDirectory::get_pointer(const Page &page, Size index) -> Pointer
{
    CALICO_EXPECT_LT(index, NodeHeader::cell_count(page));
    return {get_u16(page, NodeHeader::cell_directory_offset(page) + index * CELL_POINTER_SIZE)};
}

auto CellDirectory::set_pointer(Page &page, Size index, Pointer pointer) -> void
{
    CALICO_EXPECT_LT(index, NodeHeader::cell_count(page));
    CALICO_EXPECT_LE(pointer.value, page.size());
    put_u16(page, NodeHeader::cell_directory_offset(page) + index * CELL_POINTER_SIZE, static_cast<uint16_t>(pointer.value));
}

auto CellDirectory::insert_pointer(Page &page, Size index, Pointer pointer) -> void
{
    CALICO_EXPECT_GE(pointer.value, NodeHeader::cell_area_offset(page));
    CALICO_EXPECT_LT(pointer.value, page.size());
    CALICO_EXPECT_LE(index, NodeHeader::cell_count(page));
    const auto start = NodeLayout::content_offset(page.id());
    const auto offset = start + CELL_POINTER_SIZE * index;
    const auto size = (NodeHeader::cell_count(page) - index) * CELL_POINTER_SIZE;
    auto chunk = page.bytes(offset, size + CELL_POINTER_SIZE);
    mem_move(chunk.range(CELL_POINTER_SIZE), chunk, size);
    NodeHeader::set_cell_count(page, NodeHeader::cell_count(page) + 1);
    set_pointer(page, index, pointer);
}

auto CellDirectory::remove_pointer(Page &page, Size index) -> void
{
    CALICO_EXPECT_GT(NodeHeader::cell_count(page), 0);
    CALICO_EXPECT_LT(index, NodeHeader::cell_count(page));
    const auto start = NodeLayout::header_offset(page.id()) + NodeLayout::HEADER_SIZE;
    const auto offset = start + CELL_POINTER_SIZE * index;
    const auto size = (NodeHeader::cell_count(page) - index - 1) * CELL_POINTER_SIZE;
    auto chunk = page.bytes(offset, size + CELL_POINTER_SIZE);
    mem_move(chunk, chunk.range(CELL_POINTER_SIZE), size);
    NodeHeader::set_cell_count(page, NodeHeader::cell_count(page) - 1);
}

auto BlockAllocator::usable_space(const Page &page) -> Size
{
    return NodeHeader::free_total(page) + NodeHeader::gap_size(page);
}

auto BlockAllocator::reset(Page &page) -> void
{
    CALICO_EXPECT_TRUE(page.is_writable());
    NodeHeader::set_frag_count(page, 0);
    NodeHeader::set_free_start(page, 0);
    NodeHeader::set_free_total(page, 0);
}

auto BlockAllocator::compute_free_total(const Page &page) -> Size
{
    auto free_total = NodeHeader::frag_count(page);
    for (Size i {}, ptr {NodeHeader::free_start(page)}; ptr; ++i) {
        free_total += get_block_size(page, ptr);
        ptr = get_next_pointer(page, ptr);
    }
    CALICO_EXPECT_LE(free_total, page.size() - NodeHeader::cell_directory_offset(page));
    return free_total;
}

auto BlockAllocator::get_next_pointer(const Page &page, Size offset) -> Size
{
    return get_u16(page, offset);
}

auto BlockAllocator::get_block_size(const Page &page, Size offset) -> Size
{
    return get_u16(page, offset + CELL_POINTER_SIZE);
}

auto BlockAllocator::set_next_pointer(Page &page, Size offset, Size next_pointer) -> void
{
    CALICO_EXPECT_LT(next_pointer, page.size());
    return put_u16(page, offset, static_cast<uint16_t>(next_pointer));
}

auto BlockAllocator::set_block_size(Page &page, Size offset, Size block_size) -> void
{
    CALICO_EXPECT_GE(block_size, CELL_POINTER_SIZE + sizeof(uint16_t));
    CALICO_EXPECT_LT(block_size, NodeHeader::max_usable_space(page));
    return put_u16(page, offset + CELL_POINTER_SIZE, static_cast<uint16_t>(block_size));
}

auto BlockAllocator::allocate_from_free(Page &page, Size needed_size) -> Size
{
    // NOTE: We use a value of zero to indicate that there is no previous pointer.
    Size prev_ptr {};
    auto curr_ptr = NodeHeader::free_start(page);

    for (Size i {}; curr_ptr != 0; ++i) {
        if (needed_size <= get_block_size(page, curr_ptr))
            return take_free_space(page, prev_ptr, curr_ptr, needed_size);
        prev_ptr = curr_ptr;
        curr_ptr = get_next_pointer(page, curr_ptr);
    }
    return 0;
}

auto BlockAllocator::allocate_from_gap(Page &page, Size needed_size) -> Size
{
    if (needed_size <= NodeHeader::gap_size(page)) {
        const auto top = NodeHeader::cell_start(page) - needed_size;
        NodeHeader::set_cell_start(page, top);
        return top;
    }
    return 0;
}

auto BlockAllocator::allocate(Page &page, Size needed_size) -> Size
{
    CALICO_EXPECT_TRUE(page.is_writable());
    CALICO_EXPECT_LT(needed_size, page.size() - NodeLayout::content_offset(page.id()));

    if (needed_size > usable_space(page))
        return 0;
    if (auto ptr = allocate_from_free(page, needed_size))
        return ptr;
    return allocate_from_gap(page, needed_size);
}

/* Free block layout:
 *     .---------------------.-------------.---------------------------.
 *     |  Next Pointer (2B)  |  Size (2B)  |   Free FakeFile (Size-4 B)  |
 *     '---------------------'-------------'---------------------------'
 */
auto BlockAllocator::take_free_space(Page &page, Size ptr0, Size ptr1, Size needed_size) -> Size
{
    CALICO_EXPECT_LT(ptr0, page.size());
    CALICO_EXPECT_LT(ptr1, page.size());
    CALICO_EXPECT_LT(needed_size, page.size());
    const auto is_first = !ptr0;
    const auto ptr2 = get_next_pointer(page, ptr1);
    const auto free_size = get_block_size(page, ptr1);
    CALICO_EXPECT_GE(free_size, needed_size);
    const auto diff = free_size - needed_size;

    if (diff < 4) {
        NodeHeader::set_frag_count(page, NodeHeader::frag_count(page) + diff);

        if (is_first) {
            NodeHeader::set_free_start(page, ptr2);
        } else {
            set_next_pointer(page, ptr0, ptr2);
        }
    } else {
        set_block_size(page, ptr1, diff);
    }
    const auto free_total = NodeHeader::free_total(page);
    CALICO_EXPECT_GE(free_total, needed_size);
    NodeHeader::set_free_total(page, free_total - needed_size);
    return ptr1 + diff;
}

auto BlockAllocator::free(Page &page, Size ptr, Size size) -> void
{
    CALICO_EXPECT_LE(ptr + size, page.size());
    CALICO_EXPECT_GE(ptr, NodeLayout::content_offset(page.id()));

    if (size < FREE_BLOCK_HEADER_SIZE) {
        NodeHeader::set_frag_count(page, NodeHeader::frag_count(page) + size);
    } else {
        set_next_pointer(page, ptr, NodeHeader::free_start(page));
        set_block_size(page, ptr, size);
        NodeHeader::set_free_start(page, ptr);
    }
    NodeHeader::set_free_total(page, NodeHeader::free_total(page) + size);
}

auto Node::parent_id() const -> PageId
{
    return NodeHeader::parent_id(m_page);
}

auto Node::right_sibling_id() const -> PageId
{
    return NodeHeader::right_sibling_id(m_page);
}

auto Node::left_sibling_id() const -> PageId
{
    return NodeHeader::left_sibling_id(m_page);
}

auto Node::rightmost_child_id() const -> PageId
{
    return NodeHeader::rightmost_child_id(m_page);
}

auto Node::cell_count() const -> Size
{
    return NodeHeader::cell_count(m_page);
}

auto Node::set_parent_id(PageId parent_id) -> void
{
    NodeHeader::set_parent_id(m_page, parent_id);
}

auto Node::set_right_sibling_id(PageId right_sibling_id) -> void
{
    NodeHeader::set_right_sibling_id(m_page, right_sibling_id);
}

auto Node::set_rightmost_child_id(PageId rightmost_child_id) -> void
{
    NodeHeader::set_rightmost_child_id(m_page, rightmost_child_id);
}

auto Node::usable_space() const -> Size
{
    return BlockAllocator::usable_space(m_page);
}

auto Node::is_external() const -> bool
{
    return m_page.type() == PageType::EXTERNAL_NODE;
}

auto Node::child_id(Size index) const -> PageId
{
    CALICO_EXPECT_FALSE(is_external());
    CALICO_EXPECT_LE(index, cell_count());
    if (index < cell_count())
        return read_cell(index).left_child_id();
    return rightmost_child_id();
}

auto Node::read_key(Size index) const -> BytesView
{
    CALICO_EXPECT_LT(index, cell_count());
    return read_cell(index).key();
}

auto Node::read_cell(Size index) const -> Cell
{
    CALICO_EXPECT_LT(index, cell_count());
    return Cell::read_at(*this, CellDirectory::get_pointer(m_page, index).value);
}

auto Node::detach_cell(Size index, Scratch scratch) const -> Cell
{
    CALICO_EXPECT_LT(index, cell_count());
    auto cell = read_cell(index);
    cell.detach(scratch);
    return cell;
}

auto Node::extract_cell(Size index, Scratch scratch) -> Cell
{
    CALICO_EXPECT_LT(index, cell_count());
    auto cell = detach_cell(index, scratch);
    remove_at(index, cell.size());
    return cell;
}

auto Node::TEST_validate() const -> void
{
    const auto label = fmt::format("node {}: ", m_page.id().value);

    // The usable space is the total of all the free blocks, fragments, and the gap space.
    const auto usable_space = BlockAllocator::usable_space(m_page);

    // The used space is the total of the header, cell pointers list, and the cells.
    auto used_space = cell_area_offset();
    for (Size i {}; i < cell_count(); ++i) {
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
        switch (compare_three_way(key, read_key(static_cast<Size>(middle)))) {
            case ThreeWayComparison::EQ:
                return {static_cast<Size>(middle), true};
            case ThreeWayComparison::LT:
                upper = middle;
                break;
            case ThreeWayComparison::GT:
                lower = middle + 1;
        }
    }
    return {static_cast<Size>(lower), false};
}

auto Node::cell_area_offset() const -> Size
{
    return NodeHeader::cell_area_offset(m_page);
}

auto Node::header_offset() const -> Size
{
    return NodeHeader::header_offset(m_page);
}

auto Node::max_usable_space() const -> Size
{
    return NodeHeader::max_usable_space(m_page);
}

auto Node::is_overflowing() const -> bool
{
    return m_overflow != std::nullopt;
}

auto Node::is_underflowing() const -> bool
{
    if (id().is_root())
        return cell_count() == 0;
    return BlockAllocator::usable_space(m_page) > max_usable_space() / 2;
}

auto Node::overflow_cell() const -> const Cell &
{
    CALICO_EXPECT_TRUE(is_overflowing());
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

auto Node::set_child_id(Size index, PageId child_id) -> void
{
    CALICO_EXPECT_FALSE(is_external());
    CALICO_EXPECT_LE(index, cell_count());
    if (index < cell_count()) {
        put_u64(m_page, CellDirectory::get_pointer(m_page, index).value, child_id.value);
    } else {
        set_rightmost_child_id(child_id);
    }
}

auto Node::allocate(Size needed_size, std::optional<Size> skipped_cid) -> Size
{
    if (const auto ptr = BlockAllocator::allocate(m_page, needed_size))
        return ptr;
    defragment(skipped_cid);
    return BlockAllocator::allocate(m_page, needed_size);
}

auto Node::defragment() -> void
{
    defragment(std::nullopt);
}

auto Node::defragment(std::optional<Size> skipped_cid) -> void
{
    const auto n = cell_count();
    const auto to_skip = skipped_cid ? *skipped_cid : n;
    auto end = m_page.size();
    std::vector<Size> ptrs(n);

    // TODO: We're using scratch memory for this now. We only have one scratch buffer, shared among all nodes. This is okay right now
    //       since all tree operations happen in a single thread, so only one node can be defragmenting at any given time.

    for (Size index {}; index < n; ++index) {
        if (index == to_skip)
            continue;
        const auto cell = read_cell(index);
        end -= cell.size();
        cell.write(m_scratch.range(end));
        ptrs.at(index) = end;
    }
    for (Size index {}; index < n; ++index) {
        if (index != to_skip)
            CellDirectory::set_pointer(m_page, index, {ptrs.at(index)});
    }
    const auto offset = cell_area_offset();
    m_page.write(m_scratch.range(offset, m_page.size() - offset), offset);
    NodeHeader::set_cell_start(m_page, end);
    BlockAllocator::reset(m_page);
}


auto Node::insert(Cell cell) -> void
{
    const auto [index, should_be_false] = find_ge(cell.key());
    // Keys should be unique.
    CALICO_EXPECT_FALSE(should_be_false);
    insert_at(index, cell);
}

auto Node::insert_at(Size index, Cell cell) -> void
{
    CALICO_EXPECT_FALSE(is_overflowing());
    CALICO_EXPECT_LE(index, cell_count());
    CALICO_EXPECT_EQ(is_external(), cell.is_external());

    const auto local_size = cell.size();

    // We don't have room to insert the cell pointer.
    if (cell_area_offset() + CELL_POINTER_SIZE > NodeHeader::cell_start(m_page)) {
        if (BlockAllocator::usable_space(m_page) >= local_size + CELL_POINTER_SIZE) {
            defragment();
            return insert_at(index, cell);
        }
        set_overflow_cell(cell);
        return;
    }
    // insert a dummy cell pointer to save the slot.
    CellDirectory::insert_pointer(m_page, index, {m_page.size() - 1});

    // allocate space for the cell. This call may defragment the node.
    const auto offset = allocate(local_size, index);

    // We don't have room to insert the cell.
    if (!offset) {
        CALICO_EXPECT_LT(BlockAllocator::usable_space(m_page), local_size + CELL_POINTER_SIZE);
        set_overflow_cell(cell);
        CellDirectory::remove_pointer(m_page, index);
        return;
    }
    // Now we can fill in the dummy cell pointer and write_all the cell.
    CellDirectory::set_pointer(m_page, index, {offset});
    cell.write(m_page.bytes(offset, cell.size()));

    // Adjust the start of the cell content area.
    if (offset < NodeHeader::cell_start(m_page))
        NodeHeader::set_cell_start(m_page, offset);

    //    CALICO_VALIDATE(validate());
}

auto Node::remove(BytesView key) -> bool
{
    if (auto [index, found_eq] = find_ge(key); found_eq) {
        remove_at(index, read_cell(index).size());
        return true;
    }
    return false;
}

auto Node::remove_at(Size index, Size local_size) -> void
{
    CALICO_EXPECT_GE(local_size, MIN_CELL_HEADER_SIZE);
    CALICO_EXPECT_LE(local_size, get_max_local(m_page.size()) + MAX_CELL_HEADER_SIZE);
    CALICO_EXPECT_LT(index, cell_count());
    CALICO_EXPECT_FALSE(is_overflowing());
    BlockAllocator::free(m_page, CellDirectory::get_pointer(m_page, index).value, local_size);
    CellDirectory::remove_pointer(m_page, index);
    //    CALICO_VALIDATE(validate());
}

auto Node::reset(bool reset_header) -> void
{
    if (reset_header) {
        CALICO_EXPECT_TRUE(m_page.is_writable());
        auto chunk = m_page.bytes(header_offset(), NodeLayout::HEADER_SIZE);
        mem_clear(chunk, chunk.size());
        NodeHeader::set_cell_start(m_page, m_page.size());
    }
    m_overflow.reset();
}

auto transfer_cell(Node &src, Node &dst, Size index) -> void
{
    CALICO_EXPECT_EQ(src.type(), dst.type());
    auto cell = src.read_cell(index);
    const auto cell_size = cell.size();
    dst.insert(cell);
    src.remove_at(index, cell_size);
}

auto accumulate_occupied_space(const Node &Ln, const Node &rn)
{
    const auto page_size = Ln.size();
    CALICO_EXPECT_EQ(page_size, rn.size());
    CALICO_EXPECT_EQ(Ln.type(), rn.type());
    CALICO_EXPECT_FALSE(Ln.is_overflowing());
    CALICO_EXPECT_FALSE(rn.is_overflowing());
    CALICO_EXPECT_FALSE(Ln.id().is_root());
    CALICO_EXPECT_FALSE(rn.id().is_root());
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

auto internal_merge_left(Node &Lc, Node &rc, Node &parent, Size index) -> void
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
    CALICO_EXPECT_FALSE(Lc.is_overflowing());

    Lc.set_rightmost_child_id(rc.rightmost_child_id());
    parent.set_child_id(index, Lc.id());
}

auto external_merge_left(Node &Lc, Node &rc, Node &parent, Size index) -> void
{
    Lc.set_right_sibling_id(rc.right_sibling_id());

    // Move the separator from the parent to the left child node.
    auto separator = parent.read_cell(index);
    parent.remove_at(index, separator.size());

    while (rc.cell_count())
        transfer_cell(rc, Lc, 0);
    CALICO_EXPECT_FALSE(Lc.is_overflowing());
    parent.set_child_id(index, Lc.id());
}

auto merge_left(Node &Lc, Node &rc, Node &parent, Size index) -> void
{
    if (Lc.is_external()) {
        external_merge_left(Lc, rc, parent, index);
    } else {
        internal_merge_left(Lc, rc, parent, index);
    }
}

auto internal_merge_right(Node &Lc, Node &rc, Node &parent, Size index) -> void
{
    // Move the separator from the source to the left child node.
    auto separator = parent.read_cell(index);
    const auto separator_size = separator.size();
    separator.set_left_child_id(Lc.rightmost_child_id());
    Lc.set_rightmost_child_id(rc.rightmost_child_id());
    Lc.insert(separator);
    CALICO_EXPECT_EQ(parent.child_id(index + 1), rc.id());
    parent.set_child_id(index + 1, Lc.id());
    parent.remove_at(index, separator_size);

    // Transfer the rest of the cells. Lc shouldn't overflow.
    while (rc.cell_count()) {
        transfer_cell(rc, Lc, 0);
        CALICO_EXPECT_FALSE(Lc.is_overflowing());
    }
}

auto external_merge_right(Node &Lc, Node &rc, Node &parent, Size index) -> void
{
    Lc.set_right_sibling_id(rc.right_sibling_id());

    auto separator = parent.read_cell(index);
    CALICO_EXPECT_EQ(parent.child_id(index + 1), rc.id());
    parent.set_child_id(index + 1, Lc.id());
    parent.remove_at(index, separator.size());

    while (rc.cell_count())
        transfer_cell(rc, Lc, 0);
    CALICO_EXPECT_FALSE(Lc.is_overflowing());
}

auto merge_right(Node &Lc, Node &rc, Node &parent, Size index) -> void
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

    CALICO_EXPECT_TRUE(root.is_overflowing());
    child.set_overflow_cell(root.take_overflow_cell());

    root.reset(true);
    root.page().set_type(PageType::INTERNAL_NODE);
    root.set_rightmost_child_id(child.id());
    child.set_parent_id(PageId::root());
}

auto merge_root(Node &root, Node &child) -> void
{
    CALICO_EXPECT_EQ(root.rightmost_child_id(), child.id());
    const auto needs_defragment = NodeHeader::free_start(child.page()) || NodeHeader::frag_count(child.page());
    if (needs_defragment)
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
    Size counter {};
    while (predicate(src, dst, counter++)) {
        const auto last = src.cell_count() - 1;
        auto cell = src.read_cell(last);
        const auto cell_size = cell.size();
        dst.insert_at(0, cell);
        CALICO_EXPECT_FALSE(dst.is_overflowing());
        src.remove_at(last, cell_size);
    }
}

auto split_non_root_fast_internal(Node &Ln, Node &rn, Cell overflow, Size overflow_index, Scratch scratch) -> Cell
{
    transfer_cells_right_while(Ln, rn, [overflow_index](const Node &src, const Node &, Size) {
        return src.cell_count() > overflow_index;
    });

    if (overflow.is_attached())
        overflow.detach(scratch, true);
    overflow.set_is_external(false);
    overflow.set_left_child_id(Ln.id());
    return overflow;
}

auto split_non_root_fast_external(Node &Ln, Node &rn, Cell overflow, Size overflow_index, Scratch scratch) -> Cell
{
    // Note that we need to insert the overflow cell into either Ln or rn no matter what, even if it ends up being the separator.
    transfer_cells_right_while(Ln, rn, [&overflow, overflow_index](const Node &src, const Node &, Size counter) {
        const auto goes_in_src = src.cell_count() > overflow_index;
        const auto has_no_room = src.usable_space() < overflow.size() + CELL_POINTER_SIZE;
        return !counter || (goes_in_src && has_no_room);
    });

    if (Ln.cell_count() > overflow_index) {
        Ln.insert_at(overflow_index, overflow);
        CALICO_EXPECT_FALSE(Ln.is_overflowing());
    } else {
        rn.insert_at(0, overflow);
        CALICO_EXPECT_FALSE(rn.is_overflowing());
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
    CALICO_EXPECT_FALSE(should_be_false);

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
        transfer_cells_right_while(Ln, rn, [&overflow](const Node &src, const Node &, Size counter) {
            return !counter || src.usable_space() < overflow.size() + CELL_POINTER_SIZE;
        });
        Ln.insert_at(0, overflow);
        CALICO_EXPECT_FALSE(Ln.is_overflowing());

    } else if (overflow_idx == Ln.cell_count()) {
        // Just transfer a single cell in this case. This should reduce the number of splits during a sequential write, which seems to be
        // a common use case.
        transfer_cells_right_while(Ln, rn, [](const Node &, const Node &, Size counter) {
            return !counter;
        });
        rn.insert_at(rn.cell_count(), overflow);
        CALICO_EXPECT_FALSE(rn.is_overflowing());
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
    CALICO_EXPECT_FALSE(falsy);

    rn.set_rightmost_child_id(Ln.rightmost_child_id());
    rn.set_parent_id(Ln.parent_id());

    if (overflow_idx > 0 && overflow_idx < Ln.cell_count()) {
        Ln.set_rightmost_child_id(overflow.left_child_id());
        return split_non_root_fast_internal(Ln, rn, overflow, overflow_idx, scratch);

    } else if (overflow_idx == 0) {
        // TODO: Split the other way in this case, as we are possibly inserting reverse sequentially?
        transfer_cells_right_while(Ln, rn, [&overflow](const Node &src, const Node &, Size counter) {
            return !counter || src.usable_space() < overflow.size() + CELL_POINTER_SIZE;
        });
        Ln.insert_at(0, overflow);
        CALICO_EXPECT_FALSE(Ln.is_overflowing());

    } else if (overflow_idx == Ln.cell_count()) {
        // Just transfer a single cell in this case. This should reduce the number of splits during a sequential write, which seems to be
        // a common use case. If we want to change this behavior, we just need to make sure that rn still has room for the overflow cell.
        transfer_cells_right_while(Ln, rn, [](const Node &, const Node &, Size counter) {
            return !counter;
        });
        rn.insert_at(rn.cell_count(), overflow);
        CALICO_EXPECT_FALSE(rn.is_overflowing());
    }

    auto separator = Ln.extract_cell(Ln.cell_count() - 1, scratch);
    Ln.set_rightmost_child_id(separator.left_child_id());
    separator.set_left_child_id(Ln.id());
    return separator;
}

auto split_non_root(Node &Ln, Node &rn, Scratch scratch) -> Cell
{
    CALICO_EXPECT_TRUE(Ln.is_overflowing());
    CALICO_EXPECT_EQ(Ln.is_external(), rn.is_external());
    if (Ln.is_external())
        return split_external_non_root(Ln, rn, scratch);
    return split_internal_non_root(Ln, rn, scratch);
}

} // namespace calico