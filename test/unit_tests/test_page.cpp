#include <gtest/gtest.h>

#include "calico/options.h"
#include "page/deltas.h"
#include "random.h"
#include "unit_tests.h"

namespace calico {

namespace internal {
    extern std::uint32_t random_seed;
} // namespace internal

class DeltaCompressionTest: public testing::Test {
public:
    static constexpr Size PAGE_SIZE {0x200};

    [[nodiscard]]
    auto build_deltas(const std::vector<PageDelta> &unordered)
    {
        std::vector<PageDelta> deltas;
        for (const auto &delta: unordered)
            insert_delta(deltas, delta);
        compress_deltas(deltas);
        return deltas;
    }

    [[nodiscard]]
    auto insert_random_delta(std::vector<PageDelta> &deltas)
    {
        static constexpr Size MIN_DELTA_SIZE {1};
        const auto offset = random.get(PAGE_SIZE - MIN_DELTA_SIZE);
        const auto size = random.get(PAGE_SIZE - offset);
        insert_delta(deltas, {offset, size});
    }

    Random random {internal::random_seed};
};

TEST_F(DeltaCompressionTest, CompressingNothingDoesNothing)
{
    const auto empty = build_deltas({});
    ASSERT_TRUE(empty.empty());
}

TEST_F(DeltaCompressionTest, InsertEmptyDeltaDeathTest)
{
    std::vector<PageDelta> deltas;
    ASSERT_DEATH(insert_delta(deltas, {123, 0}), EXPECTATION_MATCHER);
}

TEST_F(DeltaCompressionTest, CompressingSingleDeltaDoesNothing)
{
    const auto single = build_deltas({{123, 1}});
    ASSERT_EQ(single.size(), 1);
    ASSERT_EQ(single.front().offset, 123);
    ASSERT_EQ(single.front().size, 1);
}

TEST_F(DeltaCompressionTest, DeltasAreOrdered)
{
    const auto deltas = build_deltas({
        {20, 2},
        {60, 6},
        {50, 5},
        {10, 1},
        {90, 9},
        {70, 7},
        {40, 4},
        {80, 8},
        {30, 3},
    });

    Size i {1};
    ASSERT_TRUE(std::all_of(cbegin(deltas), cend(deltas), [&i](const auto &delta) {
        const auto j = std::exchange(i, i + 1);
        return delta.offset == 10 * j && delta.size == j;
    }));
    ASSERT_EQ(deltas.size(), 9);
}

TEST_F(DeltaCompressionTest, DeltasAreNotRepeated)
{
    const auto deltas = build_deltas({
        {20, 2},
        {50, 5},
        {40, 4},
        {10, 1},
        {20, 2},
        {30, 3},
        {50, 5},
        {40, 4},
        {30, 3},
        {10, 1},
    });

    Size i {1};
    ASSERT_TRUE(std::all_of(cbegin(deltas), cend(deltas), [&i](const auto &delta) {
        const auto j = std::exchange(i, i + 1);
        return delta.offset == 10 * j && delta.size == j;
    }));
    ASSERT_EQ(deltas.size(), 5);
}

TEST_F(DeltaCompressionTest, OverlappingDeltasAreMerged)
{
    auto deltas = build_deltas({
        {0, 10},
        {20, 10},
        {40, 10},
    });

    insert_delta(deltas, {5, 10});
    insert_delta(deltas, {30, 10});
    compress_deltas(deltas);

    ASSERT_EQ(deltas.size(), 2);
    ASSERT_EQ(deltas[0].size, 15);
    ASSERT_EQ(deltas[0].offset, 0);
    ASSERT_EQ(deltas[1].size, 30);
    ASSERT_EQ(deltas[1].offset, 20);
}

TEST_F(DeltaCompressionTest, SanityCheck)
{
    static constexpr Size NUM_INSERTS {100};
    static constexpr Size MAX_DELTA_SIZE {10};
    std::vector<PageDelta> deltas;
    for (Size i {}; i < NUM_INSERTS; ++i) {
        const auto offset = random.get(PAGE_SIZE - MAX_DELTA_SIZE);
        const auto size = random.get(1, MAX_DELTA_SIZE);
        insert_delta(deltas, PageDelta {offset, size});
    }
    compress_deltas(deltas);

    std::vector<int> covering(PAGE_SIZE);
    for (const auto &[offset, size]: deltas) {
        for (Size i {}; i < size; ++i) {
            ASSERT_EQ(covering.at(offset + i), 0);
            covering[offset + i]++;
        }
    }
}


//
//class PageBacking {
//public:
//    explicit PageBacking(Size page_size)
//        : m_map {page_size}
//        , m_scratch {page_size}
//        , m_page_size {page_size} {}
//
//    auto get_page(identifier id) -> Page
//    {
//        m_map.emplace(id, std::string(m_page_size, '\x00'));
//        return Page {{id, stob(m_map[id]), nullptr, true, false}};
//    }
//
//private:
//    std::unordered_map<identifier, std::string, identifier::hash> m_map;
//    RollingScratchManager m_scratch;
//    Size m_page_size {};
//};
//
//class PageTests: public testing::Test {
//public:
//    static constexpr Size page_size = 0x100;
//
//    PageTests()
//        : m_backing {page_size} {}
//
//    auto get_page(identifier id) -> Page
//    {
//        return m_backing.get_page(id);
//    }
//
//private:
//    PageBacking m_backing;
//};
//
//TEST_F(PageTests, HeaderFields)
//{
//    auto page = get_page(identifier::root());
//    page.set_type(PageType::EXTERNAL_NODE);
//    ASSERT_EQ(page.type(), PageType::EXTERNAL_NODE);
//}
//
//TEST_F(PageTests, FreshPagesAreEmpty)
//{
//    auto page = get_page(identifier::root());
//    ASSERT_TRUE(page.view(0) == stob(std::string(page_size, '\x00')));
//}
//
//class CellBacking {
//public:
//    explicit CellBacking(Size page_size)
//        : scratch {page_size}
//        , page_size {page_size} {}
//
//    auto get_cell(const std::string &key) -> Cell
//    {
//        Cell::Parameters param;
//        param.key = stob(key);
//        param.page_size = page_size;
//        param.is_external = false;
//        Cell cell {param};
//        cell.detach(scratch.get());
//        cell.set_left_child_id(identifier {123ULL});
//        return cell;
//    }
//
//    auto get_cell(const std::string &key, const std::string &value, bool is_external, identifier overflow_id = identifier::null()) -> Cell
//    {
//        const auto local_value_size = get_local_value_size(key.size(), value.size(), page_size);
//        Cell::Parameters param;
//        param.key = stob(key);
//        param.local_value = stob(value);
//        param.value_size = value.size();
//        param.page_size = page_size;
//        param.is_external = is_external;
//
//        if (local_value_size != value.size()) {
//            CALICO_EXPECT_LT(local_value_size, value.size());
//            param.local_value.truncate(local_value_size);
//            param.overflow_id = overflow_id;
//        }
//        Cell cell {param};
//        cell.detach(scratch.get());
//        return cell;
//    }
//
//    RollingScratchManager scratch;
//    Size page_size {};
//};
////
////class NodeComponentBacking: public PageBacking {
////public:
////    explicit NodeComponentBacking(Size page_size)
////        : PageBacking {page_size}
////        , backing {get_page(identifier {2})}
////    {
////        header.set_cell_start(backing.size());
////        allocator.reset();
////    }
////
////    Page backing;
////    NodeHeader header;
////    CellDirectory directory;
////    BlockAllocator allocator;
////};
////
////class NodeComponentTests: public testing::Test {
////public:
////    static constexpr Size PAGE_SIZE {0x100};
////    NodeComponentTests()
////        : backing {PAGE_SIZE} {}
////
////    NodeComponentBacking backing;
////};
////
////class NodeHeaderTests: public NodeComponentTests {
////public:
////    auto header() -> NodeHeader&
////    {
////        return backing.header;
////    }
////};
////
////TEST_F(NodeHeaderTests, SetChildOfExternalNodeDeathTest)
////{
////    backing.backing.set_type(PageType::EXTERNAL_NODE);
////    ASSERT_DEATH(header().set_rightmost_child_id(identifier {123}), EXPECTATION_MATCHER);
////}
////
////TEST_F(NodeHeaderTests, SetSiblingOfInternalNodeDeathTest)
////{
////    backing.backing.set_type(PageType::INTERNAL_NODE);
////    ASSERT_DEATH(header().set_right_sibling_id(identifier {123}), EXPECTATION_MATCHER);
////}
////
////TEST_F(NodeHeaderTests, FieldsAreConsistent)
////{
////    backing.backing.set_type(PageType::EXTERNAL_NODE);
////    header().set_parent_id(identifier {1});
////    header().set_right_sibling_id(identifier {2});
////    header().set_cell_start(3);
////    header().set_free_start(4);
//////    header().set_free_count(5);
////    header().set_frag_count(6);
////    header().set_cell_count(7);
////    ASSERT_EQ(header().parent_id(), identifier {1});
////    ASSERT_EQ(header().right_sibling_id(), identifier {2});
////    ASSERT_EQ(header().cell_start(), 3);
////    ASSERT_EQ(header().free_start(), 4);
//////    ASSERT_EQ(header().free_count(), 5);
////    ASSERT_EQ(header().frag_count(), 6);
////    ASSERT_EQ(header().record_count(), 7);
////
////    header().set_free_start(0); // TODO
//////    header().set_free_count(0); // TODO
////}
////
////class CellDirectoryTests: public NodeComponentTests {
////public:
////    auto directory() -> CellDirectory&
////    {
////        return backing.directory;
////    }
////
////    [[nodiscard]] auto record_count() const -> Size
////    {
////        return backing.header.record_count();
////    }
////};
////
////TEST_F(CellDirectoryTests, FreshDirectoryIsEmpty)
////{
////    ASSERT_EQ(record_count(), 0);
////}
////
////TEST_F(CellDirectoryTests, RemoveFromEmptyDirectoryDeathTest)
////{
////    ASSERT_DEATH(directory().remove_pointer(0), EXPECTATION_MATCHER);
////}
////
////TEST_F(CellDirectoryTests, AccessNonexistentPointerDeathTest)
////{
////    ASSERT_DEATH(directory().set_pointer(0, {100}), EXPECTATION_MATCHER);
////    ASSERT_DEATH((void)directory().get_pointer(0), EXPECTATION_MATCHER);
////}
////
////TEST_F(CellDirectoryTests, ModifyingDirectoryChangesCellCount)
////{
////    directory().insert_pointer(0, {100});
////    ASSERT_EQ(record_count(), 1);
////    directory().remove_pointer(0);
////    ASSERT_EQ(record_count(), 0);
////}
////
////TEST_F(CellDirectoryTests, InsertedPointersCanBeRead)
////{
////    directory().insert_pointer(0, {120});
////    directory().insert_pointer(0, {110});
////    directory().insert_pointer(0, {100});
////    ASSERT_EQ(directory().get_pointer(0).value, 100);
////    ASSERT_EQ(directory().get_pointer(1).value, 110);
////    ASSERT_EQ(directory().get_pointer(2).value, 120);
////}
////
////class BlockAllocatorTests: public NodeComponentTests {
////public:
////    auto allocator() -> BlockAllocator&
////    {
////        return backing.allocator;
////    }
////
////    [[nodiscard]] auto free_start() const -> Index
////    {
////        return backing.header.free_start();
////    }
////
////    [[nodiscard]] auto frag_count() const -> Size
////    {
////        return backing.header.frag_count();
////    }
////
////    [[nodiscard]] auto max_usable_space() const -> Size
////    {
////        return backing.header.max_usable_space();
////    }
////};
////
////TEST_F(BlockAllocatorTests, FreshAllocatorIsEmpty)
////{
////    ASSERT_EQ(free_start(), 0);
////
////    // `free_start()` should be ignored if `free_count()` is zero. TODO: Not true anymore, we use a free_start of 0 to indicate "no free blocks".
//////    ASSERT_EQ(free_count(), 0);
////    ASSERT_EQ(frag_count(), 0);
////    ASSERT_EQ(allocator().usable_space(), max_usable_space());
////}
////
////TEST_F(BlockAllocatorTests, AllocatingBlockFromGapReducesUsableSpace)
////{
////    allocator().allocate(10);
////    ASSERT_EQ(allocator().usable_space(), max_usable_space() - 10);
////}
////
////TEST_F(BlockAllocatorTests, FreeingBlockIncreasesUsableSpace)
////{
////    allocator().free(allocator().allocate(10), 10);
////    ASSERT_EQ(allocator().usable_space(), max_usable_space());
////}
////
////TEST_F(BlockAllocatorTests, FreedMemoryIsMaintained)
////{
////    const auto a = allocator().allocate(10);
////    const auto b = allocator().allocate(10);
////    const auto c = allocator().allocate(3);
////    ASSERT_EQ(allocator().usable_space(), max_usable_space() - 23);
////
////    allocator().free(a, 10);
//////    ASSERT_EQ(free_count(), 1);
////    ASSERT_EQ(free_start(), a);
////
////    allocator().free(b, 10);
//////    ASSERT_EQ(free_count(), 2);
////    ASSERT_EQ(free_start(), b);
////
////    allocator().free(c, 3);
//////    ASSERT_EQ(free_count(), 2);
////    ASSERT_EQ(free_start(), b);
////    ASSERT_EQ(frag_count(), 3);
////    ASSERT_EQ(allocator().usable_space(), max_usable_space());
////}
////
////TEST_F(BlockAllocatorTests, SanityCheck)
////{
////    static constexpr Size NUM_ITERATIONS {10};
////    static constexpr Size MIN_SIZE {MIN_CELL_HEADER_SIZE + 1};
////    static constexpr Size MAX_SIZE {MIN_SIZE * 3};
////    std::unordered_map<Index, Size> allocations;
////    auto usable_space = max_usable_space();
////    Random random {internal::random_seed};
////
////    for (Index i {}; i < NUM_ITERATIONS; ++i) {
////        for (; ; ) {
////            const auto size = random.next_int(MIN_SIZE, MAX_SIZE);
////            if (const auto ptr = allocator().allocate(size)) {
////                auto itr = allocations.find(ptr);
////                ASSERT_EQ(itr, allocations.end());
////                allocations.emplace(ptr, size);
////                usable_space -= size;
////                ASSERT_EQ(allocator().usable_space(), usable_space);
////            } else {
////                break;
////            }
////        }
////        for (const auto &[index, size]: allocations) {
////            allocator().free(index, size);
////            usable_space += size;
////            ASSERT_EQ(allocator().usable_space(), usable_space);
////        }
////        allocations.clear();
////    }
////}
////
////class FreeBlockTests: public BlockAllocatorTests {
////public:
////    static constexpr Size NUM_BLOCKS {3};
////    static constexpr Size BLOCK_TOTAL {28};
////    static constexpr Size BLOCK_SIZES[] {16, 8, 4};
////
////    FreeBlockTests()
////    {
////        // free_start() -> c -> b -> a -> NULL
////        const auto a = allocator().allocate(BLOCK_SIZES[0]);
////        const auto b = allocator().allocate(BLOCK_SIZES[1]);
////        const auto c = allocator().allocate(BLOCK_SIZES[2]);
////        EXPECT_EQ(allocator().usable_space(), max_usable_space() - BLOCK_TOTAL);
////        allocator().free(a, BLOCK_SIZES[0]);
////        allocator().free(b, BLOCK_SIZES[1]);
////        allocator().free(c, BLOCK_SIZES[2]);
//////        EXPECT_EQ(free_count(), NUM_BLOCKS);
////        EXPECT_EQ(free_start(), c);
////        EXPECT_EQ(allocator().usable_space(), max_usable_space());
////    }
////};
////
////TEST_F(FreeBlockTests, TakeWholeBlock)
////{
////    // Take all of block `a`.
////    allocator().allocate(BLOCK_SIZES[0]);
//////    ASSERT_EQ(free_count(), NUM_BLOCKS - 1);
////    ASSERT_EQ(allocator().usable_space(), max_usable_space() - BLOCK_SIZES[0]);
////}
////
////TEST_F(FreeBlockTests, TakePartialBlock)
////{
////    // Take 3/4 of block `a`, which remains a free block.
////    allocator().allocate(3 * BLOCK_SIZES[0] / 4);
//////    ASSERT_EQ(free_count(), NUM_BLOCKS);
////    ASSERT_EQ(allocator().usable_space(), max_usable_space() - 3*BLOCK_SIZES[0]/4);
////}
////
////TEST_F(FreeBlockTests, ReduceBlockToFragments)
////{
////    // Take most of block `a`, which becomes 3 fragment bytes.
////    allocator().allocate(BLOCK_SIZES[0]  - 3);
//////    ASSERT_EQ(free_count(), NUM_BLOCKS - 1);
////    ASSERT_EQ(frag_count(), 3);
////    ASSERT_EQ(allocator().usable_space(), max_usable_space() - BLOCK_SIZES[0] + 3);
////}
//
//class NodeBacking: public PageBacking {
//public:
//    explicit NodeBacking(Size page_size)
//        : PageBacking {page_size},
//          scratch(page_size, '\x00')
//    {}
//
//    auto get_node(identifier id, PageType type) -> Node
//    {
//        Node node {get_page(id), true, scratch.data()};
//        node.page().set_type(type);
//        return node;
//    }
//
//    std::string scratch;
//};
//
//class NodeTests: public PageTests {
//public:
//    NodeTests()
//        : node_backing {page_size}
//        , cell_backing {page_size}
//        , overflow_value(page_size, 'x') {}
//
//    auto make_node(identifier id, PageType type)
//    {
//        return node_backing.get_node(id, type);
//    }
//
//    Random random {internal::random_seed};
//    NodeBacking node_backing;
//    CellBacking cell_backing;
//    std::string normal_value {"world"};
//    std::string overflow_value;
//    identifier arbitrary_pid {2ULL};
//};
//
//TEST_F(NodeTests, RootNodeFieldsAreDistinct)
//{
//    auto root = make_node(identifier::root(), PageType::EXTERNAL_NODE);
//    FileHeaderReader reader {root.page().view(0, FileLayout::HEADER_SIZE)};
//    FileHeaderWriter writer {root.page().bytes(0, FileLayout::HEADER_SIZE)};
//
//    const auto number = random.next_int(std::numeric_limits<std::uint64_t>::max() / 2,
//                                                        std::numeric_limits<std::uint64_t>::max());
//
//    writer.set_page_count(number);
//    writer.set_free_start(identifier {number * 10});
//    writer.set_record_count(number * 20);
//    writer.set_flushed_lsn(SeqNum {number * 30});
//    writer.set_page_size(0x1234);
//    writer.update_magic_code();
//    writer.update_header_crc();
//    root.page().set_lsn(SeqNum {number * 40});
//    NodeHeader::set_right_sibling_id(root.page(), identifier {number * 50});
//    NodeHeader::set_left_sibling_id(root.page(), identifier {number * 60});
//    NodeHeader::set_cell_count(root.page(), 0x2345);
//    NodeHeader::set_cell_start(root.page(), 0x3456);
//    NodeHeader::set_frag_count(root.page(), 0x4567);
//    NodeHeader::set_free_start(root.page(), 0x5678);
//    NodeHeader::set_free_total(root.page(), 0x6789);
//    CALICO_EXPECT_EQ(root.page().lsn(), SeqNum {number * 40});
//    CALICO_EXPECT_EQ(root.page().type(), PageType::EXTERNAL_NODE);
//    ASSERT_TRUE(reader.is_magic_code_consistent());
//    ASSERT_TRUE(reader.is_header_crc_consistent());
//    ASSERT_EQ(reader.page_count(), number);
//    ASSERT_EQ(reader.free_start(), number * 10);
//    ASSERT_EQ(reader.record_count(), number * 20);
//    ASSERT_EQ(reader.flushed_lsn(), number * 30);
//    ASSERT_EQ(reader.page_size(), 0x1234);
//    ASSERT_EQ(NodeHeader::right_sibling_id(root.page()), identifier {number * 50});
//    ASSERT_EQ(NodeHeader::left_sibling_id(root.page()), identifier {number * 60});
//    ASSERT_EQ(NodeHeader::cell_count(root.page()), 0x2345);
//    ASSERT_EQ(NodeHeader::cell_start(root.page()), 0x3456);
//    ASSERT_EQ(NodeHeader::frag_count(root.page()), 0x4567);
//    ASSERT_EQ(NodeHeader::free_start(root.page()), 0x5678);
//    ASSERT_EQ(NodeHeader::free_total(root.page()), 0x6789);
//}
//
//TEST_F(NodeTests, NonRootNodeFieldsAreDistinct)
//{
//    auto root = make_node(++identifier::root(), PageType::EXTERNAL_NODE);
//    const auto number = random.next_int(std::numeric_limits<std::uint64_t>::max() / 2,
//                                        std::numeric_limits<std::uint64_t>::max());
//
//    root.page().set_lsn(SeqNum {number * 40});
//    NodeHeader::set_parent_id(root.page(), identifier {number * 50});
//    NodeHeader::set_right_sibling_id(root.page(), identifier {number * 60});
//    NodeHeader::set_left_sibling_id(root.page(), identifier {number * 80});
//    NodeHeader::set_cell_count(root.page(), 0x2345);
//    NodeHeader::set_cell_start(root.page(), 0x3456);
//    NodeHeader::set_frag_count(root.page(), 0x4567);
//    NodeHeader::set_free_start(root.page(), 0x5678);
//    NodeHeader::set_free_total(root.page(), 0x6789);
//    CALICO_EXPECT_EQ(root.page().lsn(), SeqNum {number * 40});
//    CALICO_EXPECT_EQ(root.page().type(), PageType::EXTERNAL_NODE);
//    ASSERT_EQ(NodeHeader::parent_id(root.page()), identifier {number * 50});
//    ASSERT_EQ(NodeHeader::right_sibling_id(root.page()), identifier {number * 60});
//    ASSERT_EQ(NodeHeader::left_sibling_id(root.page()), identifier {number * 80});
//    ASSERT_EQ(NodeHeader::cell_count(root.page()), 0x2345);
//    ASSERT_EQ(NodeHeader::cell_start(root.page()), 0x3456);
//    ASSERT_EQ(NodeHeader::frag_count(root.page()), 0x4567);
//    ASSERT_EQ(NodeHeader::free_start(root.page()), 0x5678);
//    ASSERT_EQ(NodeHeader::free_total(root.page()), 0x6789);
//}
//
//TEST_F(NodeTests, FreshNodesAreEmpty)
//{
//    auto node = make_node(identifier::root(), PageType::EXTERNAL_NODE);
//    ASSERT_EQ(node.cell_count(), 0);
//}
//
//TEST_F(NodeTests, RemoveAtFromEmptyNodeDeathTest)
//{
//    auto node = make_node(identifier::root(), PageType::EXTERNAL_NODE);
//    ASSERT_DEATH(node.remove_at(0, MAX_CELL_HEADER_SIZE), BOOL_EXPECTATION_MATCHER);
//}
//
//TEST_F(NodeTests, FindInEmptyNodeFindsNothing)
//{
//    auto node = make_node(identifier::root(), PageType::EXTERNAL_NODE);
//    auto [index, found_eq] = node.find_ge(stob("hello"));
//    ASSERT_FALSE(found_eq);
//
//    // We would insert "hello" at this index.
//    ASSERT_EQ(index, 0);
//}
//
//TEST_F(NodeTests, UsableSpaceIsUpdatedOnInsert)
//{
//    auto node = make_node(identifier::root(), PageType::EXTERNAL_NODE);
//    auto cell = cell_backing.get_cell("hello", normal_value, node.is_external());
//    const auto usable_space_after = node.usable_space() - cell.size() - CELL_POINTER_SIZE;
//    node.insert(std::move(cell));
//    ASSERT_EQ(node.usable_space(), usable_space_after);
//}
//
//TEST_F(NodeTests, SanityCheck)
//{
//    static constexpr Size NUM_ITERATIONS {10};
//    Random random {internal::random_seed};
//    auto node = make_node(identifier::root(), PageType::EXTERNAL_NODE);
//    for (Index i {}; i < NUM_ITERATIONS; ++i) {
//        while (!node.is_overflowing()) {
//            const auto key = random_string(random, 2, 5);
//            const auto value = random_string(random, 0, 2 * overflow_value.size());
//            if (const auto [index, found_eq] = node.find_ge(stob(key)); !found_eq) {
//                auto cell = cell_backing.get_cell(key, value, node.is_external());
//                if (cell.local_value().size() != value.size())
//                    cell.set_overflow_id(arbitrary_pid);
//                node.insert(std::move(cell));
//            }
//        }
//        (void)node.take_overflow_cell();
//        while (node.cell_count()) {
//            const auto key = random_string(random, 2, 5);
//            const auto [index, found_eq] = node.find_ge(stob(key));
//            const auto to_remove = index - (index == node.cell_count());
//            node.remove_at(to_remove, node.read_cell(to_remove).size());
//        }
//        ASSERT_EQ(node.usable_space(), node.max_usable_space());
//    }
//}
//
//auto get_maximally_sized_external_cell(NodeTests &test)
//{
//    // The largest possible cell is one that has a key of length get_max_local(page_size) and a non-empty value. We
//    // cannot store part of the key on an overflow page in the current design, so we have an embedded payload of size
//    // get_max_local(page_size), an overflow ID, and maybe a left child ID.
//    const auto max_key_length = get_max_local(NodeTests::page_size);
//    return test.cell_backing.get_cell(test.random.next_string(max_key_length), "value", true, test.arbitrary_pid);
//}
//
//auto get_maximally_sized_internal_cell(NodeTests &test)
//{
//    // The largest possible cell is one that has a key of length get_max_local(page_size) and a non-empty value. We
//    // cannot store part of the key on an overflow page in the current design, so we have an embedded payload of size
//    // get_max_local(page_size), an overflow ID, and maybe a left child ID.
//    const auto max_key_length = get_max_local(NodeTests::page_size);
//    auto cell = test.cell_backing.get_cell(test.random.next_string(max_key_length), "", false, identifier::null());
//    cell.set_left_child_id(test.arbitrary_pid);
//    return cell;
//}
//
//auto run_maximally_sized_cell_test(NodeTests &test, identifier id, Size max_records_before_overflow, bool is_external)
//{
//    auto node = test.make_node(id, is_external ? PageType::EXTERNAL_NODE : PageType::INTERNAL_NODE);
//
//    for (Index i {}; i <= max_records_before_overflow; ++i) {
//        auto cell = is_external
//            ? get_maximally_sized_external_cell(test)
//            : get_maximally_sized_internal_cell(test);
//        ASSERT_FALSE(node.is_overflowing());
//        node.insert(std::move(cell));
//    }
//    // Overflow cell is not included in the count.
//    ASSERT_EQ(node.cell_count(), max_records_before_overflow);
//    ASSERT_TRUE(node.is_overflowing());
//}
//
//TEST_F(NodeTests, InternalRootFitsAtLeastFourRecords)
//{
//    run_maximally_sized_cell_test(*this, identifier::root(), 4, false);
//}
//
//TEST_F(NodeTests, ExternalRootFitsAtLeastThreeRecords)
//{
//    run_maximally_sized_cell_test(*this, identifier::root(), 3, true);
//}
//
//TEST_F(NodeTests, InternalNonRootFitsAtLeastFiveRecords)
//{
//    run_maximally_sized_cell_test(*this, identifier {ROOT_ID_VALUE + 1}, 5, false);
//}
//
//template<class Test>auto get_node_with_one_cell(Test &test, bool has_overflow = false)
//{
//    static identifier next_id {2ULL};
//    auto value = has_overflow ? test.overflow_value : test.normal_value;
//    auto node = test.node_backing.get_node(next_id, PageType::EXTERNAL_NODE);
//    auto cell = test.cell_backing.get_cell("hello", value, true);
//    next_id.value++;
//
//    if (has_overflow)
//        cell.set_overflow_id(test.arbitrary_pid);
//    node.insert(std::move(cell));
//    return node;
//}
//
//TEST_F(NodeTests, InsertingCellIncrementsCellCount)
//{
//    auto node = get_node_with_one_cell(*this);
//    ASSERT_EQ(node.cell_count(), 1);
//}
//
//TEST_F(NodeTests, FindExact)
//{
//    auto node = get_node_with_one_cell(*this);
//    auto [index, found_eq] = node.find_ge(stob("hello"));
//    ASSERT_TRUE(found_eq);
//    ASSERT_EQ(index, 0);
//}
//
//TEST_F(NodeTests, FindLessThan)
//{
//    auto node = get_node_with_one_cell(*this);
//    auto [index, found_eq] = node.find_ge(stob("helln"));
//    ASSERT_FALSE(found_eq);
//    ASSERT_EQ(index, 0);
//}
//
//TEST_F(NodeTests, FindGreaterThan)
//{
//    auto node = get_node_with_one_cell(*this);
//    auto [index, found_eq] = node.find_ge(stob("hellp"));
//    ASSERT_FALSE(found_eq);
//    ASSERT_EQ(index, 1);
//}
//
//TEST_F(NodeTests, ReadCell)
//{
//    auto node = get_node_with_one_cell(*this);
//    auto cell = node.read_cell(0);
//    ASSERT_EQ(cell.overflow_id(), identifier::null());
//    ASSERT_TRUE(cell.key() == stob("hello"));
//    ASSERT_TRUE(cell.local_value() == stob("world"));
//}
//
//TEST_F(NodeTests, ReadCellWithOverflow)
//{
//    auto node = get_node_with_one_cell(*this, true);
//    auto cell = node.read_cell(0);
//    ASSERT_EQ(cell.overflow_id(), arbitrary_pid);
//}
//
//TEST_F(NodeTests, InsertDuplicateKeyDeathTest)
//{
//    std::string value {"world"};
//    auto node = make_node(identifier::root(), PageType::EXTERNAL_NODE);
//    auto cell = cell_backing.get_cell("hello", value, true);
//    node.insert(std::move(cell));
//    ASSERT_DEATH(node.insert(cell_backing.get_cell("hello", value, true)), BOOL_EXPECTATION_MATCHER);
//}
//
//TEST_F(NodeTests, RemovingNonexistentCellDoesNothing)
//{
//    auto node = get_node_with_one_cell(*this);
//    ASSERT_FALSE(node.remove(stob("not_found")));
//    ASSERT_EQ(node.cell_count(), 1);
//}
//
//TEST_F(NodeTests, RemovingCellDecrementsCellCount)
//{
//    auto node = get_node_with_one_cell(*this);
//    node.remove(stob("hello"));
//    ASSERT_EQ(node.cell_count(), 0);
//}
//
//TEST_F(NodeTests, UsableSpaceIsUpdatedOnRemove)
//{
//    auto node = make_node(identifier::root(), PageType::EXTERNAL_NODE);
//    auto cell = cell_backing.get_cell("hello", normal_value, true);
//    const auto usable_space_before = node.usable_space();
//    node.insert(std::move(cell));
//    node.remove(stob("hello"));
//    ASSERT_EQ(node.usable_space(), usable_space_before);
//}
//
//TEST_F(NodeTests, SplitNonRootExternal)
//{
//    auto lhs = make_node(identifier {2ULL}, PageType::EXTERNAL_NODE);
//    auto rhs = make_node(identifier {3ULL}, PageType::EXTERNAL_NODE);
//    RollingScratchManager scratch {lhs.size()};
//
//    lhs.insert(cell_backing.get_cell(make_key(100), normal_value, true));
//    lhs.insert(cell_backing.get_cell(make_key(200), normal_value, true));
//    while (!lhs.is_overflowing())
//        lhs.insert(cell_backing.get_cell(make_key(100 + lhs.cell_count()), normal_value, true));
//    auto separator = split_non_root(lhs, rhs, scratch.get());
//
//    ASSERT_EQ(separator.left_child_id(), lhs.id());
//}

} // <anonymous>