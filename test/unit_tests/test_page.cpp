#include <gtest/gtest.h>

#include "calico/bytes.h"
#include "calico/options.h"
#include "random.h"
#include "unit_tests.h"
#include "page/cell.h"
#include "page/node.h"
#include "page/page.h"
#include "utils/expect.h"
#include "utils/layout.h"
#include "utils/scratch.h"

namespace {

using namespace calico;
using namespace calico::page;
using namespace calico::utils;

class PageBacking {
public:
    explicit PageBacking(Size page_size)
        : m_map {page_size}
        , m_scratch {page_size}
        , m_page_size {page_size} {}

    auto get_page(PID id) -> Page
    {
        m_map.emplace(id, std::string(m_page_size, '\x00'));
        return Page {{id, stob(m_map[id]), nullptr, true, false}};
    }

private:
    std::unordered_map<PID, std::string, PID::Hasher> m_map;
    ScratchManager m_scratch;
    Size m_page_size {};
};

class PageTests: public testing::Test {
public:
    static constexpr Size page_size = 0x100;

    PageTests()
        : m_backing {page_size} {}

    auto get_page(PID id) -> Page
    {
        return m_backing.get_page(id);
    }

private:
    PageBacking m_backing;
};

TEST_F(PageTests, HeaderFields)
{
    auto page = get_page(PID::root());
    page.set_type(PageType::EXTERNAL_NODE);
    ASSERT_EQ(page.type(), PageType::EXTERNAL_NODE);
}

TEST_F(PageTests, FreshPagesAreEmpty)
{
    auto page = get_page(PID::root());
    ASSERT_TRUE(page.view(0) == stob(std::string(page_size, '\x00')));
}

class CellBacking {
public:
    explicit CellBacking(Size page_size)
        : scratch {page_size}
        , page_size {page_size} {}

    auto get_cell(const std::string &key) -> Cell
    {
        Cell::Parameters param;
        param.key = stob(key);
        param.page_size = page_size;
        param.is_external = false;
        Cell cell {param};
        cell.detach(scratch.get());
        cell.set_left_child_id(PID {123});
        return cell;
    }

    auto get_cell(const std::string &key, const std::string &value, bool is_external, PID overflow_id = PID::null()) -> Cell
    {
        const auto local_value_size = get_local_value_size(key.size(), value.size(), page_size);
        Cell::Parameters param;
        param.key = stob(key);
        param.local_value = stob(value);
        param.value_size = value.size();
        param.page_size = page_size;
        param.is_external = is_external;

        if (local_value_size != value.size()) {
            CALICO_EXPECT_LT(local_value_size, value.size());
            param.local_value.truncate(local_value_size);
            param.overflow_id = overflow_id;
        }
        Cell cell {param};
        cell.detach(scratch.get());
        return cell;
    }

    ScratchManager scratch;
    Size page_size {};
};

class NodeComponentBacking: public PageBacking {
public:
    explicit NodeComponentBacking(Size page_size)
        : PageBacking {page_size}
        , backing {get_page(PID {2})}
        , header {backing}
        , directory {header}
        , allocator {header}
    {
        header.set_cell_start(backing.size());
        allocator.reset();
    }

    Page backing;
    NodeHeader header;
    CellDirectory directory;
    BlockAllocator allocator;
};

class NodeComponentTests: public testing::Test {
public:
    static constexpr Size PAGE_SIZE {0x100};
    NodeComponentTests()
        : backing {PAGE_SIZE} {}

    NodeComponentBacking backing;
};

class NodeHeaderTests: public NodeComponentTests {
public:
    auto header() -> NodeHeader&
    {
        return backing.header;
    }
};

TEST_F(NodeHeaderTests, SetChildOfExternalNodeDeathTest)
{
    backing.backing.set_type(PageType::EXTERNAL_NODE);
    ASSERT_DEATH(header().set_rightmost_child_id(PID {123}), EXPECTATION_MATCHER);
}

TEST_F(NodeHeaderTests, SetSiblingOfInternalNodeDeathTest)
{
    backing.backing.set_type(PageType::INTERNAL_NODE);
    ASSERT_DEATH(header().set_right_sibling_id(PID {123}), EXPECTATION_MATCHER);
}

TEST_F(NodeHeaderTests, FieldsAreConsistent)
{
    backing.backing.set_type(PageType::EXTERNAL_NODE);
    header().set_parent_id(PID {1});
    header().set_right_sibling_id(PID {2});
    header().set_cell_start(3);
    header().set_free_start(4);
    header().set_free_count(5);
    header().set_frag_count(6);
    header().set_cell_count(7);
    ASSERT_EQ(header().parent_id(), PID {1});
    ASSERT_EQ(header().right_sibling_id(), PID {2});
    ASSERT_EQ(header().cell_start(), 3);
    ASSERT_EQ(header().free_start(), 4);
    ASSERT_EQ(header().free_count(), 5);
    ASSERT_EQ(header().frag_count(), 6);
    ASSERT_EQ(header().cell_count(), 7);

    header().set_free_start(0); // TODO
    header().set_free_count(0); // TODO
}

class CellDirectoryTests: public NodeComponentTests {
public:
    auto directory() -> CellDirectory&
    {
        return backing.directory;
    }

    [[nodiscard]] auto cell_count() const -> Size
    {
        return backing.header.cell_count();
    }
};

TEST_F(CellDirectoryTests, FreshDirectoryIsEmpty)
{
    ASSERT_EQ(cell_count(), 0);
}

TEST_F(CellDirectoryTests, RemoveFromEmptyDirectoryDeathTest)
{
    ASSERT_DEATH(directory().remove_pointer(0), EXPECTATION_MATCHER);
}

TEST_F(CellDirectoryTests, AccessNonexistentPointerDeathTest)
{
    ASSERT_DEATH(directory().set_pointer(0, {100}), EXPECTATION_MATCHER);
    ASSERT_DEATH((void)directory().get_pointer(0), EXPECTATION_MATCHER);
}

TEST_F(CellDirectoryTests, ModifyingDirectoryChangesCellCount)
{
    directory().insert_pointer(0, {100});
    ASSERT_EQ(cell_count(), 1);
    directory().remove_pointer(0);
    ASSERT_EQ(cell_count(), 0);
}

TEST_F(CellDirectoryTests, InsertedPointersCanBeRead)
{
    directory().insert_pointer(0, {120});
    directory().insert_pointer(0, {110});
    directory().insert_pointer(0, {100});
    ASSERT_EQ(directory().get_pointer(0).value, 100);
    ASSERT_EQ(directory().get_pointer(1).value, 110);
    ASSERT_EQ(directory().get_pointer(2).value, 120);
}

class BlockAllocatorTests: public NodeComponentTests {
public:
    auto allocator() -> BlockAllocator&
    {
        return backing.allocator;
    }

    [[nodiscard]] auto free_count() const -> Size
    {
        return backing.header.free_count();
    }

    [[nodiscard]] auto free_start() const -> Index
    {
        return backing.header.free_start();
    }

    [[nodiscard]] auto frag_count() const -> Size
    {
        return backing.header.frag_count();
    }

    [[nodiscard]] auto max_usable_space() const -> Size
    {
        return backing.header.max_usable_space();
    }
};

TEST_F(BlockAllocatorTests, FreshAllocatorIsEmpty)
{
    // `free_start()` should be ignored if `free_count()` is zero.
    ASSERT_EQ(free_count(), 0);
    ASSERT_EQ(frag_count(), 0);
    ASSERT_EQ(allocator().usable_space(), max_usable_space());
}

TEST_F(BlockAllocatorTests, AllocatingBlockFromGapReducesUsableSpace)
{
    allocator().allocate(10);
    ASSERT_EQ(allocator().usable_space(), max_usable_space() - 10);
}

TEST_F(BlockAllocatorTests, FreeingBlockIncreasesUsableSpace)
{
    allocator().free(allocator().allocate(10), 10);
    ASSERT_EQ(allocator().usable_space(), max_usable_space());
}

TEST_F(BlockAllocatorTests, FreedMemoryIsMaintained)
{
    const auto a = allocator().allocate(10);
    const auto b = allocator().allocate(10);
    const auto c = allocator().allocate(3);
    ASSERT_EQ(allocator().usable_space(), max_usable_space() - 23);

    allocator().free(a, 10);
    ASSERT_EQ(free_count(), 1);
    ASSERT_EQ(free_start(), a);

    allocator().free(b, 10);
    ASSERT_EQ(free_count(), 2);
    ASSERT_EQ(free_start(), b);

    allocator().free(c, 3);
    ASSERT_EQ(free_count(), 2);
    ASSERT_EQ(free_start(), b);
    ASSERT_EQ(frag_count(), 3);
    ASSERT_EQ(allocator().usable_space(), max_usable_space());
}

TEST_F(BlockAllocatorTests, SanityCheck)
{
    static constexpr Size NUM_ITERATIONS {10};
    static constexpr Size MIN_SIZE {MIN_CELL_HEADER_SIZE + 1};
    static constexpr Size MAX_SIZE {MIN_SIZE * 3};
    std::unordered_map<Index, Size> allocations;
    auto usable_space = max_usable_space();
    Random random {0};

    for (Index i {}; i < NUM_ITERATIONS; ++i) {
        for (; ; ) {
            const auto size = random.next_int(MIN_SIZE, MAX_SIZE);
            if (const auto ptr = allocator().allocate(size)) {
                auto itr = allocations.find(ptr);
                ASSERT_EQ(itr, allocations.end());
                allocations.emplace(ptr, size);
                usable_space -= size;
                ASSERT_EQ(allocator().usable_space(), usable_space);
            } else {
                break;
            }
        }
        for (const auto &[index, size]: allocations) {
            allocator().free(index, size);
            usable_space += size;
            ASSERT_EQ(allocator().usable_space(), usable_space);
        }
        allocations.clear();
    }
}

class FreeBlockTests: public BlockAllocatorTests {
public:
    static constexpr Size NUM_BLOCKS {3};
    static constexpr Size BLOCK_TOTAL {28};
    static constexpr Size BLOCK_SIZES[] {16, 8, 4};

    FreeBlockTests()
    {
        // free_start() -> c -> b -> a -> NULL
        const auto a = allocator().allocate(BLOCK_SIZES[0]);
        const auto b = allocator().allocate(BLOCK_SIZES[1]);
        const auto c = allocator().allocate(BLOCK_SIZES[2]);
        EXPECT_EQ(allocator().usable_space(), max_usable_space() - BLOCK_TOTAL);
        allocator().free(a, BLOCK_SIZES[0]);
        allocator().free(b, BLOCK_SIZES[1]);
        allocator().free(c, BLOCK_SIZES[2]);
        EXPECT_EQ(free_count(), NUM_BLOCKS);
        EXPECT_EQ(free_start(), c);
        EXPECT_EQ(allocator().usable_space(), max_usable_space());
    }
};

TEST_F(FreeBlockTests, TakeWholeBlock)
{
    // Take all of block `a`.
    allocator().allocate(BLOCK_SIZES[0]);
    ASSERT_EQ(free_count(), NUM_BLOCKS - 1);
    ASSERT_EQ(allocator().usable_space(), max_usable_space() - BLOCK_SIZES[0]);
}

TEST_F(FreeBlockTests, TakePartialBlock)
{
    // Take 3/4 of block `a`, which remains a free block.
    allocator().allocate(3 * BLOCK_SIZES[0] / 4);
    ASSERT_EQ(free_count(), NUM_BLOCKS);
    ASSERT_EQ(allocator().usable_space(), max_usable_space() - 3*BLOCK_SIZES[0]/4);
}

TEST_F(FreeBlockTests, ReduceBlockToFragments)
{
    // Take most of block `a`, which becomes 3 fragment bytes.
    allocator().allocate(BLOCK_SIZES[0]  - 3);
    ASSERT_EQ(free_count(), NUM_BLOCKS - 1);
    ASSERT_EQ(frag_count(), 3);
    ASSERT_EQ(allocator().usable_space(), max_usable_space() - BLOCK_SIZES[0] + 3);
}

class NodeBacking: public PageBacking {
public:
    explicit NodeBacking(Size page_size)
        : PageBacking {page_size} {}

    auto get_node(PID id, PageType type) -> Node
    {
        Node node {get_page(id), true};
        node.page().set_type(type);
        return node;
    }
};

class NodeTests: public PageTests {
public:
    NodeTests()
        : node_backing {page_size}
        , cell_backing {page_size}
        , overflow_value(page_size, 'x') {}

    auto make_node(PID id, PageType type)
    {
        return node_backing.get_node(id, type);
    }

    Random random {0};
    NodeBacking node_backing;
    CellBacking cell_backing;
    std::string normal_value {"world"};
    std::string overflow_value;
    PID arbitrary_pid {2};
};

TEST_F(NodeTests, FreshNodesAreEmpty)
{
    auto node = make_node(PID::root(), PageType::EXTERNAL_NODE);
    ASSERT_EQ(node.cell_count(), 0);
}

TEST_F(NodeTests, RemoveAtFromEmptyNodeDeathTest)
{
    auto node = make_node(PID::root(), PageType::EXTERNAL_NODE);
    ASSERT_DEATH(node.remove_at(0, MAX_CELL_HEADER_SIZE), EXPECTATION_MATCHER);
}

TEST_F(NodeTests, FindInEmptyNodeFindsNothing)
{
    auto node = make_node(PID::root(), PageType::EXTERNAL_NODE);
    auto [index, found_eq] = node.find_ge(stob("hello"));
    ASSERT_FALSE(found_eq);

    // We would insert "hello" at this index.
    ASSERT_EQ(index, 0);
}

TEST_F(NodeTests, UsableSpaceIsUpdatedOnInsert)
{
    auto node = make_node(PID::root(), PageType::EXTERNAL_NODE);
    auto cell = cell_backing.get_cell("hello", normal_value, node.is_external());
    const auto usable_space_after = node.usable_space() - cell.size() - CELL_POINTER_SIZE;
    node.insert(std::move(cell));
    ASSERT_EQ(node.usable_space(), usable_space_after);
}

TEST_F(NodeTests, SanityCheck)
{
    static constexpr Size NUM_ITERATIONS {10};
    Random random {0};
    auto node = make_node(PID::root(), PageType::EXTERNAL_NODE);
    for (Index i {}; i < NUM_ITERATIONS; ++i) {
        while (!node.is_overflowing()) {
            const auto key = random_string(random, 2, 5);
            const auto value = random_string(random, 0, 2 * overflow_value.size());
            if (const auto [index, found_eq] = node.find_ge(stob(key)); !found_eq) {
                auto cell = cell_backing.get_cell(key, value, node.is_external());
                if (cell.local_value().size() != value.size())
                    cell.set_overflow_id(arbitrary_pid);
                node.insert(std::move(cell));
            }
        }
        (void)node.take_overflow_cell();
        while (node.cell_count()) {
            const auto key = random_string(random, 2, 5);
            const auto [index, found_eq] = node.find_ge(stob(key));
            const auto to_remove = index - (index == node.cell_count());
            node.remove_at(to_remove, node.read_cell(to_remove).size());
        }
        ASSERT_EQ(node.usable_space(), node.max_usable_space());
    }
}

auto get_maximally_sized_external_cell(NodeTests &test)
{
    // The largest possible cell is one that has a key of length get_max_local(page_size) and a non-empty value. We
    // cannot store part of the key on an overflow page in the current design, so we have an embedded payload of size
    // get_max_local(page_size), an overflow ID, and maybe a left child ID.
    const auto max_key_length = get_max_local(NodeTests::page_size);
    return test.cell_backing.get_cell(test.random.next_string(max_key_length), "value", true, test.arbitrary_pid);
}

auto get_maximally_sized_internal_cell(NodeTests &test)
{
    // The largest possible cell is one that has a key of length get_max_local(page_size) and a non-empty value. We
    // cannot store part of the key on an overflow page in the current design, so we have an embedded payload of size
    // get_max_local(page_size), an overflow ID, and maybe a left child ID.
    const auto max_key_length = get_max_local(NodeTests::page_size);
    auto cell = test.cell_backing.get_cell(test.random.next_string(max_key_length), "", false, PID::null());
    cell.set_left_child_id(test.arbitrary_pid);
    return cell;
}

auto run_maximally_sized_cell_test(NodeTests &test, PID id, Size max_records_before_overflow, bool is_external)
{
    auto node = test.make_node(id, is_external ? PageType::EXTERNAL_NODE : PageType::INTERNAL_NODE);

    for (Index i {}; i <= max_records_before_overflow; ++i) {
        auto cell = is_external
            ? get_maximally_sized_external_cell(test)
            : get_maximally_sized_internal_cell(test);
        ASSERT_FALSE(node.is_overflowing());
        node.insert(std::move(cell));
    }
    // Overflow cell is not included in the count.
    ASSERT_EQ(node.cell_count(), max_records_before_overflow);
    ASSERT_TRUE(node.is_overflowing());
}

TEST_F(NodeTests, InternalRootFitsAtLeastThreeRecords)
{
    run_maximally_sized_cell_test(*this, PID::root(), 3, false);
}

TEST_F(NodeTests, ExternalRootFitsAtLeastThreeRecords)
{
    run_maximally_sized_cell_test(*this, PID::root(), 3, true);
}

TEST_F(NodeTests, InternalNonRootFitsAtLeastFourRecords)
{
    run_maximally_sized_cell_test(*this, PID {ROOT_ID_VALUE + 1}, 4, false);
}

template<class Test>auto get_node_with_one_cell(Test &test, bool has_overflow = false)
{
    static PID next_id {2};
    auto value = has_overflow ? test.overflow_value : test.normal_value;
    auto node = test.node_backing.get_node(next_id, PageType::EXTERNAL_NODE);
    auto cell = test.cell_backing.get_cell("hello", value, true);
    next_id.value++;

    if (has_overflow)
        cell.set_overflow_id(test.arbitrary_pid);
    node.insert(std::move(cell));
    return node;
}

TEST_F(NodeTests, InsertingCellIncrementsCellCount)
{
    auto node = get_node_with_one_cell(*this);
    ASSERT_EQ(node.cell_count(), 1);
}

TEST_F(NodeTests, FindExact)
{
    auto node = get_node_with_one_cell(*this);
    auto [index, found_eq] = node.find_ge(stob("hello"));
    ASSERT_TRUE(found_eq);
    ASSERT_EQ(index, 0);
}

TEST_F(NodeTests, FindLessThan)
{
    auto node = get_node_with_one_cell(*this);
    auto [index, found_eq] = node.find_ge(stob("helln"));
    ASSERT_FALSE(found_eq);
    ASSERT_EQ(index, 0);
}

TEST_F(NodeTests, FindGreaterThan)
{
    auto node = get_node_with_one_cell(*this);
    auto [index, found_eq] = node.find_ge(stob("hellp"));
    ASSERT_FALSE(found_eq);
    ASSERT_EQ(index, 1);
}

TEST_F(NodeTests, ReadCell)
{
    auto node = get_node_with_one_cell(*this);
    auto cell = node.read_cell(0);
    ASSERT_EQ(cell.overflow_id(), PID::null());
    ASSERT_TRUE(cell.key() == stob("hello"));
    ASSERT_TRUE(cell.local_value() == stob("world"));
}

TEST_F(NodeTests, ReadCellWithOverflow)
{
    auto node = get_node_with_one_cell(*this, true);
    auto cell = node.read_cell(0);
    ASSERT_EQ(cell.overflow_id(), arbitrary_pid);
}

TEST_F(NodeTests, InsertDuplicateKeyDeathTest)
{
    std::string value {"world"};
    auto node = make_node(PID::root(), PageType::EXTERNAL_NODE);
    auto cell = cell_backing.get_cell("hello", value, true);
    node.insert(std::move(cell));
    ASSERT_DEATH(node.insert(cell_backing.get_cell("hello", value, true)), EXPECTATION_MATCHER);
}

TEST_F(NodeTests, RemovingNonexistentCellDoesNothing)
{
    auto node = get_node_with_one_cell(*this);
    ASSERT_FALSE(node.remove(stob("not_found")));
    ASSERT_EQ(node.cell_count(), 1);
}

TEST_F(NodeTests, RemovingCellDecrementsCellCount)
{
    auto node = get_node_with_one_cell(*this);
    node.remove(stob("hello"));
    ASSERT_EQ(node.cell_count(), 0);
}

TEST_F(NodeTests, A)
{
    auto node = make_node(PID::root(), PageType::INTERNAL_NODE);
    const std::vector<std::string> keys {"Vn98oi", "VtZPREUPcKsuw", "W4PH8S", "WAA", "WE3wrByTqG", "WGPeRx7qo", "WMXX2VmvTlAi","WYwDilYMS", "byFKBvxMMndEY"};
    for (const auto &key: keys)
        node.insert(cell_backing.get_cell(key, "", false));
    auto [index, found_eq] = node.find_ge(stob("WamfVthwD2"));
    printf("%lu, %d\n", index, found_eq);
//    for (Index i {}; i < keys.size(); ++i)
//        printf("%s\n", btos(node.read_key(i)).c_str());
}

TEST_F(NodeTests, UsableSpaceIsUpdatedOnRemove)
{
    auto node = make_node(PID::root(), PageType::EXTERNAL_NODE);
    auto cell = cell_backing.get_cell("hello", normal_value, true);
    const auto usable_space_before = node.usable_space();
    node.insert(std::move(cell));
    node.remove(stob("hello"));
    ASSERT_EQ(node.usable_space(), usable_space_before);
}

} // <anonymous>