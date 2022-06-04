#include <gtest/gtest.h>

#include "bytes.h"
#include "common.h"
#include "random.h"
#include "unit_tests.h"
#include "page/cell.h"
#include "page/file_header.h"
#include "page/node.h"
#include "page/page.h"
#include "page/update.h"
#include "utils/assert.h"
#include "utils/scratch.h"

namespace {

using namespace cub;
using Range = UpdateManager::Range;

[[maybe_unused]] auto show_ranges(const std::vector<Range> &ranges)
{
    static constexpr Size step = 3;
    std::vector<std::string> display;
    Index max_index {};
    for (const auto &[x, dx]: ranges)
        max_index = std::max(max_index, x + dx);

    for (const auto &[x, dx]: ranges) {
        std::string line;
        const auto y = x + dx;
        for (Index i {}; i <= max_index; ++i) {
            if (i >= x && i < y) {
                line += std::string(step, '=');
            } else if (i == y) {
                line += '=' + std::string(step - 1, ' ');
            } else {
                line += '.' + std::string(step - 1, ' ');
            }
        }
        display.emplace_back(line);
    }

    std::ostringstream oss;
    for (Index i {}; i <= max_index; ++i)
        oss << std::left << std::setw(3) << i;
    oss << '\n';

    auto output = oss.str();
    for (const auto &line: display)
        output += line + '\n';
    return output;
}

TEST(UpdateTests, BasicAssertions)
{
    test::update_basic_assertions();
}

class UpdateManagerTests: public testing::Test {
private:
    ScratchManager m_scratch {page_size};

public:
    static constexpr Size page_size = 0x400;

    UpdateManagerTests()
        : manager {m_scratch.get()}
        , scratch {m_scratch.get()}
        , snapshot {scratch.data()} {}

    UpdateManager manager;
    Scratch scratch;
    BytesView snapshot;
};

TEST_F(UpdateManagerTests, FreshUpdateManagerHasNoChanges)
{
    ASSERT_FALSE(manager.has_changes());
}

TEST_F(UpdateManagerTests, RegistersChange)
{
    manager.indicate_change(1, 2);
    ASSERT_TRUE(manager.has_changes());
    const auto changes = manager.collect_changes(snapshot);
    const auto change = changes.at(0);
    ASSERT_EQ(change.offset, 1);
    ASSERT_EQ(change.before.size(), 2);
    ASSERT_EQ(change.after.size(), 2);
}

TEST_F(UpdateManagerTests, DoesNotMergeDisjointChanges)
{
    manager.indicate_change(0, 1);
    manager.indicate_change(2, 1);
    manager.indicate_change(4, 1);
    const auto changes = manager.collect_changes(snapshot);
    ASSERT_EQ(changes.size(), 3);
}

TEST_F(UpdateManagerTests, MergesIntersectingChanges)
{
    manager.indicate_change(0, 1);
    manager.indicate_change(1, 1);
    manager.indicate_change(2, 1);
    const auto changes = manager.collect_changes(snapshot);
    ASSERT_EQ(changes.size(), 1);
}

class PageBacking {
public:
    explicit PageBacking(Size page_size)
        : m_map {page_size}
        , m_scratch {page_size}
        , m_page_size {page_size} {}

    auto get_page(PID id) -> Page
    {
        m_map.emplace(id, std::string(m_page_size, '\x00'));
        Page page {{id, _b(m_map[id]), nullptr, true, false}};
        page.enable_tracking(m_scratch.get());
        return page;
    }

private:
    std::unordered_map<PID, std::string, PID::Hasher> m_map;
    ScratchManager m_scratch;
    Size m_page_size {};
};

class PageTests: public testing::Test {
public:
    static constexpr Size page_size = 0x100;
    static constexpr Size halfway_point = page_size / 2;

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
    page.set_lsn(LSN {123});
    ASSERT_EQ(page.type(), PageType::EXTERNAL_NODE);
    ASSERT_EQ(page.lsn(), LSN {123});
}

TEST_F(PageTests, FreshPagesAreEmpty)
{
    auto page = get_page(PID::root());
    ASSERT_FALSE(page.has_changes());
    ASSERT_TRUE(page.range(0) == _b(std::string(page_size, '\x00')));
}

TEST_F(PageTests, RegistersHeaderChange)
{
    auto page = get_page(PID::root());
    ASSERT_EQ(page.type(), PageType::NULL_PAGE);
    page.set_type(PageType::EXTERNAL_NODE);
    ASSERT_EQ(page.type(), PageType::EXTERNAL_NODE);
    ASSERT_TRUE(page.has_changes());
}

TEST_F(PageTests, RegistersContentChange)
{
    auto page = get_page(PID::root());
    ASSERT_EQ(page.get_u32(halfway_point), 0);
    page.put_u32(halfway_point, 42);
    ASSERT_EQ(page.get_u32(halfway_point), 42);
    ASSERT_TRUE(page.has_changes());
}

auto perform_basic_changes_and_collect(Page &page)
{
    page.set_type(PageType::EXTERNAL_NODE);
    page.put_u32(PageTests::halfway_point, 42);
    return page.collect_changes();
}

TEST_F(PageTests, UndoChanges)
{
    auto page = get_page(PID::root());
    auto changes = perform_basic_changes_and_collect(page);
    page.set_lsn(LSN::base());
    page.undo_changes(LSN::null(), changes);
    ASSERT_EQ(page.lsn(), LSN::null()) << "Page LSN should have been updated";
    ASSERT_EQ(page.type(), PageType::NULL_PAGE);
    ASSERT_EQ(page.get_u32(halfway_point), 0);
}

TEST_F(PageTests, RedoChanges)
{
    auto temp = get_page(PID::root());
    auto changes = perform_basic_changes_and_collect(temp);
    auto page = get_page(PID::root());
    page.redo_changes(LSN::base(), changes);
    ASSERT_EQ(page.lsn(), LSN::base()) << "Page LSN should have been updated";
    ASSERT_EQ(page.type(), PageType::EXTERNAL_NODE);
    ASSERT_EQ(page.get_u32(halfway_point), 42);
}

class CellBacking {
public:
    explicit CellBacking(Size page_size)
        : scratch {page_size}
        , page_size {page_size} {}

    auto get_cell(const std::string &key, const std::string &value, PID overflow_id = PID::null()) -> Cell
    {
        const auto local_value_size = get_local_value_size(key.size(), value.size(), page_size);
        Cell::Parameters param;
        param.key = _b(key);
        param.local_value = _b(value);
        param.value_size = value.size();

        if (local_value_size != value.size()) {
            CUB_EXPECT_LT(local_value_size, value.size());
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

TEST_F(NodeTests, NodeHeaderFields)
{
    auto node = make_node(PID {2}, PageType::EXTERNAL_NODE);
    node.page().set_lsn(LSN::base());
    node.set_parent_id(PID {123});
    node.set_right_sibling_id(PID {456});
    node.update_header_crc();
    ASSERT_EQ(node.page().type(), PageType::EXTERNAL_NODE);
    ASSERT_EQ(node.page().lsn(), LSN::base());
    ASSERT_EQ(node.parent_id(), PID {123});
    ASSERT_EQ(node.right_sibling_id(), PID {456});
}

TEST_F(NodeTests, FileHeaderFields)
{
    auto node = make_node(PID::root(), PageType::EXTERNAL_NODE);
    FileHeader header {node};
    header.update_magic_code();
    header.set_page_count(1);
    header.set_node_count(2);
    header.set_free_count(3);
    header.set_free_start(PID {123});
    header.set_page_size(4);
    header.set_block_size(5);
    header.set_key_count(6);
    header.set_flushed_lsn(LSN {456});
    header.update_header_crc();
    ASSERT_EQ(header.page_count(), 1);
    ASSERT_EQ(header.node_count(), 2);
    ASSERT_EQ(header.free_count(), 3);
    ASSERT_EQ(header.free_start(), PID {123});
    ASSERT_EQ(header.page_size(), 4);
    ASSERT_EQ(header.block_size(), 5);
    ASSERT_EQ(header.key_count(), 6);
    ASSERT_EQ(header.flushed_lsn(), LSN {456});
}

TEST_F(NodeTests, NodeAllocationCausesPageChanges)
{
    auto node = make_node(PID::root(), PageType::EXTERNAL_NODE);
    ASSERT_TRUE(node.page().has_changes());
}

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
    auto [index, found_eq] = node.find_ge(_b("hello"));
    ASSERT_FALSE(found_eq);

    // We would insert "hello" at this index.
    ASSERT_EQ(index, 0);
}

TEST_F(NodeTests, UsableSpaceIsUpdatedOnInsert)
{
    auto node = make_node(PID::root(), PageType::EXTERNAL_NODE);
    auto cell = cell_backing.get_cell("hello", normal_value);
    const auto usable_space_after = node.usable_space() - cell.size() - CELL_POINTER_SIZE;
    node.insert(std::move(cell));
    ASSERT_EQ(node.usable_space(), usable_space_after);
}

auto get_maximally_sized_cell(NodeTests &test)
{
    // The largest possible cell is one that has a key of length get_max_local(page_size), a non-empty value, and
    // resides in an internal node. We cannot store part of the key on an overflow page in the current design,
    // so we have an embedded payload of size get_max_local(page_size), an overflow ID, and a left child ID.
    const auto max_key_length = get_max_local(NodeTests::page_size);

    auto cell = test.cell_backing.get_cell(test.random.next_string(max_key_length), "value", test.arbitrary_pid);
    cell.set_left_child_id(test.arbitrary_pid);
    return cell;
}

auto run_maximally_sized_cell_test(NodeTests &test, PID id, Size max_records_before_overflow)
{
    Random random {0};
    auto node = test.make_node(id, PageType::INTERNAL_NODE);

    for (Index i {}; i <= max_records_before_overflow; ++i) {
        auto cell = get_maximally_sized_cell(test);
        ASSERT_FALSE(node.is_overflowing());
        node.insert(std::move(cell));
    }
    ASSERT_TRUE(node.is_overflowing());
}

TEST_F(NodeTests, RootFitsAtLeastThreeRecords)
{
    run_maximally_sized_cell_test(*this, PID::root(), 3);
}

TEST_F(NodeTests, NonRootFitsAtLeastFourRecords)
{
    run_maximally_sized_cell_test(*this, PID {ROOT_ID_VALUE + 1}, 4);
}

template<class Test>auto get_node_with_one_cell(Test &test, bool has_overflow = false)
{
    static PID next_id {2};
    auto value = has_overflow ? test.overflow_value : test.normal_value;
    auto node = test.node_backing.get_node(next_id, PageType::INTERNAL_NODE);
    auto cell = test.cell_backing.get_cell("hello", value);
    next_id.value++;

    if (has_overflow)
        cell.set_overflow_id(test.arbitrary_pid);
    cell.set_left_child_id(test.arbitrary_pid);

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
    auto [index, found_eq] = node.find_ge(_b("hello"));
    ASSERT_TRUE(found_eq);
    ASSERT_EQ(index, 0);
}

TEST_F(NodeTests, FindLessThan)
{
    auto node = get_node_with_one_cell(*this);
    auto [index, found_eq] = node.find_ge(_b("helln"));
    ASSERT_FALSE(found_eq);
    ASSERT_EQ(index, 0);
}

TEST_F(NodeTests, FindGreaterThan)
{
    auto node = get_node_with_one_cell(*this);
    auto [index, found_eq] = node.find_ge(_b("hellp"));
    ASSERT_FALSE(found_eq);
    ASSERT_EQ(index, 1);
}

TEST_F(NodeTests, ReadCell)
{
    auto node = get_node_with_one_cell(*this);
    auto cell = node.read_cell(0);
    ASSERT_EQ(cell.left_child_id(), arbitrary_pid);
    ASSERT_EQ(cell.overflow_id(), PID::null());
    ASSERT_TRUE(cell.key() == _b("hello"));
    ASSERT_TRUE(cell.local_value() == _b("world"));
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
    auto cell = cell_backing.get_cell("hello", value);
    node.insert(cell_backing.get_cell("hello", value));
    ASSERT_DEATH(node.insert(cell_backing.get_cell("hello", value)), EXPECTATION_MATCHER);
}

TEST_F(NodeTests, RemovingNonexistentCellDoesNothing)
{
    auto node = get_node_with_one_cell(*this);
    ASSERT_FALSE(node.remove(_b("not_found")));
    ASSERT_EQ(node.cell_count(), 1);
}

TEST_F(NodeTests, RemovingCellDecrementsCellCount)
{
    auto node = get_node_with_one_cell(*this);
    node.remove(_b("hello"));
    ASSERT_EQ(node.cell_count(), 0);
}

TEST_F(NodeTests, UsableSpaceIsUpdatedOnRemove)
{
    auto node = make_node(PID::root(), PageType::EXTERNAL_NODE);
    auto cell = cell_backing.get_cell("hello", normal_value);
    const auto usable_space_before = node.usable_space();
    node.insert(std::move(cell));
    node.remove(_b("hello"));
    ASSERT_EQ(node.usable_space(), usable_space_before);
}

class NodesCanMergeTests
    : public testing::TestWithParam<std::tuple<PageType, Size>>
{
public:
    NodesCanMergeTests()
        : page_type {std::get<0>(GetParam())}
        , page_size {std::get<1>(GetParam())}
        , scratch {page_size}
        , overflow_value(page_size, 'x')
        , node_backing {page_size}
        , cell_backing {page_size}
        , max_value_size {get_max_local(page_size) - 1}
        , lhs {make_node(PID {2}, page_type)}
        , rhs {make_node(PID {3}, page_type)} {}

    auto make_node(PID id, PageType type) -> Node
    {
        auto node = node_backing.get_node(id, type);
        // Nodes get their rightmost child ID during the splitting procedure, so we have to fake it here.
        if (page_type == PageType::INTERNAL_NODE)
            node.set_rightmost_child_id(arbitrary_pid);
        return node;
    }

    auto make_cell(const std::string &key, const std::string &value, PID overflow_id = PID::root())
    {
        auto cell = cell_backing.get_cell(key, value, overflow_id);
        if (page_type == PageType::INTERNAL_NODE)
            cell.set_left_child_id(arbitrary_pid);
        return cell;
    }
    
    Random random {0};
    PageType page_type;
    Size page_size;
    PID arbitrary_pid {123};
    ScratchManager scratch;
    std::string overflow_value;
    std::string normal_value {"value"};
    NodeBacking node_backing;
    CellBacking cell_backing;
    Size max_value_size {};
    Node lhs;
    Node rhs;
};

TEST_P(NodesCanMergeTests, EmptyNodeIsUnderflowing)
{
    ASSERT_TRUE(lhs.is_underflowing_());
}

TEST_P(NodesCanMergeTests, NodeWithTwoMaximalCellsIsUnderflowing)
{
    lhs.insert(make_cell("a", random.next_string(max_value_size)));
    lhs.insert(make_cell("b", random.next_string(max_value_size)));
    ASSERT_TRUE(lhs.is_underflowing_());
}

TEST_P(NodesCanMergeTests, NodeWithMoreThanTwoMaximalCellsIsNotUnderflowing)
{
    lhs.insert(make_cell("a", random.next_string(max_value_size)));
    lhs.insert(make_cell("b", random.next_string(max_value_size)));
    lhs.insert(make_cell("c", random.next_string(10)));
    ASSERT_FALSE(lhs.is_underflowing_());
}

TEST_P(NodesCanMergeTests, MostlyEmptyNodesCanMerge)
{
    lhs.insert(make_cell("a", random.next_string(max_value_size)));
    lhs.insert(make_cell("b", random.next_string(max_value_size)));
    rhs.insert(make_cell("c", random.next_string(max_value_size)));
    const auto cell = make_cell("d", random.next_string(max_value_size));
    ASSERT_TRUE(can_merge_siblings(lhs, rhs, cell));
}

TEST_P(NodesCanMergeTests, MostlyFullNodesCannotMerge)
{
    lhs.insert(make_cell("a", random.next_string(max_value_size)));
    lhs.insert(make_cell("b", random.next_string(max_value_size)));
    rhs.insert(make_cell("c", random.next_string(max_value_size)));
    rhs.insert(make_cell("d", random.next_string(max_value_size)));
    const auto cell = make_cell("e", random.next_string(max_value_size));
    ASSERT_FALSE(can_merge_siblings(lhs, rhs, cell));
}

template<class Test> auto make_barely_underflowing_node(Test &test, Node &node, char base_key)
{
    static constexpr Size room_for_third_cell {10};
    const auto underflow_limit = node.usable_space() / 2;
    ASSERT_EQ(underflow_limit, node.max_usable_space() / 2);
    node.insert(test.make_cell(std::string(1, base_key++), test.random.next_string(test.max_value_size)));
    node.insert(test.make_cell(std::string(1, base_key++), test.random.next_string(test.max_value_size - room_for_third_cell)));
    ASSERT_GT(node.usable_space(), underflow_limit);
    const auto diff = node.usable_space() - underflow_limit;
    ASSERT_GT(diff, CELL_POINTER_SIZE + MAX_CELL_HEADER_SIZE + 1);
    const auto adjust = !node.is_external() * PAGE_ID_SIZE;
    static constexpr Size key_size {1};
    const auto value_size = diff - CELL_POINTER_SIZE - MIN_CELL_HEADER_SIZE - key_size - adjust;
    auto cell = test.make_cell(std::string(1, base_key++), test.random.next_string(value_size));
    ASSERT_EQ(cell.size() + CELL_POINTER_SIZE, diff);
    node.insert(std::move(cell));
    ASSERT_EQ(node.usable_space(), underflow_limit);
    ASSERT_TRUE(node.is_underflowing_());
}

TEST_P(NodesCanMergeTests, BarelyUnderflowingNodesCanMerge)
{
    make_barely_underflowing_node(*this, lhs, '1');
    make_barely_underflowing_node(*this, rhs, '5');
    auto separator = lhs.extract_cell(lhs.cell_count() - 1, scratch.get());
    separator.set_left_child_id(arbitrary_pid);
    ASSERT_TRUE(can_merge_siblings(lhs, rhs, separator));
}

TEST_P(NodesCanMergeTests, BarelyNotUnderflowingNodesCannotMerge)
{
    make_barely_underflowing_node(*this, lhs, '1');
    make_barely_underflowing_node(*this, rhs, '5');
    auto last = lhs.extract_cell(lhs.cell_count() - 1, scratch.get());
    // Add one to the cell size.
    auto separator = make_cell(_s(last.key()), _s(last.local_value()) + "!");
    separator.set_left_child_id(arbitrary_pid);
    ASSERT_EQ(separator.size(), last.size() + 1 + PAGE_ID_SIZE*lhs.is_external());
    ASSERT_FALSE(can_merge_siblings(lhs, rhs, separator));
}

TEST_P(NodesCanMergeTests, NodeCanFitFourMaximallySizedCells)
{
    lhs.insert(make_cell("a", random.next_string(max_value_size)));
    lhs.insert(make_cell("b", random.next_string(max_value_size)));
    lhs.insert(make_cell("c", random.next_string(max_value_size)));
    auto cell = make_cell("d", random.next_string(max_value_size));
    lhs.insert(std::move(cell));
    ASSERT_FALSE(lhs.is_overflowing());
}

TEST_P(NodesCanMergeTests, CanMergeDifferentlyTypedNodesDeathTest)
{
    std::string value {"v"};
    rhs.page().set_type(rhs.is_external() ? PageType::INTERNAL_NODE : PageType::EXTERNAL_NODE);
    const auto cell = make_cell("k", value);
    ASSERT_DEATH(can_merge_siblings(lhs, rhs, cell), EXPECTATION_MATCHER);
}

TEST_P(NodesCanMergeTests, CanMergeOverflowingNodesDeathTest)
{
    const auto cell = make_cell("a", "1");
    lhs.set_overflow_cell(make_cell("b", "2"));
    ASSERT_DEATH(can_merge_siblings(lhs, rhs, cell), EXPECTATION_MATCHER);
}

TEST_P(NodesCanMergeTests, NodeShouldFitFourMaximallySizedCells)
{
    const auto max_occupied_by_cell = MAX_CELL_HEADER_SIZE + get_max_local(page_size) + CELL_POINTER_SIZE;
    ASSERT_LT(max_occupied_by_cell * 4, page_size);
}

const auto external_node_parameter_combinations = testing::Values(
    std::tuple<PageType, Size> {PageType::EXTERNAL_NODE, 0x100},
    std::tuple<PageType, Size> {PageType::EXTERNAL_NODE, 0x1000},
    std::tuple<PageType, Size> {PageType::EXTERNAL_NODE, 0x8000});

const auto internal_node_parameter_combinations = testing::Values(
    std::tuple<PageType, Size> {PageType::INTERNAL_NODE, 0x100},
    std::tuple<PageType, Size> {PageType::INTERNAL_NODE, 0x1000},
    std::tuple<PageType, Size> {PageType::INTERNAL_NODE, 0x8000});

INSTANTIATE_TEST_SUITE_P(
    ExternalNodesCanMerge,
    NodesCanMergeTests,
    external_node_parameter_combinations
);

INSTANTIATE_TEST_SUITE_P(
    InternalNodesCanMerge,
    NodesCanMergeTests,
    internal_node_parameter_combinations
);

class NodeMergeTests: public NodesCanMergeTests {
public:
    NodeMergeTests()
        : parent {make_node(PID::root(), PageType::INTERNAL_NODE)}
    {
        while (values.size() < 5)
            values.emplace_back(random.next_string(max_value_size / 2));

        auto cell = make_cell("c", values.at(2));
        parent.set_rightmost_child_id(rhs.id());
        cell.set_left_child_id(lhs.id());
        parent.insert(std::move(cell));

        Index i {};
        for (const auto c: std::string {"abde"}) {
            // Skip index 2, which will belong to the separator.
            i += i == 2;
            auto child_cell = make_cell(std::string(1, c), values.at(i));
            if (!lhs.is_external())
                child_cell.set_left_child_id(PID {(i+1) * 10});
            if (i < 2) {
                lhs.insert(std::move(child_cell));
            } else {
                rhs.insert(std::move(child_cell));
            }
            ++i;
        }
    }

    ~NodeMergeTests() override = default;

    auto check_merged_node(const Node &node)
    {
        Index i {};
        EXPECT_EQ(node.cell_count(), values.size());
        for (const auto c: std::string {"abcde"}) {
            EXPECT_EQ(_s(node.read_key(i)), std::string(1, c));
            EXPECT_EQ(_s(node.read_cell(i).local_value()), values.at(i));
            ++i;
        }
    }

    std::vector<std::string> values;
    Node parent;
};

class NodeMergeLeftTests: public NodeMergeTests {
public:
    NodeMergeLeftTests()
    {
        merge_left(lhs, rhs, parent, 0);
    }
    ~NodeMergeLeftTests() override = default;
};

TEST_P(NodeMergeLeftTests, MergeLeft)
{
    ASSERT_EQ(parent.child_id(0), lhs.id());
    ASSERT_EQ(parent.rightmost_child_id(), lhs.id());
    ASSERT_EQ(parent.cell_count(), 0);

    ASSERT_EQ(lhs.cell_count(), 5);
    check_merged_node(lhs);

    if (!lhs.is_external()) {
        ASSERT_EQ(lhs.child_id(0), PID {10});
        ASSERT_EQ(lhs.child_id(1), PID {20});
        ASSERT_EQ(lhs.child_id(2), arbitrary_pid);
        ASSERT_EQ(lhs.child_id(3), PID {40});
        ASSERT_EQ(lhs.child_id(4), PID {50});
        ASSERT_EQ(lhs.child_id(5), arbitrary_pid);
    }
}

INSTANTIATE_TEST_SUITE_P(
    ExternalMergeLeft,
    NodeMergeLeftTests,
    external_node_parameter_combinations
);

INSTANTIATE_TEST_SUITE_P(
    InternalMergeLeft,
    NodeMergeLeftTests,
    internal_node_parameter_combinations
);

class NodeMergeRightTests: public NodeMergeTests {
public:
    NodeMergeRightTests()
    {
        // TODO: We use an index of 1 here for reasons. Fix that. `index` should refer to the index of lhs in parent.
        //       We'll have to correct the call sites in Tree too.
        merge_right(lhs, rhs, parent, 1);
    }
    ~NodeMergeRightTests() override = default;
};

TEST_P(NodeMergeRightTests, MergeRight)
{
    // Note that merge_right() actually merges into the left node. This is so that we don't have to potentially recur back up
    // the tree and back down in order to update the right sibling ID of the node's left sibling (if it is an external node).
    ASSERT_EQ(parent.child_id(0), lhs.id());
    ASSERT_EQ(parent.rightmost_child_id(), lhs.id());
    ASSERT_EQ(parent.cell_count(), 0);

    ASSERT_EQ(lhs.cell_count(), 5);
    check_merged_node(lhs);

    if (!lhs.is_external()) {
        ASSERT_EQ(lhs.child_id(0), PID {10});
        ASSERT_EQ(lhs.child_id(1), PID {20});
        ASSERT_EQ(lhs.child_id(2), arbitrary_pid);
        ASSERT_EQ(lhs.child_id(3), PID {40});
        ASSERT_EQ(lhs.child_id(4), PID {50});
        ASSERT_EQ(lhs.child_id(5), arbitrary_pid);
    }
}

INSTANTIATE_TEST_SUITE_P(
    ExternalMergeRight,
    NodeMergeRightTests,
    external_node_parameter_combinations
);

INSTANTIATE_TEST_SUITE_P(
    InternalMergeRight,
    NodeMergeRightTests,
    internal_node_parameter_combinations
);

} // <anonymous>