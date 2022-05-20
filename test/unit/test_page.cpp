#include <gtest/gtest.h>

#include "common.h"
#include "page/cell.h"
#include "page/node.h"
#include "page/page.h"
#include "utils/assert.h"
#include "utils/scratch.h"
#include "utils/slice.h"
#include "unit.h"

namespace {

using namespace cub;

class PageTests: public testing::Test {
public:
    static constexpr Size page_size = 0x100;
    static constexpr Size halfway_point = page_size / 2;

    PageTests()
        : m_scratch{page_size} {}

    auto get_page(PID id) -> Page
    {
        m_backing.emplace(id, std::string(page_size, '\x00'));
        Page page {{id, to_bytes(m_backing[id]), nullptr, true, false}};
        page.enable_tracking(m_scratch.get());
        return page;
    }

private:
    std::unordered_map<PID, std::string, PID::Hasher> m_backing;
    ScratchManager m_scratch;
};

TEST_F(PageTests, FreshPagesAreEmpty)
{
    auto page = get_page(PID::root());
    ASSERT_FALSE(page.has_changes());
    ASSERT_TRUE(page.range(0) == to_bytes(std::string(page_size, '\x00')));
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
    ASSERT_EQ(page.lsn(), LSN::null());
    page.undo_changes(LSN::base(), changes);
    ASSERT_EQ(page.lsn(), LSN::base()) << "Page LSN should have been updated";
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

auto make_cell(Node &node, const std::string &key, std::string &value)
{
    auto builder = CellBuilder{node.page().size()}
        .set_key(to_bytes(key))
        .set_value(to_bytes(value));

    if (const auto overflow = builder.overflow(); !overflow.is_empty())
        value.resize(value.size() - overflow.size());

    return builder.build();
}

class NodeTests: public PageTests {
public:
    NodeTests()
        : overflow_value(0x100, 'x') {}

    auto make_node(PID id, PageType type)
    {
        Node node {get_page(id), true};
        node.page().set_type(type);
        return node;
    }

    PID arbitrary_pid {2};
    std::string normal_value {"world"};
    std::string overflow_value;
};

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
    ASSERT_DEATH(node.remove_at(0, Cell::MAX_HEADER_SIZE), EXPECTATION_MATCHER);
}

TEST_F(NodeTests, FindInEmptyNodeFindsNothing)
{
    auto node = make_node(PID::root(), PageType::EXTERNAL_NODE);
    auto [index, found_eq] = node.find_ge(to_bytes("hello"));
    ASSERT_FALSE(found_eq);

    // We would insert "hello" at this index.
    ASSERT_EQ(index, 0);
}

TEST_F(NodeTests, UsableSpaceIsUpdatedOnInsert)
{
    auto node = make_node(PID::root(), PageType::EXTERNAL_NODE);
    auto cell = make_cell(node, "hello", normal_value);
    const auto usable_space_after = node.usable_space() - cell.size() - CELL_POINTER_SIZE;
    node.insert(std::move(cell));
    ASSERT_EQ(node.usable_space(), usable_space_after);
}

auto get_node_with_one_cell(NodeTests &test, bool has_overflow = false)
{
    auto value = has_overflow ? test.overflow_value : test.normal_value;
    auto node = test.make_node(PID::root(), PageType::INTERNAL_NODE);
    auto cell = make_cell(node, "hello", value);

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
    auto [index, found_eq] = node.find_ge(to_bytes("hello"));
    ASSERT_TRUE(found_eq);
    ASSERT_EQ(index, 0);
}

TEST_F(NodeTests, FindLessThan)
{
    auto node = get_node_with_one_cell(*this);
    auto [index, found_eq] = node.find_ge(to_bytes("helln"));
    ASSERT_FALSE(found_eq);
    ASSERT_EQ(index, 0);
}

TEST_F(NodeTests, FindGreaterThan)
{
    auto node = get_node_with_one_cell(*this);
    auto [index, found_eq] = node.find_ge(to_bytes("hellp"));
    ASSERT_FALSE(found_eq);
    ASSERT_EQ(index, 1);
}

TEST_F(NodeTests, ReadCell)
{
    auto node = get_node_with_one_cell(*this);
    auto cell = node.read_cell(0);
    ASSERT_EQ(cell.left_child_id(), arbitrary_pid);
    ASSERT_EQ(cell.overflow_id(), PID::null());
    ASSERT_TRUE(cell.key() == to_bytes("hello"));
    ASSERT_TRUE(cell.local_value() == to_bytes("world"));
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
    auto cell = make_cell(node, "hello", value);
    node.insert(make_cell(node, "hello", value));
    ASSERT_DEATH(node.insert(make_cell(node, "hello", value)), EXPECTATION_MATCHER);
}

TEST_F(NodeTests, RemovingNonexistentCellDoesNothing)
{
    auto node = get_node_with_one_cell(*this);
    ASSERT_FALSE(node.remove(to_bytes("not_found")));
    ASSERT_EQ(node.cell_count(), 1);
}

TEST_F(NodeTests, RemovingCellDecrementsCellCount)
{
    auto node = get_node_with_one_cell(*this);
    node.remove(to_bytes("hello"));
    ASSERT_EQ(node.cell_count(), 0);
}

TEST_F(NodeTests, UsableSpaceIsUpdatedOnRemove)
{
    auto node = make_node(PID::root(), PageType::EXTERNAL_NODE);
    auto cell = make_cell(node, "hello", normal_value);
    const auto usable_space_before = node.usable_space();
    node.insert(std::move(cell));
    node.remove(to_bytes("hello"));
    ASSERT_EQ(node.usable_space(), usable_space_before);
}

} // <anonymous>