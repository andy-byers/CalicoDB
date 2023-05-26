// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#include "encoding.h"
#include "logging.h"
#include "schema.h"
#include "tree.h"
#include "unit_tests.h"
#include <gtest/gtest.h>

namespace calicodb
{

static constexpr std::size_t kInitialRecordCount = 100;

class NodeTests
    : public PagerTestHarness<tools::FakeEnv>,
      public testing::Test
{
public:
    explicit NodeTests()
        : node_scratch(kPageSize, '\0'),
          cell_scratch(kPageSize, '\0')
    {
    }

    ~NodeTests() override
    {
        m_pager->finish();
    }

    auto SetUp() -> void override
    {
        ASSERT_OK(m_pager->start_reader());
        ASSERT_OK(m_pager->start_writer());
        ASSERT_OK(Tree::create(*m_pager, true, nullptr));
        tree = std::make_unique<Tree>(*m_pager, nullptr);
    }

    [[nodiscard]] auto get_node(bool is_external) -> Node
    {
        Node node;
        EXPECT_OK(tree->allocate(is_external, node));
        return node;
    }

    auto TearDown() -> void override
    {
        m_pager->finish();
    }

    [[nodiscard]] auto make_long_key(std::size_t value) const
    {
        const auto suffix = tools::integral_key<6>(value);
        const std::string key(kPageSize * 2 - suffix.size(), '0');
        return key + suffix;
    }

    [[nodiscard]] auto make_value(char c, bool overflow = false) const
    {
        std::size_t size{kPageSize};
        if (overflow) {
            size /= 3;
        } else {
            size /= 20;
        }
        return std::string(size, c);
    }

    std::unique_ptr<Tree> tree;
    std::string node_scratch;
    std::string cell_scratch;
    std::string collect_scratch;
    tools::RandomGenerator random;
};

class BlockAllocatorTests : public NodeTests
{
public:
    explicit BlockAllocatorTests() = default;

    ~BlockAllocatorTests() override = default;

    auto SetUp() -> void override
    {
        NodeTests::SetUp();
        node = get_node(true);
    }

    auto TearDown() -> void override
    {
        tree->release(std::move(node));
        NodeTests::TearDown();
    }

    auto reserve_for_test(std::size_t n) -> void
    {
        ASSERT_LT(n, kPageSize - FileHeader::kSize - NodeHeader::kSize)
            << "reserve_for_test(" << n << ") leaves no room for possible headers";
        size = n;
        base = kPageSize - n;
    }

    std::size_t size = 0;
    std::size_t base = 0;
    Node node;
};

TEST_F(BlockAllocatorTests, MergesAdjacentBlocks)
{
    reserve_for_test(40);

    // ..........#####...............#####.....
    BlockAllocator::release(node, base + 10, 5);
    BlockAllocator::release(node, base + 30, 5);
    ASSERT_EQ(BlockAllocator::accumulate_free_bytes(node), 10);

    // .....##########...............#####.....
    BlockAllocator::release(node, base + 5, 5);
    ASSERT_EQ(BlockAllocator::accumulate_free_bytes(node), 15);

    // .....##########...............##########
    BlockAllocator::release(node, base + 35, 5);
    ASSERT_EQ(BlockAllocator::accumulate_free_bytes(node), 20);

    // .....###############..........##########
    BlockAllocator::release(node, base + 15, 5);
    ASSERT_EQ(BlockAllocator::accumulate_free_bytes(node), 25);

    // .....###############.....###############
    BlockAllocator::release(node, base + 25, 5);
    ASSERT_EQ(BlockAllocator::accumulate_free_bytes(node), 30);

    // .....###################################
    BlockAllocator::release(node, base + 20, 5);
    ASSERT_EQ(BlockAllocator::accumulate_free_bytes(node), 35);

    // ########################################
    BlockAllocator::release(node, base, 5);
    ASSERT_EQ(BlockAllocator::accumulate_free_bytes(node), size);
}

TEST_F(BlockAllocatorTests, ConsumesAdjacentFragments)
{
    reserve_for_test(40);
    node.header.frag_count = 6;

    // .........*#####**...........**#####*....
    BlockAllocator::release(node, base + 10, 5);
    BlockAllocator::release(node, base + 30, 5);

    // .....##########**...........**#####*....
    BlockAllocator::release(node, base + 5, 4);
    ASSERT_EQ(BlockAllocator::accumulate_free_bytes(node), 15);
    ASSERT_EQ(node.header.frag_count, 5);

    // .....#################......**#####*....
    BlockAllocator::release(node, base + 17, 5);
    ASSERT_EQ(BlockAllocator::accumulate_free_bytes(node), 22);
    ASSERT_EQ(node.header.frag_count, 3);

    // .....##############################*....
    BlockAllocator::release(node, base + 22, 6);
    ASSERT_EQ(BlockAllocator::accumulate_free_bytes(node), 30);
    ASSERT_EQ(node.header.frag_count, 1);

    // .....##############################*....
    BlockAllocator::release(node, base + 36, 4);
    ASSERT_EQ(BlockAllocator::accumulate_free_bytes(node), 35);
    ASSERT_EQ(node.header.frag_count, 0);
}

TEST_F(BlockAllocatorTests, ExternalNodesDoNotConsume3ByteFragments)
{
    reserve_for_test(11);
    node.header.frag_count = 3;

    // ....***####
    BlockAllocator::release(node, base + 7, 4);

    // ####***####
    BlockAllocator::release(node, base + 0, 4);
    ASSERT_EQ(BlockAllocator::accumulate_free_bytes(node), size - node.header.frag_count);
    ASSERT_EQ(node.header.frag_count, 3);
}

TEST_F(BlockAllocatorTests, InternalNodesConsume3ByteFragments)
{
    tree->release(std::move(node));
    node = get_node(false);

    reserve_for_test(11);
    node.header.frag_count = 3;

    // ....***####
    BlockAllocator::release(node, base + 7, 4);

    // ###########
    BlockAllocator::release(node, base + 0, 4);
    ASSERT_EQ(BlockAllocator::accumulate_free_bytes(node), size);
    ASSERT_EQ(node.header.frag_count, 0);
}

TEST_F(NodeTests, AllocatorSkipsPointerMapPage)
{
    // Page 1 is allocated before Pager::open() returns, and this call skips page 2.
    ASSERT_EQ(get_node(true).page.id(), Id(3));
}

class TreeTests
    : public PagerTestHarness<tools::FakeEnv>,
      public testing::TestWithParam<std::size_t>
{
public:
    TreeTests()
        : param(GetParam()),
          collect_scratch(kPageSize, '\x00'),
          root_id(Id::root())
    {
    }

    auto SetUp() -> void override
    {
        ASSERT_OK(m_pager->start_reader());
        ASSERT_OK(m_pager->start_writer());
        ASSERT_OK(Tree::create(*m_pager, true, nullptr));
        tree = std::make_unique<Tree>(*m_pager, nullptr);
    }

    auto TearDown() -> void override
    {
        m_pager->finish();
    }

    [[nodiscard]] auto make_long_key(std::size_t value) const
    {
        const auto suffix = tools::integral_key<6>(value);
        const std::string key(kPageSize * 2 - suffix.size(), '0');
        return key + suffix;
    }

    [[nodiscard]] auto make_value(char c, bool overflow = false) const
    {
        std::size_t size{kPageSize};
        if (overflow) {
            size /= 3;
        } else {
            size /= 20;
        }
        return std::string(size, c);
    }

    tools::RandomGenerator random;
    std::size_t param;
    std::string collect_scratch;
    std::unique_ptr<Tree> tree;
    Id root_id;
};

TEST_P(TreeTests, ConstructsAndDestructs)
{
    tree->TEST_validate();
}

TEST_P(TreeTests, KeysAreUnique)
{
    bool exists;
    ASSERT_OK(tree->put("a", make_value('x')));
    ASSERT_OK(tree->put("b", make_value('2')));
    ASSERT_OK(tree->put("a", make_value('1')));

    tree->TEST_validate();

    std::string value;
    ASSERT_OK(tree->get("a", &value));
    ASSERT_EQ(value, make_value('1'));
    ASSERT_OK(tree->get("b", &value));
    ASSERT_EQ(value, make_value('2'));
}

TEST_P(TreeTests, RecordsAreErased)
{
    (void)tree->put("a", make_value('1'));
    ASSERT_OK(tree->erase("a"));
    std::string value;
    ASSERT_TRUE(tree->get("a", &value).is_not_found());
    ASSERT_OK(tree->erase("a"));
}

TEST_P(TreeTests, HandlesLargePayloads)
{
    ASSERT_OK(tree->put(make_long_key('a'), "1"));
    ASSERT_OK(tree->put("b", make_value('2', true)));
    ASSERT_OK(tree->put(make_long_key('c'), make_value('3', true)));

    std::string value;
    ASSERT_OK(tree->get(make_long_key('a'), &value));
    ASSERT_EQ(value, "1");
    ASSERT_OK(tree->get("b", &value));
    ASSERT_EQ(value, make_value('2', true));
    ASSERT_OK(tree->get(make_long_key('c'), &value));
    ASSERT_EQ(value, make_value('3', true));

    ASSERT_OK(tree->erase(make_long_key('a')));
    ASSERT_OK(tree->erase("b"));
    ASSERT_OK(tree->erase(make_long_key('c')));
}

TEST_P(TreeTests, GetNonexistentKeys)
{
    // Missing 0
    ASSERT_OK(tree->put(make_long_key(1), make_value('0', true)));
    // Missing 2
    ASSERT_OK(tree->put(make_long_key(3), make_value('0', true)));
    ASSERT_OK(tree->put(make_long_key(4), make_value('0', true)));
    ASSERT_OK(tree->put(make_long_key(5), make_value('0', true)));
    // Missing 6
    ASSERT_OK(tree->put(make_long_key(7), make_value('0', true)));
    ASSERT_OK(tree->put(make_long_key(8), make_value('0', true)));
    ASSERT_OK(tree->put(make_long_key(9), make_value('0', true)));
    // Missing 10

    ASSERT_NOK(tree->get(make_long_key(0), nullptr));
    ASSERT_NOK(tree->get(make_long_key(2), nullptr));
    ASSERT_NOK(tree->get(make_long_key(6), nullptr));
    ASSERT_NOK(tree->get(make_long_key(10), nullptr));

    ASSERT_OK(tree->get(make_long_key(1), nullptr));
    ASSERT_OK(tree->get(make_long_key(3), nullptr));
    ASSERT_OK(tree->get(make_long_key(5), nullptr));
    ASSERT_OK(tree->get(make_long_key(7), nullptr));
    ASSERT_OK(tree->get(make_long_key(9), nullptr));
}

TEST_P(TreeTests, ResolvesOverflowsOnLeftmostPosition)
{
    for (std::size_t i = 0; i < 100; ++i) {
        ASSERT_OK(tree->put(make_long_key(99 - i), make_value('v', true)));
    }
    tree->TEST_validate();
}

TEST_P(TreeTests, ResolvesOverflowsOnRightmostPosition)
{
    for (std::size_t i = 0; i < 100; ++i) {
        ASSERT_OK(tree->put(make_long_key(i), make_value('v')));
    }
    tree->TEST_validate();
}

TEST_P(TreeTests, ResolvesOverflowsOnMiddlePosition)
{
    for (std::size_t i = 0, j = 99; i < j; ++i, --j) {
        ASSERT_OK(tree->put(make_long_key(i), make_value('v')));
        ASSERT_OK(tree->put(make_long_key(j), make_value('v')));
    }
    tree->TEST_validate();
}

static auto add_initial_records(TreeTests &test, bool has_overflow = false)
{
    for (std::size_t i = 0; i < kInitialRecordCount; ++i) {
        (void)test.tree->put(test.make_long_key(i), test.make_value('v', has_overflow));
    }
}

TEST_P(TreeTests, ToStringDoesNotCrash)
{
    add_initial_records(*this);
    (void)tree->TEST_to_string();
}

TEST_P(TreeTests, ResolvesUnderflowsOnRightmostPosition)
{
    add_initial_records(*this);
    for (std::size_t i = 0; i < kInitialRecordCount; ++i) {
        ASSERT_OK(tree->erase(make_long_key(kInitialRecordCount - i - 1)));
    }
    tree->TEST_validate();
}

TEST_P(TreeTests, ResolvesUnderflowsOnLeftmostPosition)
{
    add_initial_records(*this);
    for (std::size_t i = 0; i < kInitialRecordCount; ++i) {
        ASSERT_OK(tree->erase(make_long_key(i)));
    }
    tree->TEST_validate();
}

TEST_P(TreeTests, ResolvesUnderflowsOnMiddlePosition)
{
    add_initial_records(*this);
    for (std::size_t i{}, j{kInitialRecordCount - 1}; i < j; ++i, --j) {
        ASSERT_OK(tree->erase(make_long_key(i)));
        ASSERT_OK(tree->erase(make_long_key(j)));
    }
    tree->TEST_validate();
}

TEST_P(TreeTests, ResolvesOverflowsFromOverwrite)
{
    add_initial_records(*this);
    // Replace the small values with very large ones.
    add_initial_records(*this, true);
    tree->TEST_validate();
}

TEST_P(TreeTests, SplitWithShortAndLongKeys)
{
    for (unsigned i = 0; i < kInitialRecordCount; ++i) {
        char key[3]{};
        put_u16(key, kInitialRecordCount - i - 1);
        ASSERT_OK(tree->put({key, 2}, "v"));
    }
    for (unsigned i = 0; i < kInitialRecordCount; ++i) {
        const auto key = random.Generate(kPageSize);
        ASSERT_OK(tree->put(key, "v"));
    }
    tree->TEST_validate();
}

TEST_P(TreeTests, EmptyKeyBehavior)
{
    ASSERT_TRUE(tree->put("", "").is_invalid_argument());
    ASSERT_TRUE(tree->get("", nullptr).is_not_found());
    ASSERT_OK(tree->erase(""));
}

INSTANTIATE_TEST_SUITE_P(
    TreeTests,
    TreeTests,
    ::testing::Values(0));

class TreeSanityChecks : public TreeTests
{
public:
    auto random_chunk(bool overflow, bool nonzero = true)
    {
        return random.Generate(random.Next(nonzero, kPageSize * overflow + 12));
    }

    auto random_write() -> std::pair<std::string, std::string>
    {
        const auto key = random_chunk(overflow_keys);
        const auto val = random_chunk(overflow_values, false);
        EXPECT_OK(tree->put(key, val));
        return {key.to_string(), val.to_string()};
    }

    bool overflow_keys = GetParam() & 0b10;
    bool overflow_values = GetParam() & 0b01;
};

TEST_P(TreeSanityChecks, Insert)
{
    for (std::size_t i = 0; i < kInitialRecordCount * 10; ++i) {
        random_write();
    }
    tree->TEST_validate();
}

TEST_P(TreeSanityChecks, Search)
{
    std::unordered_map<std::string, std::string> records;
    for (std::size_t i = 0; i < kInitialRecordCount * 10; ++i) {
        const auto [k, v] = random_write();
        records[k] = v;
    }
    tree->TEST_validate();

    int i = 0;
    for (const auto &[key, value] : records) {
        std::string result;
        ASSERT_OK(tree->get(key, &result));
        ASSERT_EQ(result, value);

        ASSERT_OK(tree->erase(key));
        ASSERT_TRUE(tree->get(key, &result).is_not_found());

        ++i;
    }
}

TEST_P(TreeSanityChecks, Erase)
{
    std::unordered_map<std::string, std::string> records;
    for (std::size_t iteration = 0; iteration < 3; ++iteration) {
        for (std::size_t i = 0; i < kInitialRecordCount * 10; ++i) {
            const auto [k, v] = random_write();
            records[k] = v;
        }

        std::size_t i = 0;
        for (const auto &[key, value] : records) {
            ASSERT_OK(tree->erase(key));
        }
        tree->TEST_validate();
        records.clear();
    }
}

TEST_P(TreeSanityChecks, SmallRecords)
{
    std::unordered_map<std::string, std::string> records;
    for (std::size_t iteration = 0; iteration < 3; ++iteration) {
        for (std::size_t i = 0; i < kInitialRecordCount * 100; ++i) {
            const auto key = tools::integral_key<6>(i);
            ASSERT_OK(tree->put(key, ""));
            records[key] = "";
        }

        std::size_t i = 0;
        for (const auto &[key, value] : records) {
            ASSERT_OK(tree->erase(key));
        }
        tree->TEST_validate();
        records.clear();
    }
}

TEST_P(TreeSanityChecks, Destruction)
{
    for (std::size_t i = 0; i < kInitialRecordCount * 10; ++i) {
        random_write();
    }
    ASSERT_OK(Tree::destroy(*tree));
}

// Parameter bits:
//     0b01: Use overflowing values
//     0b10: Use overflowing keys
INSTANTIATE_TEST_SUITE_P(
    TreeSanityChecks,
    TreeSanityChecks,
    ::testing::Values(
        0b00,
        0b01,
        0b10,
        0b11));

class EmptyTreeCursorTests : public TreeTests
{
protected:
    ~EmptyTreeCursorTests() override = default;
    auto SetUp() -> void override
    {
        TreeTests::SetUp();
    }
};

TEST_P(EmptyTreeCursorTests, KeyAndValueUseSeparateMemory)
{
    std::unique_ptr<Cursor> cursor(new CursorImpl(*tree));
    cursor->seek_first();
    ASSERT_FALSE(cursor->is_valid());
    cursor->seek_last();
    ASSERT_FALSE(cursor->is_valid());
    cursor->seek("42");
    ASSERT_FALSE(cursor->is_valid());
}

INSTANTIATE_TEST_SUITE_P(
    EmptyTreeCursorTests,
    EmptyTreeCursorTests,
    ::testing::Values(0));

class CursorTests : public TreeTests
{
protected:
    ~CursorTests() override = default;
    auto SetUp() -> void override
    {
        TreeTests::SetUp();
        add_initial_records(*this);
    }

    auto make_cursor() -> std::unique_ptr<Cursor>
    {
        switch (GetParam()) {
            case 0:
                return std::make_unique<CursorImpl>(*tree);
            case 1:
                return std::make_unique<SchemaCursor>(*tree);
        }
        return nullptr;
    }
};

TEST_P(CursorTests, AccountsForNodeBoundaries)
{
    for (std::size_t i = 0; i + 5 < kInitialRecordCount; i += 5) {
        ASSERT_OK(tree->erase(make_long_key(i + 1)));
        ASSERT_OK(tree->erase(make_long_key(i + 2)));
        ASSERT_OK(tree->erase(make_long_key(i + 3)));
        ASSERT_OK(tree->erase(make_long_key(i + 4)));
    }
    auto cursor = make_cursor();
    for (std::size_t i = 0; i + 10 < kInitialRecordCount; i += 5) {
        cursor->seek(make_long_key(i + 1));
        ASSERT_EQ(make_long_key(i + 5), cursor->key());
        cursor->seek(make_long_key(i + 2));
        ASSERT_EQ(make_long_key(i + 5), cursor->key());
        cursor->seek(make_long_key(i + 3));
        ASSERT_EQ(make_long_key(i + 5), cursor->key());
        cursor->seek(make_long_key(i + 4));
        ASSERT_EQ(make_long_key(i + 5), cursor->key());
    }
}

TEST_P(CursorTests, SeeksForward)
{
    auto cursor = make_cursor();
    cursor->seek_first();
    std::size_t i = 0;
    while (cursor->is_valid()) {
        ASSERT_TRUE(cursor->is_valid());
        ASSERT_EQ(cursor->key(), make_long_key(i++));
        ASSERT_EQ(cursor->value(), make_value('v'));
        cursor->next();
    }
    ASSERT_EQ(i, kInitialRecordCount);
}

TEST_P(CursorTests, SeeksForwardFromBoundary)
{
    auto cursor = make_cursor();
    cursor->seek(make_long_key(kInitialRecordCount / 4));
    while (cursor->is_valid()) {
        cursor->next();
    }
}

TEST_P(CursorTests, SeeksForwardToBoundary)
{
    auto cursor = make_cursor();
    auto bounds = make_cursor();
    cursor->seek_first();
    bounds->seek(make_long_key(kInitialRecordCount * 3 / 4));
    while (cursor->key() != bounds->key()) {
        ASSERT_TRUE(cursor->is_valid());
        cursor->next();
    }
}

TEST_P(CursorTests, SeeksForwardBetweenBoundaries)
{
    auto cursor = make_cursor();
    cursor->seek(make_long_key(kInitialRecordCount / 4));
    auto bounds = make_cursor();
    bounds->seek(make_long_key(kInitialRecordCount * 3 / 4));
    while (cursor->key() != bounds->key()) {
        ASSERT_TRUE(cursor->is_valid());
        cursor->next();
    }
}

TEST_P(CursorTests, SeeksBackward)
{
    auto cursor = make_cursor();
    cursor->seek_last();
    std::size_t i = 0;
    while (cursor->is_valid()) {
        ASSERT_EQ(cursor->key().to_string(), make_long_key(kInitialRecordCount - 1 - i++));
        ASSERT_EQ(cursor->value(), make_value('v'));
        cursor->previous();
    }
    ASSERT_EQ(i, kInitialRecordCount);
}

TEST_P(CursorTests, SeeksBackwardFromBoundary)
{
    auto cursor = make_cursor();
    const auto bounds = kInitialRecordCount * 3 / 4;
    cursor->seek(make_long_key(bounds));
    for (std::size_t i = 0; i <= bounds; ++i) {
        ASSERT_TRUE(cursor->is_valid());
        cursor->previous();
    }
    ASSERT_FALSE(cursor->is_valid());
}

TEST_P(CursorTests, SeeksBackwardToBoundary)
{
    auto cursor = make_cursor();
    cursor->seek_last();
    auto bounds = make_cursor();
    bounds->seek(make_long_key(kInitialRecordCount / 4));
    while (cursor->key() != bounds->key()) {
        ASSERT_TRUE(cursor->is_valid());
        cursor->previous();
    }
}

TEST_P(CursorTests, SeeksBackwardBetweenBoundaries)
{
    auto cursor = make_cursor();
    auto bounds = make_cursor();
    cursor->seek(make_long_key(kInitialRecordCount * 3 / 4));
    bounds->seek(make_long_key(kInitialRecordCount / 4));
    while (cursor->key() != bounds->key()) {
        ASSERT_TRUE(cursor->is_valid());
        ASSERT_NE(cursor->key(), bounds->key());
        cursor->previous();
    }
    ASSERT_EQ(cursor->key(), bounds->key());
}

TEST_P(CursorTests, SanityCheck_Forward)
{
    auto cursor = make_cursor();
    for (std::size_t iteration = 0; iteration < 100; ++iteration) {
        const auto i = random.Next(kInitialRecordCount - 1);
        const auto key = make_long_key(i);
        cursor->seek(key);

        ASSERT_TRUE(cursor->is_valid());
        ASSERT_EQ(cursor->key(), key);

        for (std::size_t n = 0; n < random.Next(10); ++n) {
            cursor->next();

            if (const auto j = i + n + 1; j < kInitialRecordCount) {
                ASSERT_TRUE(cursor->is_valid());
                ASSERT_EQ(cursor->key(), make_long_key(j));
            } else {
                ASSERT_FALSE(cursor->is_valid());
                break;
            }
        }
    }
}

TEST_P(CursorTests, SanityCheck_Backward)
{
    auto cursor = make_cursor();
    for (std::size_t iteration = 0; iteration < 100; ++iteration) {
        const auto i = random.Next(kInitialRecordCount - 1);
        const auto key = make_long_key(i);
        cursor->seek(key);

        ASSERT_TRUE(cursor->is_valid());
        ASSERT_EQ(cursor->key(), key);

        for (std::size_t n = 0; n < random.Next(10); ++n) {
            cursor->previous();

            if (i > n) {
                ASSERT_TRUE(cursor->is_valid());
                ASSERT_EQ(cursor->key(), make_long_key(i - n - 1));
            } else {
                ASSERT_FALSE(cursor->is_valid());
                break;
            }
        }
    }
}

TEST_P(CursorTests, SeekOutOfRange)
{
    ASSERT_OK(tree->erase(make_long_key(0)));
    auto cursor = make_cursor();

    cursor->seek(make_long_key(0));
    ASSERT_TRUE(cursor->is_valid());
    ASSERT_EQ(cursor->key(), make_long_key(1));

    cursor->seek(make_long_key(kInitialRecordCount));
    ASSERT_FALSE(cursor->is_valid());
}

#if not NDEBUG
TEST_P(CursorTests, InvalidCursorDeathTest)
{
    auto cursor = make_cursor();
    ASSERT_DEATH((void)cursor->key(), kExpectationMatcher);
    ASSERT_DEATH((void)cursor->value(), kExpectationMatcher);
    ASSERT_DEATH((void)cursor->next(), kExpectationMatcher);
    ASSERT_DEATH((void)cursor->previous(), kExpectationMatcher);
}
#endif // NDEBUG

INSTANTIATE_TEST_SUITE_P(
    CursorTests,
    CursorTests,
    ::testing::Values(0, 1));

class PointerMapTests : public TreeTests
{
public:
    [[nodiscard]] auto map_size() -> std::size_t
    {
        return kPageSize / (sizeof(char) + Id::kSize);
    }
};

TEST_P(PointerMapTests, FirstPointerMapIsPage2)
{
    ASSERT_EQ(PointerMap::lookup(Id(1)), Id(0));
    ASSERT_EQ(PointerMap::lookup(Id(2)), Id(2));
    ASSERT_EQ(PointerMap::lookup(Id(3)), Id(2));
    ASSERT_EQ(PointerMap::lookup(Id(4)), Id(2));
    ASSERT_EQ(PointerMap::lookup(Id(5)), Id(2));
}

TEST_P(PointerMapTests, ReadsAndWritesEntries)
{
    ASSERT_OK(PointerMap::write_entry(*m_pager, Id(3), PointerMap::Entry{Id(33), PointerMap::kTreeNode}));
    ASSERT_OK(PointerMap::write_entry(*m_pager, Id(4), PointerMap::Entry{Id(44), PointerMap::kFreelistLink}));
    ASSERT_OK(PointerMap::write_entry(*m_pager, Id(5), PointerMap::Entry{Id(55), PointerMap::kOverflowLink}));

    PointerMap::Entry entry_1, entry_2, entry_3;
    ASSERT_OK(PointerMap::read_entry(*m_pager, Id(3), entry_1));
    ASSERT_OK(PointerMap::read_entry(*m_pager, Id(4), entry_2));
    ASSERT_OK(PointerMap::read_entry(*m_pager, Id(5), entry_3));

    ASSERT_EQ(entry_1.back_ptr.value, 33);
    ASSERT_EQ(entry_2.back_ptr.value, 44);
    ASSERT_EQ(entry_3.back_ptr.value, 55);
    ASSERT_EQ(entry_1.type, PointerMap::kTreeNode);
    ASSERT_EQ(entry_2.type, PointerMap::kFreelistLink);
    ASSERT_EQ(entry_3.type, PointerMap::kOverflowLink);
}

TEST_P(PointerMapTests, PointerMapCanFitAllPointers)
{
    // PointerMap::find_map() expects the given pointer map page to be allocated already.
    for (std::size_t i = 0; i < map_size() * 2; ++i) {
        Page page;
        ASSERT_OK(m_pager->allocate(page));
        m_pager->release(std::move(page));
    }

    for (std::size_t i = 0; i < map_size() + 10; ++i) {
        if (i != map_size()) {
            const Id id(i + 3);
            const PointerMap::Entry entry{id, PointerMap::kTreeNode};
            ASSERT_OK(PointerMap::write_entry(*m_pager, id, entry));
        }
    }
    for (std::size_t i = 0; i < map_size() + 10; ++i) {
        if (i != map_size()) {
            const Id id(i + 3);
            PointerMap::Entry entry;
            ASSERT_OK(PointerMap::read_entry(*m_pager, id, entry));
            ASSERT_EQ(entry.back_ptr.value, id.value);
            ASSERT_EQ(entry.type, PointerMap::kTreeNode);
        }
    }
}

TEST_P(PointerMapTests, MapPagesAreRecognized)
{
    Id id(2);
    ASSERT_EQ(PointerMap::lookup(id), id);

    // Back pointers for the next "map.map_size()" pages are stored on page 2. The next pointermap page is
    // the page following the last page whose back pointer is on page 2. This pattern continues forever.
    for (std::size_t i = 0; i < 1'000'000; ++i) {
        id.value += map_size() + 1;
        ASSERT_EQ(PointerMap::lookup(id), id);
    }
}

TEST_P(PointerMapTests, FindsCorrectMapPages)
{
    std::size_t counter = 0;
    Id map_id(2);

    for (Id page_id(3); page_id.value <= 100 * map_size(); ++page_id.value) {
        if (counter++ == map_size()) {
            // Found a map page. Calls to find() with a page ID between this page and the next map page
            // should map to this page ID.
            map_id.value += map_size() + 1;
            counter = 0;
        } else {
            ASSERT_EQ(PointerMap::lookup(page_id), map_id);
        }
    }
}

#ifndef NDEBUG
TEST_P(PointerMapTests, LookupNullIdDeathTest)
{
    ASSERT_DEATH((void)PointerMap::lookup(Id(0)), kExpectationMatcher);
    ASSERT_DEATH((void)PointerMap::is_map(Id(0)), kExpectationMatcher);
}
#endif // NDEBUG

INSTANTIATE_TEST_SUITE_P(
    PointerMapTests,
    PointerMapTests,
    ::testing::Values(0));

class VacuumTests : public TreeTests
{
public:
    auto SetUp() -> void override
    {
        TreeTests::SetUp();
        cell_scratch.resize(kPageSize);
        node_scratch.resize(kPageSize);
        schema = std::make_unique<Schema>(*m_pager, m_state.status);
    }

    auto acquire_node(Id pid, bool is_writable = false)
    {
        Node node;
        EXPECT_OK(tree->acquire(pid, is_writable, node));
        return node;
    }

    auto allocate_node(bool is_external)
    {
        Node node;
        EXPECT_OK(tree->allocate(is_external, node));
        return node;
    }

    auto release_node(Node node) const
    {
        tree->release(std::move(node));
    }

    auto is_root_external()
    {
        auto root = acquire_node(Id::root());
        const auto is_external = root.header.is_external;
        release_node(std::move(root));
        return is_external;
    }

    auto clean_up_test(std::size_t max_key_size, std::size_t max_value_size)
    {
        std::vector<std::string> keys;
        while (is_root_external()) {
            const auto key = random.Generate(max_key_size);
            const auto exists = tree->get(key, nullptr).is_ok();
            (void)tree->put(key, random.Generate(max_value_size));
            if (!exists) {
                keys.emplace_back(key.to_string());
            }
            ASSERT_NE(PointerMap::lookup(Id(m_pager->page_count())), Id(m_pager->page_count()));
        }
        for (const auto &key : keys) {
            ASSERT_OK(tree->erase(key));
        }

        bool vacuumed;
        Id target(m_pager->page_count());
        do {
            ASSERT_OK(tree->vacuum_one(target, *schema, &vacuumed));
            --target.value;
        } while (vacuumed);
        ASSERT_TRUE(target.is_null());
    }

    auto sanity_check(std::size_t lower_bounds, std::size_t record_count, std::size_t max_key_size, std::size_t max_value_size) const
    {
        std::unordered_map<std::string, std::string> map;

        for (std::size_t iteration = 0; iteration < 5; ++iteration) {
            while (map.size() < lower_bounds + record_count) {
                const auto key_size = random.Next(1, max_key_size);
                const auto key = random.Generate(key_size);
                const auto value_size = random.Next(max_value_size);
                const auto value = random.Generate(value_size);
                ASSERT_OK(tree->put(key, value));
                map[key.to_string()] = value.to_string();
            }

            auto itr = begin(map);
            while (map.size() > lower_bounds) {
                ASSERT_OK(tree->erase(itr->first));
                itr = map.erase(itr);
            }

            Id target(m_pager->page_count());
            for (;;) {
                bool vacuumed = false;
                ASSERT_OK(tree->vacuum_one(target, *schema, &vacuumed));
                if (!vacuumed) {
                    break;
                }
                tree->TEST_validate();
                --target.value;
            }

            m_pager->set_page_count(target.value);

            auto *cursor = new CursorImpl(*tree);
            for (const auto &[key, value] : map) {
                cursor->seek(key);
                ASSERT_TRUE(cursor->is_valid());
                ASSERT_EQ(cursor->key(), key);
                ASSERT_EQ(cursor->value(), value);
            }
            delete cursor;
        }
    }

    auto vacuum_and_validate(const std::string &value)
    {
        bool vacuumed;
        ASSERT_EQ(m_pager->page_count(), 6);
        ASSERT_OK(tree->vacuum_one(Id(6), *schema, &vacuumed));
        ASSERT_TRUE(vacuumed);
        ASSERT_OK(tree->vacuum_one(Id(5), *schema, &vacuumed));
        ASSERT_TRUE(vacuumed);
        m_pager->set_page_count(4);
        ASSERT_EQ(m_pager->page_count(), 4);

        std::string result;
        ASSERT_OK(tree->get("a", &result));
        ASSERT_EQ(result, "value");
        ASSERT_OK(tree->get("b", &result));
        ASSERT_EQ(result, value);
    }

    std::unique_ptr<Schema> schema;
    std::string node_scratch;
    std::string cell_scratch;
};

//      P   1   2   3
// [1] [2] [3] [4] [5]
//
TEST_P(VacuumTests, FreelistRegistersBackPointers)
{
    // Should skip page 2, leaving it available for use as a pointer map.
    auto node_3 = allocate_node(true);
    auto node_4 = allocate_node(true);
    auto node_5 = allocate_node(true);
    ASSERT_EQ(node_5.page.id().value, 5);

    ASSERT_OK(m_pager->destroy(std::move(node_5.page)));
    ASSERT_OK(m_pager->destroy(std::move(node_4.page)));
    ASSERT_OK(m_pager->destroy(std::move(node_3.page)));

    PointerMap::Entry entry;
    ASSERT_OK(PointerMap::read_entry(*m_pager, Id(5), entry));
    ASSERT_EQ(entry.type, PointerMap::kFreelistLink);
    ASSERT_EQ(entry.back_ptr, Id(4));

    ASSERT_OK(PointerMap::read_entry(*m_pager, Id(4), entry));
    ASSERT_EQ(entry.type, PointerMap::kFreelistLink);
    ASSERT_EQ(entry.back_ptr, Id(3));

    ASSERT_OK(PointerMap::read_entry(*m_pager, Id(3), entry));
    ASSERT_EQ(entry.type, PointerMap::kFreelistLink);
    ASSERT_EQ(entry.back_ptr, Id::null());
}

TEST_P(VacuumTests, OverflowChainRegistersBackPointers)
{
    // Creates an overflow chain of length 2, rooted at the second cell on the root page.
    std::string overflow_data(kPageSize * 2, 'x');
    ASSERT_OK(tree->put("a", overflow_data));

    PointerMap::Entry head_entry, tail_entry;
    ASSERT_OK(PointerMap::read_entry(*m_pager, Id(3), head_entry));
    ASSERT_OK(PointerMap::read_entry(*m_pager, Id(4), tail_entry));

    ASSERT_TRUE(head_entry.back_ptr.is_root());
    ASSERT_EQ(tail_entry.back_ptr, Id(3));
    ASSERT_EQ(head_entry.type, PointerMap::kOverflowHead);
    ASSERT_EQ(tail_entry.type, PointerMap::kOverflowLink);
}

TEST_P(VacuumTests, OverflowChainIsNullTerminated)
{
    {
        // NodeManager::allocate() accounts for the first pointer map page on page 2.
        auto node_3 = allocate_node(true);
        Page page_4;
        ASSERT_OK(m_pager->allocate(page_4));
        ASSERT_EQ(page_4.id().value, 4);
        write_next_id(node_3.page, Id(3));
        write_next_id(page_4, Id(3));
        ASSERT_OK(m_pager->destroy(std::move(page_4)));
        ASSERT_OK(m_pager->destroy(std::move(node_3.page)));
    }

    ASSERT_OK(tree->put("a", std::string(3 * kPageSize / 2, 'x')));

    Page page_3;
    Page page_4;
    ASSERT_OK(m_pager->acquire(Id(3), page_3));
    ASSERT_OK(m_pager->acquire(Id(4), page_4));
    ASSERT_EQ(read_next_id(page_3), Id(4));
    ASSERT_EQ(read_next_id(page_4), Id::null());
    m_pager->release(std::move(page_3));
    m_pager->release(std::move(page_4));
}

TEST_P(VacuumTests, VacuumsFreelistInOrder)
{
    auto node_3 = allocate_node(true);
    auto node_4 = allocate_node(true);
    auto node_5 = allocate_node(true);
    ASSERT_EQ(node_5.page.id().value, 5);

    // Page Types:     N   P   3   2   1
    // Page Contents: [1] [2] [3] [4] [5]
    // Page IDs:       1   2   3   4   5
    ASSERT_OK(m_pager->destroy(std::move(node_3.page)));
    ASSERT_OK(m_pager->destroy(std::move(node_4.page)));
    ASSERT_OK(m_pager->destroy(std::move(node_5.page)));

    // Page Types:     N   P   2   1
    // Page Contents: [1] [2] [3] [4] [X]
    // Page IDs:       1   2   3   4   5
    bool vacuumed = false;
    ASSERT_OK(tree->vacuum_one(Id(5), *schema, &vacuumed));
    ASSERT_TRUE(vacuumed);

    PointerMap::Entry entry;
    ASSERT_OK(PointerMap::read_entry(*m_pager, Id(4), entry));
    ASSERT_EQ(entry.type, PointerMap::kFreelistLink);
    ASSERT_EQ(entry.back_ptr, Id::null());

    // Page Types:     N   P   1
    // Page Contents: [1] [2] [3] [X] [X]
    // Page IDs:       1   2   3   4   5
    ASSERT_OK(tree->vacuum_one(Id(4), *schema, &vacuumed));
    ASSERT_TRUE(vacuumed);
    ASSERT_OK(PointerMap::read_entry(*m_pager, Id(3), entry));
    ASSERT_EQ(entry.type, PointerMap::kFreelistLink);
    ASSERT_EQ(entry.back_ptr, Id::null());

    // Page Types:     N   P
    // Page Contents: [1] [2] [X] [X] [X]
    // Page IDs:       1   2   3   4   5
    ASSERT_OK(tree->vacuum_one(Id(3), *schema, &vacuumed));
    ASSERT_TRUE(vacuumed);

    // Page Types:     N
    // Page Contents: [1] [X] [X] [X] [X]
    // Page IDs:       1   2   3   4   5
    ASSERT_OK(tree->vacuum_one(Id(2), *schema, &vacuumed));
    ASSERT_TRUE(vacuumed);

    // Page Types:     N
    // Page Contents: [1]
    // Page IDs:       1
    m_pager->set_page_count(1);
    ASSERT_EQ(m_pager->page_count(), 1);
}

TEST_P(VacuumTests, VacuumsFreelistInReverseOrder)
{
    auto node_3 = allocate_node(true);
    auto node_4 = allocate_node(true);
    auto node_5 = allocate_node(true);

    // Page Types:     N   P   1   2   3
    // Page Contents: [a] [b] [c] [d] [e]
    // Page IDs:       1   2   3   4   5
    ASSERT_OK(m_pager->destroy(std::move(node_5.page)));
    ASSERT_OK(m_pager->destroy(std::move(node_4.page)));
    ASSERT_OK(m_pager->destroy(std::move(node_3.page)));

    // Step 1:
    //     Page Types:     N   P       1   2
    //     Page Contents: [a] [b] [e] [d] [e]
    //     Page IDs:       1   2   3   4   5

    // Step 2:
    //     Page Types:     N   P   2   1
    //     Page Contents: [a] [b] [e] [d] [ ]
    //     Page IDs:       1   2   3   4   5
    bool vacuumed = false;
    ASSERT_OK(tree->vacuum_one(Id(5), *schema, &vacuumed));
    ASSERT_TRUE(vacuumed);
    PointerMap::Entry entry;
    ASSERT_OK(PointerMap::read_entry(*m_pager, Id(4), entry));
    ASSERT_EQ(entry.back_ptr, Id::null());
    ASSERT_EQ(entry.type, PointerMap::kFreelistLink);
    {
        Page page;
        ASSERT_OK(m_pager->acquire(Id(4), page));
        ASSERT_EQ(read_next_id(page), Id(3));
        m_pager->release(std::move(page));
    }

    // Page Types:     N   P   1
    // Page Contents: [a] [b] [e] [ ] [ ]
    // Page IDs:       1   2   3   4   5
    ASSERT_OK(tree->vacuum_one(Id(4), *schema, &vacuumed));
    ASSERT_TRUE(vacuumed);
    ASSERT_OK(PointerMap::read_entry(*m_pager, Id(3), entry));
    ASSERT_EQ(entry.type, PointerMap::kFreelistLink);
    ASSERT_EQ(entry.back_ptr, Id::null());

    // Page Types:     N   P
    // Page Contents: [a] [b] [ ] [ ] [ ]
    // Page IDs:       1   2   3   4   5
    ASSERT_OK(tree->vacuum_one(Id(3), *schema, &vacuumed));
    ASSERT_TRUE(vacuumed);

    // Page Types:     N
    // Page Contents: [a] [ ] [ ] [ ] [ ]
    // Page IDs:       1   2   3   4   5
    ASSERT_OK(tree->vacuum_one(Id(2), *schema, &vacuumed));
    ASSERT_TRUE(vacuumed);

    // Page Types:     N
    // Page Contents: [a]
    // Page IDs:       1
    m_pager->set_page_count(1);
    ASSERT_EQ(m_pager->page_count(), 1);
}

TEST_P(VacuumTests, CleansUpOverflowValues)
{
    clean_up_test(16, kPageSize * 2);
}

// When external nodes are merged, the separator is removed. If the separator key was overflowing, we
// must not forget to move its overflow pages to the freelist.
TEST_P(VacuumTests, CleansUpOverflowKeys)
{
    clean_up_test(kPageSize * 2, 16);
}

TEST_P(VacuumTests, CleansUpOverflowPayloads)
{
    clean_up_test(kPageSize * 2, kPageSize * 2);
}

TEST_P(VacuumTests, VacuumFreelistSanityCheck)
{
    static constexpr std::size_t kNumAllocations = kFrameCount / 2;
    std::default_random_engine rng(42);

    for (std::size_t iteration = 0; iteration < 1'000; ++iteration) {
        ASSERT_EQ(m_pager->page_count(), 1);

        std::vector<Node> nodes;
        for (std::size_t i = 0; i < kNumAllocations; ++i) {
            nodes.emplace_back(allocate_node(true));
        }

        std::shuffle(begin(nodes), end(nodes), rng);

        for (auto &node : nodes) {
            ASSERT_OK(m_pager->destroy(std::move(node).take()));
        }

        // This will vacuum the whole freelist, as well as the pointer map page on page 2.
        Id target(m_pager->page_count());
        bool vacuumed = false;
        for (std::size_t i = 0; i < kNumAllocations; ++i) {
            ASSERT_OK(tree->vacuum_one(target, *schema, &vacuumed));
            ASSERT_TRUE(vacuumed);
            --target.value;
        }
        m_pager->set_page_count(1);
    }
}

TEST_P(VacuumTests, VacuumsOverflowChain_A)
{
    // Save these pages until the overflow chain is created, otherwise they will be used for it.
    auto node_3 = allocate_node(true);
    auto node_4 = allocate_node(true);
    ASSERT_EQ(node_4.page.id().value, 4);

    // Creates an overflow chain of length 2, rooted at the second cell on the root page.
    std::string overflow_data(3 * kPageSize / 2, 'x');
    ASSERT_OK(tree->put("a", "value"));
    ASSERT_OK(tree->put("b", overflow_data));

    ASSERT_OK(m_pager->destroy(std::move(node_3.page)));
    ASSERT_OK(m_pager->destroy(std::move(node_4.page)));

    // Page Types:     n   p   2   1   A   B
    // Page Contents: [a] [b] [c] [d] [e] [f]
    // Page IDs:       1   2   3   4   5   6

    // Page Types:     n   p   1   B   A
    // Page Contents: [a] [b] [c] [f] [e] [ ]
    // Page IDs:       1   2   3   4   5   6

    // Page Types:     n   p   A   B
    // Page Contents: [a] [b] [e] [f] [ ] [ ]
    // Page IDs:       1   2   3   4   5   6

    // Page Types:     n   p   A   B
    // Page Contents: [a] [b] [e] [f]
    // Page IDs:       1   2   3   4
    vacuum_and_validate(overflow_data);

    PointerMap::Entry head_entry, tail_entry;
    ASSERT_OK(PointerMap::read_entry(*m_pager, Id(3), head_entry));
    ASSERT_OK(PointerMap::read_entry(*m_pager, Id(4), tail_entry));

    ASSERT_TRUE(head_entry.back_ptr.is_root());
    ASSERT_EQ(tail_entry.back_ptr, Id(3));
    ASSERT_EQ(head_entry.type, PointerMap::kOverflowHead);
    ASSERT_EQ(tail_entry.type, PointerMap::kOverflowLink);
}

TEST_P(VacuumTests, VacuumsOverflowChain_B)
{
    // This time, we'll force the head of the overflow chain to be the last page in the file.
    auto node_3 = allocate_node(true);
    auto node_4 = allocate_node(true);
    auto node_5 = allocate_node(true);
    auto node_6 = allocate_node(true);
    ASSERT_EQ(node_6.page.id().value, 6);
    ASSERT_OK(m_pager->destroy(std::move(node_5.page)));
    ASSERT_OK(m_pager->destroy(std::move(node_6.page)));

    std::string overflow_data(3 * kPageSize / 2, 'x');
    ASSERT_OK(tree->put("a", "value"));
    ASSERT_OK(tree->put("b", overflow_data));

    // Page Types:     n   p   2   1   B   A
    // Page Contents: [a] [b] [c] [d] [e] [f]
    // Page IDs:       1   2   3   4   5   6
    ASSERT_OK(m_pager->destroy(std::move(node_3.page)));
    ASSERT_OK(m_pager->destroy(std::move(node_4.page)));

    // Page Types:     n   p   1   A   B
    // Page Contents: [a] [b] [c] [f] [e] [ ]
    // Page IDs:       1   2   3   4   5   6

    // Page Types:     n   p   B   A
    // Page Contents: [a] [b] [e] [f] [ ] [ ]
    // Page IDs:       1   2   3   4   5   6

    // Page Types:     n   p   B   A
    // Page Contents: [a] [b] [e] [f]
    // Page IDs:       1   2   3   4
    vacuum_and_validate(overflow_data);

    PointerMap::Entry head_entry, tail_entry;
    ASSERT_OK(PointerMap::read_entry(*m_pager, Id(4), head_entry));
    ASSERT_OK(PointerMap::read_entry(*m_pager, Id(3), tail_entry));

    ASSERT_TRUE(head_entry.back_ptr.is_root());
    ASSERT_EQ(tail_entry.back_ptr, Id(4));
    ASSERT_EQ(head_entry.type, PointerMap::kOverflowHead);
    ASSERT_EQ(tail_entry.type, PointerMap::kOverflowLink);
}

TEST_P(VacuumTests, VacuumOverflowChainSanityCheck)
{
    std::vector<Node> reserved;
    reserved.emplace_back(allocate_node(true));
    reserved.emplace_back(allocate_node(true));
    reserved.emplace_back(allocate_node(true));
    reserved.emplace_back(allocate_node(true));
    reserved.emplace_back(allocate_node(true));
    ASSERT_EQ(reserved.back().page.id().value, 7);

    // Create overflow chains, but don't overflow the root node. Should create 3 chains, 1 of length 1, and 2 of length 2.
    std::vector<std::string> values;
    for (std::size_t i = 0; i < 3; ++i) {
        const auto value = random.Generate(kPageSize * std::min<std::size_t>(i + 1, 2) * 2 / 3);
        ASSERT_OK(tree->put(tools::integral_key<1>(i), value));
        values.emplace_back(value.to_string());
    }

    while (!reserved.empty()) {
        ASSERT_OK(m_pager->destroy(std::move(reserved.back().page)));
        reserved.pop_back();
    }

    bool vacuumed;
    ASSERT_EQ(m_pager->page_count(), 12);
    ASSERT_OK(tree->vacuum_one(Id(12), *schema, &vacuumed));
    ASSERT_OK(tree->vacuum_one(Id(11), *schema, &vacuumed));
    ASSERT_OK(tree->vacuum_one(Id(10), *schema, &vacuumed));
    ASSERT_OK(tree->vacuum_one(Id(9), *schema, &vacuumed));
    ASSERT_OK(tree->vacuum_one(Id(8), *schema, &vacuumed));
    m_pager->set_page_count(7);
    ASSERT_EQ(m_pager->page_count(), 7);

    auto *cursor = new CursorImpl(*tree);
    cursor->seek_first();
    for (std::size_t i = 0; i < values.size(); ++i) {
        ASSERT_TRUE(cursor->is_valid());
        ASSERT_EQ(cursor->key().to_string(), tools::integral_key<1>(i));
        ASSERT_EQ(cursor->value().to_string(), values[i]);
        cursor->next();
    }
    ASSERT_FALSE(cursor->is_valid());
    delete cursor;
}

TEST_P(VacuumTests, VacuumsNodes)
{
    auto node_3 = allocate_node(true);
    auto node_4 = allocate_node(true);
    ASSERT_EQ(node_4.page.id().value, 4);

    std::vector<std::string> values;
    for (std::size_t i = 0; i < 5; ++i) {
        const auto key = tools::integral_key(i);
        const auto value = random.Generate(kPageSize / 4 - 40 - key.size());
        ASSERT_OK(tree->put(key, value));
        values.emplace_back(value.to_string());
    }

    // Page Types:     n   p   2   1   n   n
    // Page Contents: [a] [b] [c] [d] [e] [f]
    // Page IDs:       1   2   3   4   5   6
    ASSERT_OK(m_pager->destroy(std::move(node_3.page)));
    ASSERT_OK(m_pager->destroy(std::move(node_4.page)));

    // Page Types:     n   p   1   n   n
    // Page Contents: [a] [b] [c] [f] [e] [ ]
    // Page IDs:       1   2   3   4   5   6

    // Page Types:     n   p   n   n
    // Page Contents: [a] [b] [e] [f] [ ] [ ]
    // Page IDs:       1   2   3   4   5   6

    // Page Types:     n   p   n   n
    // Page Contents: [a] [b] [e] [f]
    // Page IDs:       1   2   3   4
    ASSERT_EQ(m_pager->page_count(), 6)
        << "test was incorrectly initialized (check the key and value sizes)";
    bool vacuumed = false;
    ASSERT_OK(tree->vacuum_one(Id(6), *schema, &vacuumed));
    ASSERT_TRUE(vacuumed);
    ASSERT_OK(tree->vacuum_one(Id(5), *schema, &vacuumed));
    ASSERT_TRUE(vacuumed);
    m_pager->set_page_count(4);

    auto *cursor = new CursorImpl(*tree);
    cursor->seek_first();
    for (std::size_t i = 0; i < values.size(); ++i) {
        ASSERT_TRUE(cursor->is_valid());
        ASSERT_EQ(cursor->key(), tools::integral_key(i));
        ASSERT_EQ(cursor->value(), values[i]);
        cursor->next();
    }
    ASSERT_FALSE(cursor->is_valid());
    delete cursor;
}

TEST_P(VacuumTests, SanityCheck_Freelist)
{
    sanity_check(0, 50, 16, 16);
}

TEST_P(VacuumTests, SanityCheck_Freelist_OverflowHead)
{
    sanity_check(0, 50, 16, kPageSize / 2);
}

TEST_P(VacuumTests, SanityCheck_Freelist_OverflowLink)
{
    sanity_check(0, 50, 16, kPageSize * 2);
}

TEST_P(VacuumTests, SanityCheck_Nodes_1)
{
    sanity_check(50, 50, 16, 16);
}

TEST_P(VacuumTests, SanityCheck_Nodes_2)
{
    sanity_check(200, 50, 16, 16);
}

TEST_P(VacuumTests, SanityCheck_NodesWithOverflowValues)
{
    sanity_check(50, 50, 16, kPageSize * 2);
}

TEST_P(VacuumTests, SanityCheck_NodesWithOverflowKeys)
{
    sanity_check(50, 50, kPageSize * 2, 16);
}

TEST_P(VacuumTests, SanityCheck_NodesWithOverflowPayloads)
{
    sanity_check(50, 50, kPageSize * 2, kPageSize * 2);
}

INSTANTIATE_TEST_SUITE_P(
    VacuumTests,
    VacuumTests,
    ::testing::Values(0));

class MultiTreeTests : public TreeTests
{
public:
    MultiTreeTests()
        : payload_values(kInitialRecordCount)
    {
        for (auto &value : payload_values) {
            value = random.Generate(kPageSize * 2).to_string();
        }
    }

    auto SetUp() -> void override
    {
        TreeTests::SetUp();
    }

    auto create_tree()
    {
        Id root;
        EXPECT_OK(Tree::create(*m_pager, last_tree_id.is_null(), &root));
        ++last_tree_id.value;
        root_ids.emplace_back(root);
        multi_tree.emplace_back(std::make_unique<Tree>(*m_pager, &root_ids.back()));
        return multi_tree.size() - 1;
    }

    auto fill_tree(std::size_t tid)
    {
        for (std::size_t i = 0; i < kInitialRecordCount; ++i) {
            const auto value = payload_values[(i + tid) % payload_values.size()];
            ASSERT_OK(multi_tree[tid]->put(make_long_key(i), value));
        }
        multi_tree[tid]->TEST_validate();
    }

    auto check_tree(std::size_t tid)
    {
        std::string value;
        for (std::size_t i = 0; i < kInitialRecordCount; ++i) {
            ASSERT_OK(multi_tree[tid]->get(make_long_key(i), &value));
            ASSERT_EQ(value, payload_values[(i + tid) % payload_values.size()]);
        }
    }

    auto clear_tree(std::size_t tid)
    {
        for (std::size_t i = 0; i < kInitialRecordCount; ++i) {
            ASSERT_OK(multi_tree[tid]->erase(make_long_key(i)));
        }
        multi_tree[tid]->TEST_validate();
    }

    Id last_tree_id = Id::root();
    std::vector<std::unique_ptr<Tree>> multi_tree;
    std::vector<std::string> payload_values;
    std::list<Id> root_ids;
};

TEST_P(MultiTreeTests, CreateAdditionalTrees)
{
    create_tree();
    create_tree();
    create_tree();
}

TEST_P(MultiTreeTests, DuplicateKeysAreAllowedBetweenTrees)
{
    const auto tid_1 = create_tree();
    const auto tid_2 = create_tree();

    auto &hello_tree = multi_tree[tid_1];
    auto &world_tree = multi_tree[tid_2];
    ASSERT_OK(hello_tree->put("same_key", "hello"));
    ASSERT_OK(world_tree->put("same_key", "world"));

    std::string value;
    ASSERT_OK(hello_tree->get("same_key", &value));
    ASSERT_EQ(value, "hello");
    ASSERT_OK(world_tree->get("same_key", &value));
    ASSERT_EQ(value, "world");
}

TEST_P(MultiTreeTests, NonRootTreeSplitsAndMerges)
{
    const auto tid = create_tree();
    fill_tree(tid);
    clear_tree(tid);
}

TEST_P(MultiTreeTests, MultipleSplitsAndMerges_1)
{
    std::vector<std::size_t> tids(10);
    for (auto &tid : tids) {
        tid = create_tree();
    }
    for (const auto &tid : tids) {
        fill_tree(tid);
    }
    for (const auto &tid : tids) {
        check_tree(tid);
    }
    for (const auto &tid : tids) {
        clear_tree(tid);
    }
}

TEST_P(MultiTreeTests, MultipleSplitsAndMerges_2)
{
    for (std::size_t i = 0; i < 10; ++i) {
        const auto tid = create_tree();
        fill_tree(tid);
        check_tree(tid);
        clear_tree(tid);
    }
}

INSTANTIATE_TEST_SUITE_P(
    MultiTreeTests,
    MultiTreeTests,
    ::testing::Values(0));

template <class T>
class PermutationGenerator
{
    std::vector<T> m_values;
    std::vector<std::size_t> m_indices;

public:
    explicit PermutationGenerator(std::vector<T> values)
        : m_values(std::move(values)),
          m_indices(m_values.size())
    {
        std::iota(begin(m_indices), end(m_indices), 0);
    }

    [[nodiscard]] auto operator()(std::vector<T> &out) -> bool
    {
        if (out.size() != m_values.size()) {
            out.resize(m_values.size());
        }
        const auto not_reset = std::next_permutation(
            begin(m_indices), end(m_indices));
        auto value = begin(out);
        for (const auto &index : m_indices) {
            *value++ = m_values[index];
        }
        return not_reset;
    }
};

TEST(PermutationGeneratorTests, GeneratesAllPermutationsInLexicographicalOrder)
{
    std::vector<int> result;
    PermutationGenerator<int> generator({
        1,
        2,
        3,
    });

    for (int iteration = 0; iteration < 2; ++iteration) {
        ASSERT_TRUE(generator(result));
        ASSERT_EQ((std::vector<int>{1, 3, 2}), result);
        ASSERT_TRUE(generator(result));
        ASSERT_EQ((std::vector<int>{2, 1, 3}), result);
        ASSERT_TRUE(generator(result));
        ASSERT_EQ((std::vector<int>{2, 3, 1}), result);
        ASSERT_TRUE(generator(result));
        ASSERT_EQ((std::vector<int>{3, 1, 2}), result);
        ASSERT_TRUE(generator(result));
        ASSERT_EQ((std::vector<int>{3, 2, 1}), result);
        ASSERT_FALSE(generator(result));
        ASSERT_EQ((std::vector<int>{1, 2, 3}), result);
    }
}

class RebalanceTests : public TreeTests
{
public:
    ~RebalanceTests() override = default;

    struct RecordInfo {
        std::size_t key = 0;
        std::size_t value_size = 0;

        auto operator<(const RecordInfo &rhs) const -> bool
        {
            return key < rhs.key;
        }
    };

    auto run(const std::vector<std::size_t> &sizes) -> void
    {
        std::vector<RecordInfo> info;
        info.reserve(sizes.size());
        for (std::size_t i = 0; i < sizes.size(); ++i) {
            info.emplace_back(RecordInfo{i, sizes[i]});
        }
        PermutationGenerator<RecordInfo> generator(info);
        while (generator(info)) {
            std::size_t iteration = 0;
            for (std::size_t i = 0; i < GetParam(); ++i) {
                for (const auto &[k, value_size] : info) {
                    ASSERT_OK(tree->put(
                        tools::integral_key(iteration * info.size() + k),
                        m_random.Generate(value_size)));
                }
                ++iteration;
            }

            tree->TEST_validate();

            iteration = 0;
            for (std::size_t i = 0; i < GetParam(); ++i) {
                for (const auto &[k, _] : info) {
                    ASSERT_OK(tree->erase(tools::integral_key(iteration * info.size() + k)));
                }
                ++iteration;
            }
        }
    }

protected:
    tools::RandomGenerator m_random;
};

TEST_P(RebalanceTests, A)
{
    run({500, 500, 500, 500, 500, 500});
}

TEST_P(RebalanceTests, B)
{
    run({1'000, 500, 500, 500, 500, 500});
}

TEST_P(RebalanceTests, C)
{
    run({500, 500, 500, 1'000, 1'000, 1'000});
}

TEST_P(RebalanceTests, D)
{
    run({500, 1'000, 1'000, 1'000, 1'000, 1'000});
}

TEST_P(RebalanceTests, E)
{
    run({1'000, 1'000, 1'000, 1'000, 1'000, 1'000});
}

INSTANTIATE_TEST_SUITE_P(
    RebalanceTests,
    RebalanceTests,
    ::testing::Values(1, 2, 5));

} // namespace calicodb