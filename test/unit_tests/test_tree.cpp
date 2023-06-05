// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#include "encoding.h"
#include "fake_env.h"
#include "freelist.h"
#include "logging.h"
#include "schema.h"
#include "tree.h"
#include "unit_tests.h"
#include <gtest/gtest.h>

namespace calicodb
{

static constexpr std::size_t kInitialRecordCount = 100;

class NodeTests
    : public PagerTestHarness<FakeEnv>,
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
        const auto suffix = numeric_key<6>(value);
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
    RandomGenerator random;
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
    : public PagerTestHarness<FakeEnv>,
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
        tree->close_internal_cursor();
        m_pager->finish();
    }

    [[nodiscard]] auto make_long_key(std::size_t value) const
    {
        const auto suffix = numeric_key<6>(value);
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

    auto validate() const -> void
    {
        ASSERT_TRUE(Freelist::assert_state(*m_pager));
        tree->TEST_validate();
    }

    RandomGenerator random;
    std::size_t param;
    std::string collect_scratch;
    std::unique_ptr<Tree> tree;
    Id root_id;
};

TEST_P(TreeTests, ConstructsAndDestructs)
{
    validate();
}

TEST_P(TreeTests, KeysAreUnique)
{
    bool exists;
    ASSERT_OK(tree->put("a", make_value('x')));
    ASSERT_OK(tree->put("b", make_value('2')));
    ASSERT_OK(tree->put("a", make_value('1')));

    validate();

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

TEST_P(TreeTests, LongVsShortKeys)
{
    for (int i = 0; i < 2; ++i) {
        const auto tree_key_len = i == 0 ? 1 : kPageSize * 2 - 1;
        const auto search_key_len = kPageSize * 2 - tree_key_len;
        ASSERT_OK(tree->put(std::string(tree_key_len, 'a'), make_value('1', true)));
        ASSERT_OK(tree->put(std::string(tree_key_len, 'b'), make_value('2', true)));
        ASSERT_OK(tree->put(std::string(tree_key_len, 'c'), make_value('3', true)));

        auto *c = new CursorImpl(*tree);
        c->seek(std::string(search_key_len, i == 0 ? 'A' : 'a'));
        ASSERT_TRUE(c->is_valid());
        ASSERT_EQ(std::string(tree_key_len, 'a'), c->key());
        ASSERT_EQ(make_value('1', true), c->value());
        c->seek(std::string(search_key_len, i == 0 ? 'a' : 'b'));
        ASSERT_TRUE(c->is_valid());
        ASSERT_EQ(std::string(tree_key_len, 'b'), c->key());
        ASSERT_EQ(make_value('2', true), c->value());
        c->seek(std::string(search_key_len, i == 0 ? 'b' : 'c'));
        ASSERT_TRUE(c->is_valid());
        ASSERT_EQ(std::string(tree_key_len, 'c'), c->key());
        ASSERT_EQ(make_value('3', true), c->value());
        delete c;

        ASSERT_OK(tree->erase(std::string(tree_key_len, 'a')));
        ASSERT_OK(tree->erase(std::string(tree_key_len, 'b')));
        ASSERT_OK(tree->erase(std::string(tree_key_len, 'c')));
    }
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
    validate();
}

TEST_P(TreeTests, ResolvesOverflowsOnRightmostPosition)
{
    for (std::size_t i = 0; i < 100; ++i) {
        ASSERT_OK(tree->put(make_long_key(i), make_value('v')));
    }
    validate();
}

TEST_P(TreeTests, ResolvesOverflowsOnMiddlePosition)
{
    for (std::size_t i = 0, j = 99; i < j; ++i, --j) {
        ASSERT_OK(tree->put(make_long_key(i), make_value('v')));
        ASSERT_OK(tree->put(make_long_key(j), make_value('v')));
    }
    validate();
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
    validate();
}

TEST_P(TreeTests, ResolvesUnderflowsOnLeftmostPosition)
{
    add_initial_records(*this);
    for (std::size_t i = 0; i < kInitialRecordCount; ++i) {
        ASSERT_OK(tree->erase(make_long_key(i)));
    }
    validate();
}

TEST_P(TreeTests, ResolvesUnderflowsOnMiddlePosition)
{
    add_initial_records(*this);
    for (std::size_t i{}, j{kInitialRecordCount - 1}; i < j; ++i, --j) {
        ASSERT_OK(tree->erase(make_long_key(i)));
        ASSERT_OK(tree->erase(make_long_key(j)));
    }
    validate();
}

TEST_P(TreeTests, ResolvesOverflowsFromOverwrite)
{
    add_initial_records(*this);
    // Replace the small values with very large ones.
    add_initial_records(*this, true);
    validate();
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
    validate();
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
        return {std::string(key), std::string(val)};
    }

    const bool overflow_keys = GetParam() & 0b10;
    const bool overflow_values = GetParam() & 0b01;
    const std::size_t record_count =
        kInitialRecordCount * 5 +
        kInitialRecordCount * 5 * !overflow_keys +
        kInitialRecordCount * 5 * !overflow_values;
};

TEST_P(TreeSanityChecks, Insert)
{
    for (std::size_t i = 0; i < record_count; ++i) {
        random_write();
    }
    validate();
}

TEST_P(TreeSanityChecks, Search)
{
    std::unordered_map<std::string, std::string> records;
    for (std::size_t i = 0; i < record_count; ++i) {
        const auto [k, v] = random_write();
        records[k] = v;
    }
    validate();

    for (const auto &[key, value] : records) {
        std::string result;
        ASSERT_OK(tree->get(key, &result));
        ASSERT_EQ(result, value);

        ASSERT_OK(tree->erase(key));
        ASSERT_TRUE(tree->get(key, &result).is_not_found());
    }
}

TEST_P(TreeSanityChecks, Erase)
{
    std::unordered_map<std::string, std::string> records;
    for (std::size_t iteration = 0; iteration < 3; ++iteration) {
        for (std::size_t i = 0; i < record_count; ++i) {
            const auto [k, v] = random_write();
            records[k] = v;
        }

        std::size_t i = 0;
        for (const auto &[key, value] : records) {
            ASSERT_OK(tree->erase(key));
        }
        validate();
        records.clear();
    }
}

TEST_P(TreeSanityChecks, SmallRecords)
{
    std::unordered_map<std::string, std::string> records;
    for (std::size_t iteration = 0; iteration < 3; ++iteration) {
        for (std::size_t i = 0; i < record_count * 10; ++i) {
            const auto key = numeric_key<6>(i);
            ASSERT_OK(tree->put(key, ""));
            records[key] = "";
        }

        std::size_t i = 0;
        for (const auto &[key, value] : records) {
            ASSERT_OK(tree->erase(key));
        }
        validate();
        records.clear();
    }
}

TEST_P(TreeSanityChecks, Destruction)
{
    for (std::size_t i = 0; i < record_count; ++i) {
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
    ASSERT_OK(PointerMap::write_entry(*m_pager, Id(4), PointerMap::Entry{Id(44), PointerMap::kFreelistLeaf}));
    ASSERT_OK(PointerMap::write_entry(*m_pager, Id(5), PointerMap::Entry{Id(55), PointerMap::kOverflowLink}));

    PointerMap::Entry entry_1, entry_2, entry_3;
    ASSERT_OK(PointerMap::read_entry(*m_pager, Id(3), entry_1));
    ASSERT_OK(PointerMap::read_entry(*m_pager, Id(4), entry_2));
    ASSERT_OK(PointerMap::read_entry(*m_pager, Id(5), entry_3));

    ASSERT_EQ(entry_1.back_ptr.value, 33);
    ASSERT_EQ(entry_2.back_ptr.value, 44);
    ASSERT_EQ(entry_3.back_ptr.value, 55);
    ASSERT_EQ(entry_1.type, PointerMap::kTreeNode);
    ASSERT_EQ(entry_2.type, PointerMap::kFreelistLeaf);
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

class MultiTreeTests : public TreeTests
{
public:
    MultiTreeTests()
        : payload_values(kInitialRecordCount)
    {
        for (auto &value : payload_values) {
            value = random.Generate(kPageSize * 2);
        }
    }

    auto SetUp() -> void override
    {
        TreeTests::SetUp();
    }

    auto TearDown() -> void override
    {
        for (auto &tree : multi_tree) {
            tree->close_internal_cursor();
        }
        TreeTests::TearDown();
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
                        numeric_key(iteration * info.size() + k),
                        m_random.Generate(value_size)));
                }
                ++iteration;
            }

            validate();

            iteration = 0;
            for (std::size_t i = 0; i < GetParam(); ++i) {
                for (const auto &[k, _] : info) {
                    ASSERT_OK(tree->erase(numeric_key(iteration * info.size() + k)));
                }
                ++iteration;
            }
        }
    }

protected:
    RandomGenerator m_random;
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