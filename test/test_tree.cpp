// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#include "common.h"
#include "encoding.h"
#include "fake_env.h"
#include "freelist.h"
#include "logging.h"
#include "schema.h"
#include "test.h"
#include "tree.h"
#include <gtest/gtest.h>

namespace calicodb::test
{

static constexpr std::size_t kInitialRecordCount = 100;

class TreeTestHarness
{
public:
    FakeEnv *m_env;
    std::string m_scratch;
    Status m_status;
    Pager *m_pager = nullptr;
    File *m_file = nullptr;
    Tree *m_tree = nullptr;

    TreeTestHarness()
        : m_env(new FakeEnv),
          m_scratch(kPageSize * 2, '\0')
    {
        EXPECT_OK(m_env->new_file("db", Env::kCreate, m_file));

        const Pager::Parameters pager_param = {
            "db",
            "wal",
            m_file,
            m_env,
            nullptr,
            &m_status,
            nullptr,
            kMinFrameCount * 2,
            Options::kSyncNormal,
            Options::kLockNormal,
        };

        EXPECT_OK(Pager::open(pager_param, m_pager));
        EXPECT_OK(m_pager->start_reader());
        EXPECT_OK(m_pager->start_writer());
        m_pager->initialize_root();
        EXPECT_OK(m_pager->commit());
        m_pager->finish();
    }

    ~TreeTestHarness()
    {
        delete m_tree;
        delete m_pager;
        delete m_file;
        delete m_env;
    }

    [[nodiscard]] static auto make_long_key(std::size_t value)
    {
        const auto suffix = numeric_key<6>(value);
        const std::string key(kPageSize * 2 - suffix.size(), '0');
        return key + suffix;
    }

    [[nodiscard]] static auto make_value(char c, bool overflow = false)
    {
        auto size = kPageSize;
        if (overflow) {
            size /= 3;
        } else {
            size /= 20;
        }
        return std::string(size, c);
    }

    auto open() -> void
    {
        EXPECT_OK(m_pager->start_reader());
        EXPECT_OK(m_pager->start_writer());
        m_tree = new Tree(*m_pager, m_scratch.data(), nullptr);
    }

    auto close() const -> void
    {
        m_pager->finish();
    }

    auto validate() const -> void
    {
        ASSERT_TRUE(Freelist::assert_state(*m_pager));
        m_tree->finish_operation();
        m_tree->TEST_validate();
    }
};

class TreeTests
    : public TreeTestHarness,
      public testing::Test
{
public:
    ~TreeTests() override = default;

    auto SetUp() -> void override
    {
        open();
    }
    auto TearDown() -> void override
    {
        close();
    }

    RandomGenerator random;
};

TEST_F(TreeTests, ConstructsAndDestructs)
{
    validate();
}

TEST_F(TreeTests, KeysAreUnique)
{
    bool exists;
    ASSERT_OK(m_tree->put("a", make_value('x')));
    ASSERT_OK(m_tree->put("b", make_value('2')));
    ASSERT_OK(m_tree->put("a", make_value('1')));

    validate();

    std::string value;
    ASSERT_OK(m_tree->get("a", &value));
    ASSERT_EQ(value, make_value('1'));
    ASSERT_OK(m_tree->get("b", &value));
    ASSERT_EQ(value, make_value('2'));
}

static constexpr auto kMaxLocalSize =
    kMaxCellHeaderSize +
    compute_local_pl_size(kPageSize, 0) +
    sizeof(U16); // Indirection vector entry

TEST_F(TreeTests, RootFitsAtLeast3Cells)
{
    Node root;
    ASSERT_OK(m_tree->acquire(Id(1), false, root));
    ASSERT_GE(root.usable_space, 3 * kMaxLocalSize);
    ASSERT_LT(root.usable_space, 4 * kMaxLocalSize);
    m_tree->release(std::move(root));
}

TEST_F(TreeTests, NonRootFitsAtLeast4Cells)
{
    Node nonroot;
    ASSERT_OK(m_tree->allocate(true, nonroot));
    ASSERT_GE(nonroot.usable_space, 4 * kMaxLocalSize);
    ASSERT_LT(nonroot.usable_space, 5 * kMaxLocalSize);
    m_tree->release(std::move(nonroot));
}

TEST_F(TreeTests, RecordsAreErased)
{
    (void)m_tree->put("a", make_value('1'));
    ASSERT_OK(m_tree->erase("a"));
    std::string value;
    ASSERT_TRUE(m_tree->get("a", &value).is_not_found());
    ASSERT_OK(m_tree->erase("a"));
}

TEST_F(TreeTests, HandlesLargePayloads)
{
    ASSERT_OK(m_tree->put(make_long_key('a'), "1"));
    ASSERT_OK(m_tree->put("b", make_value('2', true)));
    ASSERT_OK(m_tree->put(make_long_key('c'), make_value('3', true)));

    std::string value;
    ASSERT_OK(m_tree->get(make_long_key('a'), &value));
    ASSERT_EQ(value, "1");
    ASSERT_OK(m_tree->get("b", &value));
    ASSERT_EQ(value, make_value('2', true));
    ASSERT_OK(m_tree->get(make_long_key('c'), &value));
    ASSERT_EQ(value, make_value('3', true));

    ASSERT_OK(m_tree->erase(make_long_key('a')));
    ASSERT_OK(m_tree->erase("b"));
    ASSERT_OK(m_tree->erase(make_long_key('c')));
}

TEST_F(TreeTests, LongVsShortKeys)
{
    for (int i = 0; i < 2; ++i) {
        const auto tree_key_len = i == 0 ? 1 : kPageSize * 2 - 1;
        const auto search_key_len = kPageSize * 2 - tree_key_len;
        ASSERT_OK(m_tree->put(std::string(tree_key_len, 'a'), make_value('1', true)));
        ASSERT_OK(m_tree->put(std::string(tree_key_len, 'b'), make_value('2', true)));
        ASSERT_OK(m_tree->put(std::string(tree_key_len, 'c'), make_value('3', true)));

        auto *c = new CursorImpl(*m_tree, nullptr);
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

        ASSERT_OK(m_tree->erase(std::string(tree_key_len, 'a')));
        ASSERT_OK(m_tree->erase(std::string(tree_key_len, 'b')));
        ASSERT_OK(m_tree->erase(std::string(tree_key_len, 'c')));
    }
}

TEST_F(TreeTests, GetNonexistentKeys)
{
    // Missing 0
    ASSERT_OK(m_tree->put(make_long_key(1), make_value('0', true)));
    // Missing 2
    ASSERT_OK(m_tree->put(make_long_key(3), make_value('0', true)));
    ASSERT_OK(m_tree->put(make_long_key(4), make_value('0', true)));
    ASSERT_OK(m_tree->put(make_long_key(5), make_value('0', true)));

    // Missing 6
    ASSERT_OK(m_tree->put(make_long_key(7), make_value('0', true)));
    ASSERT_OK(m_tree->put(make_long_key(8), make_value('0', true)));
    ASSERT_OK(m_tree->put(make_long_key(9), make_value('0', true)));
    // Missing 10

    m_tree->TEST_validate();

    ASSERT_NOK(m_tree->get(make_long_key(0), nullptr));
    ASSERT_NOK(m_tree->get(make_long_key(2), nullptr));
    ASSERT_NOK(m_tree->get(make_long_key(6), nullptr));
    ASSERT_NOK(m_tree->get(make_long_key(10), nullptr));

    ASSERT_OK(m_tree->get(make_long_key(1), nullptr));
    ASSERT_OK(m_tree->get(make_long_key(3), nullptr));
    ASSERT_OK(m_tree->get(make_long_key(5), nullptr));
    ASSERT_OK(m_tree->get(make_long_key(7), nullptr));
    ASSERT_OK(m_tree->get(make_long_key(9), nullptr));
}

TEST_F(TreeTests, ResolvesOverflowsOnLeftmostPosition)
{
    for (std::size_t i = 0; i < 100; ++i) {
        ASSERT_OK(m_tree->put(make_long_key(99 - i), make_value('v', true)));
    }
    validate();
}

TEST_F(TreeTests, ResolvesOverflowsOnRightmostPosition)
{
    for (std::size_t i = 0; i < 100; ++i) {
        ASSERT_OK(m_tree->put(make_long_key(i), make_value('v')));
    }
    validate();
}

TEST_F(TreeTests, ResolvesOverflowsOnMiddlePosition)
{
    for (std::size_t i = 0, j = 99; i < j; ++i, --j) {
        ASSERT_OK(m_tree->put(make_long_key(i), make_value('v')));
        ASSERT_OK(m_tree->put(make_long_key(j), make_value('v')));
    }
    validate();
}

static auto add_initial_records(TreeTestHarness &test, bool has_overflow = false)
{
    for (std::size_t i = 0; i < kInitialRecordCount; ++i) {
        (void)test.m_tree->put(TreeTestHarness::make_long_key(i), TreeTestHarness::make_value('v', has_overflow));
    }
    test.validate();
    test.m_tree->finish_operation();
}

TEST_F(TreeTests, ToStringDoesNotCrash)
{
    add_initial_records(*this);
    (void)m_tree->TEST_to_string();
}

TEST_F(TreeTests, ResolvesUnderflowsOnRightmostPosition)
{
    add_initial_records(*this);
    validate();

    for (std::size_t i = 0; i < kInitialRecordCount; ++i) {
        ASSERT_OK(m_tree->erase(make_long_key(kInitialRecordCount - i - 1)));
    }
    validate();
}

TEST_F(TreeTests, ResolvesUnderflowsOnLeftmostPosition)
{
    add_initial_records(*this);
    for (std::size_t i = 0; i < kInitialRecordCount; ++i) {
        ASSERT_OK(m_tree->erase(make_long_key(i)));
    }
    validate();
}

TEST_F(TreeTests, ResolvesUnderflowsOnMiddlePosition)
{
    add_initial_records(*this);
    for (std::size_t i = 0, j = kInitialRecordCount - 1; i < j; ++i, --j) {
        ASSERT_OK(m_tree->erase(make_long_key(i)));
        ASSERT_OK(m_tree->erase(make_long_key(j)));
    }
    validate();
}

TEST_F(TreeTests, ResolvesOverflowsFromOverwrite)
{
    add_initial_records(*this);
    // Replace the small values with very large ones.
    add_initial_records(*this, true);
    validate();
}

TEST_F(TreeTests, SplitWithShortAndLongKeys)
{
    for (unsigned i = 0; i < kInitialRecordCount; ++i) {
        char key[3]{};
        put_u16(key, static_cast<U16>(kInitialRecordCount - i - 1));
        ASSERT_OK(m_tree->put({key, 2}, "v"));
    }
    for (unsigned i = 0; i < kInitialRecordCount; ++i) {
        const auto key = random.Generate(kPageSize);
        ASSERT_OK(m_tree->put(key, "v"));
    }
    validate();
}

TEST_F(TreeTests, EmptyKeyBehavior)
{
    ASSERT_TRUE(m_tree->put("", "").is_invalid_argument());
    ASSERT_TRUE(m_tree->get("", nullptr).is_not_found());
    ASSERT_OK(m_tree->erase(""));
}

class TreeSanityChecks
    : public TreeTestHarness,
      public testing::TestWithParam<U32>
{
public:
    auto SetUp() -> void override
    {
        open();
    }
    auto TearDown() -> void override
    {
        close();
    }

    auto random_chunk(bool overflow, bool nonzero = true)
    {
        return random.Generate(random.Next(nonzero, kPageSize * overflow + 12));
    }

    auto random_write() -> std::pair<std::string, std::string>
    {
        const auto key = random_chunk(overflow_keys);
        const auto val = random_chunk(overflow_values, false);
        EXPECT_OK(m_tree->put(key, val));
        return {std::string(key), std::string(val)};
    }

    const bool overflow_keys = GetParam() & 0b10;
    const bool overflow_values = GetParam() & 0b01;
    const std::size_t record_count =
        kInitialRecordCount * 5 +
        kInitialRecordCount * 5 * !overflow_keys +
        kInitialRecordCount * 5 * !overflow_values;
    RandomGenerator random;
};

TEST_P(TreeSanityChecks, ReadAndWrite)
{
    std::unordered_map<std::string, std::string> records;
    for (std::size_t i = 0; i < record_count; ++i) {
        const auto [k, v] = random_write();
        records[k] = v;
    }
    validate();

    for (const auto &[key, value] : records) {
        std::string result;
        ASSERT_OK(m_tree->get(key, &result));
        ASSERT_EQ(result, value);
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

        for (const auto &[key, value] : records) {
            ASSERT_OK(m_tree->erase(key));
        }
        records.clear();
    }
}

TEST_P(TreeSanityChecks, SmallRecords)
{
    std::unordered_map<std::string, std::string> records;
    for (std::size_t iteration = 0; iteration < 3; ++iteration) {
        for (std::size_t i = 0; i < record_count * 10; ++i) {
            const auto key = numeric_key<6>(i);
            ASSERT_OK(m_tree->put(key, ""));
            records[key] = "";
        }

        std::size_t i = 0;
        for (const auto &[key, value] : records) {
            ASSERT_OK(m_tree->erase(key));
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
    ASSERT_OK(Tree::destroy(*m_tree));
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
};

TEST_F(EmptyTreeCursorTests, KeyAndValueUseSeparateMemory)
{
    std::unique_ptr<Cursor> cursor(new CursorImpl(*m_tree, nullptr));
    cursor->seek_first();
    ASSERT_FALSE(cursor->is_valid());
    cursor->seek_last();
    ASSERT_FALSE(cursor->is_valid());
    cursor->seek("42");
    ASSERT_FALSE(cursor->is_valid());
}

class CursorTests
    : public TreeTestHarness,
      public testing::TestWithParam<U32>
{
protected:
    ~CursorTests() override = default;
    auto SetUp() -> void override
    {
        open();
        add_initial_records(*this);
    }

    auto make_cursor() -> std::unique_ptr<Cursor>
    {
        switch (GetParam()) {
            case 0:
                return std::make_unique<CursorImpl>(*m_tree, nullptr);
            case 1:
                return std::make_unique<SchemaCursor>(*m_tree);
        }
        return nullptr;
    }

    RandomGenerator random;
};

TEST_P(CursorTests, AccountsForNodeBoundaries)
{
    for (std::size_t i = 0; i + 5 < kInitialRecordCount; i += 5) {
        ASSERT_OK(m_tree->erase(make_long_key(i + 1)));
        ASSERT_OK(m_tree->erase(make_long_key(i + 2)));
        ASSERT_OK(m_tree->erase(make_long_key(i + 3)));
        ASSERT_OK(m_tree->erase(make_long_key(i + 4)));
    }
    m_tree->finish_operation();
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
    ASSERT_OK(m_tree->erase(make_long_key(0)));
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
    ASSERT_DEATH((void)cursor->key(), "expect");
    ASSERT_DEATH((void)cursor->value(), "expect");
    ASSERT_DEATH((void)cursor->next(), "expect");
    ASSERT_DEATH((void)cursor->previous(), "expect");
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
        return kPageSize / (sizeof(char) + sizeof(U32));
    }
};

TEST_F(PointerMapTests, FirstPointerMapIsPage2)
{
    ASSERT_EQ(PointerMap::lookup(Id(1)), Id(0));
    ASSERT_EQ(PointerMap::lookup(Id(2)), Id(2));
    ASSERT_EQ(PointerMap::lookup(Id(3)), Id(2));
    ASSERT_EQ(PointerMap::lookup(Id(4)), Id(2));
    ASSERT_EQ(PointerMap::lookup(Id(5)), Id(2));
}

TEST_F(PointerMapTests, ReadsAndWritesEntries)
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

TEST_F(PointerMapTests, PointerMapCanFitAllPointers)
{
    // PointerMap::find_map() expects the given pointer map page to be allocated already.
    for (std::size_t i = 0; i < map_size() * 2; ++i) {
        PageRef *page;
        ASSERT_OK(m_pager->allocate(page));
        m_pager->release(page);
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

TEST_F(PointerMapTests, MapPagesAreRecognized)
{
    Id id(2);
    ASSERT_EQ(PointerMap::lookup(id), id);

    // Back pointers for the next "map.map_size()" pages are stored on page 2. The next pointermap page is
    // the page following the last page whose back pointer is on page 2. This pattern continues forever.
    for (std::size_t i = 0; i < 1'000'000; ++i) {
        id.value += static_cast<U32>(map_size() + 1);
        ASSERT_EQ(PointerMap::lookup(id), id);
    }
}

TEST_F(PointerMapTests, FindsCorrectMapPages)
{
    std::size_t counter = 0;
    Id map_id(2);

    for (Id page_id(3); page_id.value <= 100 * map_size(); ++page_id.value) {
        if (counter++ == map_size()) {
            // Found a map page. Calls to find() with a page ID between this page and the next map page
            // should map to this page ID.
            map_id.value += static_cast<U32>(map_size() + 1);
            counter = 0;
        } else {
            ASSERT_EQ(PointerMap::lookup(page_id), map_id);
        }
    }
}

#ifndef NDEBUG
TEST_F(PointerMapTests, LookupNullIdDeathTest)
{
    ASSERT_DEATH((void)PointerMap::lookup(Id(0)), "expect");
    ASSERT_DEATH((void)PointerMap::is_map(Id(0)), "expect");
}
#endif // NDEBUG

class MultiTreeTests : public TreeTests
{
public:
    MultiTreeTests()
        : payload_values(kInitialRecordCount),
          m_scratch(kPageSize * 2, '\0')
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
        multi_tree.clear();
        TreeTests::TearDown();
    }

    auto create_tree()
    {
        Id root;
        EXPECT_OK(Tree::create(*m_pager, &root));
        ++last_tree_id.value;
        root_ids.emplace_back(root);
        multi_tree.emplace_back(std::make_unique<Tree>(*m_pager, m_scratch.data(), &root_ids.back()));
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
        multi_tree[tid]->finish_operation();
        multi_tree[tid]->TEST_validate();
    }

    Id last_tree_id = Id::root();
    std::vector<std::unique_ptr<Tree>> multi_tree;
    std::vector<std::string> payload_values;
    std::list<Id> root_ids;
    std::string m_scratch;
};

TEST_F(MultiTreeTests, CreateAdditionalTrees)
{
    create_tree();
    create_tree();
    create_tree();
}

TEST_F(MultiTreeTests, DuplicateKeysAreAllowedBetweenTrees)
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

TEST_F(MultiTreeTests, NonRootTreeSplitsAndMerges)
{
    const auto tid = create_tree();
    fill_tree(tid);
    clear_tree(tid);
}

TEST_F(MultiTreeTests, MultipleSplitsAndMerges_1)
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

TEST_F(MultiTreeTests, MultipleSplitsAndMerges_2)
{
    for (std::size_t i = 0; i < 10; ++i) {
        const auto tid = create_tree();
        fill_tree(tid);
        check_tree(tid);
        clear_tree(tid);
    }
}

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

class RebalanceTests
    : public TreeTestHarness,
      public testing::TestWithParam<U32>
{
public:
    static constexpr std::size_t kValueSizes[] = {10, 100, 500, kPageSize};
    ~RebalanceTests() override = default;

    auto SetUp() -> void override
    {
        open();
    }
    auto TearDown() -> void override
    {
        close();
    }

    struct RecordInfo {
        std::size_t key = 0;
        std::size_t value_size = 0;

        auto operator<(const RecordInfo &rhs) const -> bool
        {
            return key < rhs.key;
        }
    };

    auto run(const std::vector<std::size_t> &size_idx) -> void
    {
        std::vector<RecordInfo> info;
        info.reserve(size_idx.size());
        for (std::size_t i = 0; i < size_idx.size(); ++i) {
            info.emplace_back(RecordInfo{i, kValueSizes[size_idx[i]]});
        }
        PermutationGenerator<RecordInfo> generator(info);
        while (generator(info)) {
            std::size_t iteration = 0;
            for (std::size_t i = 0; i < GetParam(); ++i) {
                for (const auto &[k, value_size] : info) {
                    ASSERT_OK(m_tree->put(
                        numeric_key(iteration * info.size() + k),
                        m_random.Generate(value_size)));
                }
                ++iteration;
            }
            validate();

            iteration = 0;
            for (std::size_t i = 0; i < GetParam(); ++i) {
                for (const auto &[k, _] : info) {
                    ASSERT_OK(m_tree->erase(numeric_key(iteration * info.size() + k)));
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
    run({0, 0, 0, 0, 0, 3});
    run({1, 1, 1, 1, 1, 3});
    run({2, 2, 2, 2, 2, 3});
}

TEST_P(RebalanceTests, B)
{
    run({0, 0, 0, 0, 3, 3});
    run({1, 1, 1, 1, 3, 3});
    run({2, 2, 2, 2, 3, 3});
}

TEST_P(RebalanceTests, C)
{
    run({0, 0, 0, 3, 3, 3});
    run({1, 1, 1, 3, 3, 3});
    run({2, 2, 2, 3, 3, 3});
}

TEST_P(RebalanceTests, D)
{
    run({0, 0, 3, 3, 3, 3});
    run({1, 1, 3, 3, 3, 3});
    run({2, 2, 3, 3, 3, 3});
}

TEST_P(RebalanceTests, E)
{
    run({0, 3, 3, 3, 3, 3});
    run({1, 3, 3, 3, 3, 3});
    run({2, 3, 3, 3, 3, 3});
}

INSTANTIATE_TEST_SUITE_P(
    RebalanceTests,
    RebalanceTests,
    ::testing::Values(1, 2, 5));

} // namespace calicodb::test