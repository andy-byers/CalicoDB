// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#include "common.h"
#include "encoding.h"
#include "freelist.h"
#include "schema.h"
#include "temp.h"
#include "test.h"
#include "tree.h"

namespace calicodb::test
{

static constexpr std::size_t kInitialRecordCount = 1'000;

class TreeTestHarness
{
public:
    Env *m_env;
    std::string m_scratch;
    Status m_status;
    Stat m_stat;
    Pager *m_pager = nullptr;
    File *m_file = nullptr;
    Tree *m_tree = nullptr;

    TreeTestHarness()
        : m_env(new_temp_env()),
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
            &m_stat,
            nullptr,
            kMinFrameCount * 5,
            Options::kSyncNormal,
            Options::kLockNormal,
            false,
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

    [[nodiscard]] static auto make_normal_key(std::size_t value)
    {
        return numeric_key<6>(value);
    }

    [[nodiscard]] static auto make_long_key(std::size_t value)
    {
        const auto suffix = make_normal_key(value);
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
        m_tree = new Tree(*m_pager, m_stat, m_scratch.data(), nullptr);
    }

    auto close() const -> void
    {
        m_tree->release_nodes();
        m_pager->finish();
    }

    auto validate() const -> void
    {
        m_tree->release_nodes();
        m_tree->TEST_validate();
        ASSERT_TRUE(Freelist::assert_state(*m_pager));
        m_pager->assert_state();
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
    ASSERT_OK(m_tree->acquire(Id(1), root, false));
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

        auto *c = m_tree->new_cursor();
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
        ASSERT_OK(m_tree->put(make_long_key(99 - i), make_value('*', true)));
    }
    validate();
}

TEST_F(TreeTests, ResolvesOverflowsOnRightmostPosition)
{
    for (std::size_t i = 0; i < 100; ++i) {
        ASSERT_OK(m_tree->put(make_long_key(i), make_value('*')));
    }
    validate();
}

TEST_F(TreeTests, ResolvesOverflowsOnMiddlePosition)
{
    for (std::size_t i = 0, j = 99; i < j; ++i, --j) {
        ASSERT_OK(m_tree->put(make_long_key(i), make_value('*')));
        ASSERT_OK(m_tree->put(make_long_key(j), make_value('*')));
    }
    validate();
}

using InitFlag = U32;
static constexpr InitFlag kInitNormal = 0;
static constexpr InitFlag kInitLongKeys = 1;
static constexpr InitFlag kInitLongValues = 2;
static constexpr InitFlag kInitFlagMax = 3;
static auto init_tree(TreeTestHarness &test, InitFlag flags = kInitNormal)
{
    for (std::size_t i = 0; i < kInitialRecordCount; ++i) {
        ASSERT_OK(test.m_tree->put(
            flags & kInitLongKeys ? TreeTestHarness::make_long_key(i)
                                  : TreeTestHarness::make_normal_key(i),
            TreeTestHarness::make_value('*', flags & kInitLongValues)));
    }
    test.validate();
    test.m_tree->release_nodes();
}

TEST_F(TreeTests, PrintStructure)
{
    std::string empty, normal_keys, long_keys;
    ASSERT_OK(m_tree->print_structure(empty));
    init_tree(*this, kInitNormal);
    ASSERT_OK(m_tree->print_structure(normal_keys));
    init_tree(*this, kInitLongKeys);
    ASSERT_OK(m_tree->print_structure(long_keys));
    // empty may or may not be empty...
    ASSERT_FALSE(normal_keys.empty());
    ASSERT_FALSE(long_keys.empty());
}

TEST_F(TreeTests, PrintRecords)
{
    std::string empty, normal_keys, long_keys;
    ASSERT_OK(m_tree->print_nodes(empty));
    init_tree(*this, kInitNormal);
    ASSERT_OK(m_tree->print_nodes(normal_keys));
    init_tree(*this, kInitLongKeys);
    ASSERT_OK(m_tree->print_nodes(long_keys));
    // empty may or may not be empty...
    ASSERT_FALSE(normal_keys.empty());
    ASSERT_FALSE(long_keys.empty());
}

TEST_F(TreeTests, ResolvesUnderflowsOnRightmostPosition)
{
    init_tree(*this);
    validate();

    for (std::size_t i = 0; i < kInitialRecordCount; ++i) {
        ASSERT_OK(m_tree->erase(make_long_key(kInitialRecordCount - i - 1)));
    }
    validate();
}

TEST_F(TreeTests, ResolvesUnderflowsOnLeftmostPosition)
{
    init_tree(*this);
    for (std::size_t i = 0; i < kInitialRecordCount; ++i) {
        ASSERT_OK(m_tree->erase(make_long_key(i)));
    }
    validate();
}

TEST_F(TreeTests, ResolvesUnderflowsOnMiddlePosition)
{
    init_tree(*this);
    for (std::size_t i = 0, j = kInitialRecordCount - 1; i < j; ++i, --j) {
        ASSERT_OK(m_tree->erase(make_long_key(i)));
        ASSERT_OK(m_tree->erase(make_long_key(j)));
    }
    validate();
}

TEST_F(TreeTests, ResolvesOverflowsFromOverwrite)
{
    init_tree(*this);
    // Replace the small values with very large ones.
    init_tree(*this, kInitLongValues);
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

TEST_F(TreeTests, AllowsEmptyKey)
{
    for (InitFlag flag = kInitNormal; flag <= kInitFlagMax; ++flag) {
        std::string value;
        ASSERT_OK(m_tree->put("", "value"));
        init_tree(*this, flag);
        ASSERT_OK(m_tree->get("", &value));
        ASSERT_OK(m_tree->erase(""));
        ASSERT_EQ("value", value);
    }
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
        validate();
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
        validate();

        for (const auto &[key, value] : records) {
            ASSERT_OK(m_tree->erase(key));
        }
        records.clear();
        validate();
    }
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

TEST_F(EmptyTreeCursorTests, EmptyTreeBehavior)
{
    std::unique_ptr<Cursor> cursor(m_tree->new_cursor());
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
    ~CursorTests() override
    {
        m_schema->close();
        delete m_schema;
    }
    auto SetUp() -> void override
    {
        open();
        m_schema = new Schema(*m_pager, m_status, m_stat, m_scratch.data());
        init_tree(*this, kInitLongKeys);
    }

    auto make_cursor() -> std::unique_ptr<Cursor>
    {
        switch (GetParam()) {
            case 0:
                return std::unique_ptr<Cursor>(m_tree->new_cursor());
            case 1:
                return std::unique_ptr<Cursor>(m_schema->new_cursor());
        }
        return nullptr;
    }

    Schema *m_schema = nullptr;
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
    m_tree->release_nodes();
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
        ASSERT_EQ(cursor->value(), make_value('*'));
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
        ASSERT_EQ(cursor->key(), make_long_key(kInitialRecordCount - 1 - i++));
        ASSERT_EQ(cursor->value(), make_value('*'));
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

class MultiCursorTests : public TreeTests
{
protected:
    auto SetUp() -> void override
    {
        TreeTests::SetUp();
        init_tree(*this, kInitLongValues);
    }

    auto TearDown() -> void override
    {
        while (!m_cursors.empty()) {
            del_cursor(0);
        }
        TreeTests::TearDown();
    }

    auto add_cursor() -> Cursor *
    {
        auto *c = m_tree->new_cursor();
        m_cursors.emplace_back(c);
        return c;
    }

    auto del_cursor(std::size_t idx) -> void
    {
        delete m_cursors.at(idx);
        m_cursors.erase(begin(m_cursors) + static_cast<std::ptrdiff_t>(idx));
    }

    std::vector<Cursor *> m_cursors;
};

TEST_F(MultiCursorTests, CursorIsUnaffectedByModifications)
{
    auto cursor = add_cursor();

    cursor->seek_first();
    ASSERT_TRUE(cursor->is_valid());

    const auto v0 = cursor->value().to_string();
    // Modifying the tree causes cursors to be "saved". key() and value() can still be called,
    // and is_valid() will return true given that status() is OK. Calling next() on such a
    // cursor will cause it to be placed on the first record greater than the one it was saved
    // on. Likewise, previous() will place the cursor on the first record smaller than the
    // saved record. In either case, if no such record exists, the cursor will be invalidated.
    ASSERT_OK(m_tree->erase(make_normal_key(0)));

    // Cursor isn't aware of modifications yet.
    ASSERT_EQ(cursor->key(), make_normal_key(0));
    ASSERT_EQ(cursor->value(), v0);

    ASSERT_OK(m_tree->put(make_normal_key(0), "value"));
    ASSERT_EQ(cursor->key(), make_normal_key(0));
    ASSERT_EQ(cursor->value(), v0);
}

TEST_F(MultiCursorTests, CursorManagement)
{
    std::default_random_engine rng;
    for (std::size_t i = 1; i < 123; ++i) {
        while (m_cursors.size() < i) {
            add_cursor();
        }
        std::shuffle(
            begin(m_cursors),
            end(m_cursors),
            rng);
        while (!m_cursors.empty()) {
            del_cursor(0);
        }
    }
}

TEST_F(MultiCursorTests, LotsOfCursors)
{
    for (std::size_t i = 1; i < m_pager->buffer_count() * 10; ++i) {
        add_cursor();
    }
    for (auto *c : m_cursors) {
        c->seek_first();
    }
    for (std::size_t i = 0; i < m_cursors.size(); ++i) {
        for (std::size_t j = 0; m_cursors[i]->is_valid() && j < i; ++j) {
            // Spread the cursors out until too many page cache frames are occupied.
            m_cursors[i]->next();
        }
        ASSERT_OK(m_cursors[i]->status());
    }

    // Both put() and erase() cause live cursors to be saved.
    ASSERT_OK(m_tree->put("key", "value"));
    ASSERT_OK(m_tree->erase("key"));
}

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
            const PointerMap::Entry entry{Id(id.value + 1), PointerMap::kTreeNode};
            ASSERT_OK(PointerMap::write_entry(*m_pager, id, entry));
        }
    }
    for (std::size_t i = 0; i < map_size() + 10; ++i) {
        if (i != map_size()) {
            const Id id(i + 3);
            PointerMap::Entry entry;
            ASSERT_OK(PointerMap::read_entry(*m_pager, id, entry));
            ASSERT_EQ(entry.back_ptr.value - 1, id.value);
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
            value = random.Generate(kPageSize / 2);
        }
    }

    ~MultiTreeTests() override = default;

    auto SetUp() -> void override
    {
        TreeTests::SetUp();
        m_schema = new Schema(
            *m_pager,
            m_status,
            m_stat,
            m_scratch.data());
    }

    auto TearDown() -> void override
    {
        multi_tree.clear();
        m_schema->close();
        delete m_schema;
        m_pager->finish();
    }

    auto create_tree(std::size_t tid)
    {
        Id root;
        Bucket b;
        EXPECT_OK(Tree::create(*m_pager, &root));
        EXPECT_OK(m_schema->create_bucket(
            BucketOptions(),
            numeric_key(tid),
            &b));
        EXPECT_EQ(multi_tree.find(tid), end(multi_tree));
        multi_tree.emplace(tid, static_cast<Tree *>(b.state));
    }

    auto fill_tree(std::size_t tid)
    {
        for (std::size_t i = 0; i < kInitialRecordCount; ++i) {
            const auto value = payload_values[(i + tid) % payload_values.size()];
            ASSERT_OK(multi_tree.at(tid)->put(make_long_key(i), value));
        }
        multi_tree.at(tid)->TEST_validate();
    }

    auto check_tree(std::size_t tid)
    {
        std::string value;
        for (std::size_t i = 0; i < kInitialRecordCount; ++i) {
            ASSERT_OK(multi_tree.at(tid)->get(make_long_key(i), &value));
            ASSERT_EQ(value, payload_values[(i + tid) % payload_values.size()]);
        }
    }

    auto clear_tree(std::size_t tid)
    {
        for (std::size_t i = 0; i < kInitialRecordCount; ++i) {
            ASSERT_OK(multi_tree.at(tid)->erase(make_long_key(i)));
        }
    }

    auto drop_tree(std::size_t tid)
    {
        ASSERT_NE(multi_tree.find(tid), end(multi_tree));
        ASSERT_OK(m_schema->drop_bucket(numeric_key(tid)));
        multi_tree.erase(tid);
    }

    Schema *m_schema = nullptr;
    std::unordered_map<std::size_t, Tree *> multi_tree;
    std::vector<std::string> payload_values;
    std::string m_scratch;
};

TEST_F(MultiTreeTests, CreateAdditionalTrees)
{
    create_tree(1);
    create_tree(2);
    create_tree(3);
}

TEST_F(MultiTreeTests, DuplicateKeysAreAllowedBetweenTrees)
{
    create_tree(1);
    create_tree(2);

    auto &hello_tree = multi_tree.at(1);
    auto &world_tree = multi_tree.at(2);
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
    create_tree(1);
    fill_tree(1);
    clear_tree(1);
}

TEST_F(MultiTreeTests, MultipleSplitsAndMerges_1)
{
    for (std::size_t tid = 0; tid < 10; ++tid) {
        create_tree(tid);
    }
    for (std::size_t tid = 0; tid < 10; ++tid) {
        fill_tree(tid);
    }
    for (std::size_t tid = 0; tid < 10; ++tid) {
        check_tree(tid);
    }
    for (std::size_t tid = 0; tid < 10; ++tid) {
        clear_tree(tid);
    }
}

TEST_F(MultiTreeTests, MultipleSplitsAndMerges_2)
{
    for (std::size_t tid = 0; tid < 10; ++tid) {
        create_tree(tid);
        fill_tree(tid);
        check_tree(tid);
        clear_tree(tid);
    }
}

TEST_F(MultiTreeTests, TreeDestruction_1)
{
    for (std::size_t i = 0; i < 2; ++i) {
        create_tree(1);
        fill_tree(1);
        drop_tree(1);
    }
    create_tree(1);
    fill_tree(1);
    check_tree(1);
    clear_tree(1);
}

TEST_F(MultiTreeTests, TreeDestruction_2)
{
    for (std::size_t tid = 0; tid < 10; ++tid) {
        create_tree(tid);
        fill_tree(tid);
    }
    for (std::size_t tid = 0; tid < 10; ++tid) {
        drop_tree(tid);
    }
    for (std::size_t tid = 0; tid < 10; ++tid) {
        create_tree(tid);
        fill_tree(tid);
        check_tree(tid);
        clear_tree(tid);
    }
}

TEST_F(MultiTreeTests, SavedCursors)
{
    std::vector<std::size_t> tids;
    std::vector<Cursor *> cs;
    Status s;

    while (s.is_ok()) {
        // Create a new tree and add some records.
        tids.emplace_back(tids.size());
        create_tree(tids.back());
        for (std::size_t i = 0; s.is_ok() && i < kInitialRecordCount; ++i) {
            const auto value = payload_values[i];
            s = multi_tree.at(tids.back())->put(make_long_key(i), value);
        }
        if (!s.is_ok()) {
            break;
        }
        // Advance or wrap the cursors, all of which should be live again (not "saved").
        for (auto *c : cs) {
            if (c->is_valid()) {
                c->next();
            } else if (c->status().is_ok()) {
                c->seek_first();
            } else {
                s = c->status();
                break;
            }
        }
        if (!s.is_ok()) {
            break;
        }
        // Add a new cursor to every tree.
        for (auto tid : tids) {
            cs.emplace_back(multi_tree.at(tid)->new_cursor());
            cs.back()->seek_first();
            if (!cs.back()->is_valid()) {
                s = cs.back()->status();
                break;
            }
        }
    }

    // Should be an "out of frames" error.   TODO: This error can be made to not happen. Cursors need a callback or something to call
    ASSERT_TRUE(s.is_invalid_argument()); // TODO: to tell the schema to use the current tree and save cursors belonging to other trees.

    for (const auto *c : cs) {
        delete c;
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
    PermutationGenerator<int> generator({1, 2, 3});

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

TEST_P(RebalanceTests, SanityCheck)
{
    run({0, 0, 0, 0, 0});
    run({1, 1, 1, 1});
    run({2, 2, 2});
}

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

TEST(SuffixTruncationTests, SuffixTruncation)
{
    const auto checked_truncate_suffix = [](const auto &lhs, const auto &rhs) {
        const auto prefix = truncate_suffix(lhs, rhs);
        // lhs < prefix <= rhs
        EXPECT_FALSE(prefix.is_empty());
        EXPECT_LT(lhs, prefix);
        EXPECT_LE(prefix, rhs);
        return prefix;
    };

    ASSERT_EQ("1", checked_truncate_suffix("0", "1"));
    ASSERT_EQ("1", checked_truncate_suffix("00", "1"));
    ASSERT_EQ("1", checked_truncate_suffix("0", "11"));
    ASSERT_EQ("1", checked_truncate_suffix("00", "11"));
    ASSERT_EQ("01", checked_truncate_suffix("0", "01"));
    ASSERT_EQ("01", checked_truncate_suffix("00", "01"));
    ASSERT_EQ("10", checked_truncate_suffix("1", "10"));

    // Examples are from https://dl.acm.org/doi/pdf/10.1145/320521.320530.
    ASSERT_EQ("An", checked_truncate_suffix("A", "An"));
    ASSERT_EQ("As", checked_truncate_suffix("And", "As"));
    ASSERT_EQ("Solv", checked_truncate_suffix("Solutions", "Solve"));
    ASSERT_EQ("S", checked_truncate_suffix("Problems", "Solution"));

    // lhs may be empty, but since lhs < rhs, rhs must not be empty.
    ASSERT_EQ("0", checked_truncate_suffix("", "0"));
    ASSERT_EQ("0", checked_truncate_suffix("", "00"));
}

class CursorModificationTests
    : public TreeTestHarness,
      public testing::Test
{
public:
    ~CursorModificationTests() override = default;

    auto SetUp() -> void override
    {
        open();
    }
    auto TearDown() -> void override
    {
        close();
    }
};

TEST_F(CursorModificationTests, QuickCheck)
{
    std::unique_ptr<Cursor> c(m_tree->new_cursor());
    ASSERT_FALSE(c->is_valid());

    for (std::ptrdiff_t i = 0; i < 2; ++i) {
        for (const auto *key : {"BB", "CC", "AA"}) {
            const auto *value = key + i;
            ASSERT_OK(m_tree->put(*c, key, value));
            ASSERT_TRUE(c->is_valid());
            ASSERT_EQ(Slice(key), c->key());
            ASSERT_EQ(Slice(value), c->value());
        }
    }

    for (const auto *key : {"AA", "BB", "CC"}) {
        ASSERT_TRUE(c->is_valid());
        ASSERT_EQ(Slice(key), c->key());
        ASSERT_EQ(Slice(key + 1), c->value());
        ASSERT_OK(m_tree->erase(*c));
    }

    ASSERT_FALSE(c->is_valid());
}

TEST_F(CursorModificationTests, SeekAndPut)
{
    auto num_records = kInitialRecordCount;
    for (std::size_t i = 0; i < num_records; ++i) {
        ASSERT_OK(m_tree->put(make_long_key(i * 2), make_value(false)));
    }
    for (std::size_t iteration = 0; iteration < 2; ++iteration) {
        auto step = num_records / 10;
        std::unique_ptr<Cursor> c(m_tree->new_cursor());
        if (iteration == 0) {
            c->seek_first();
        } else {
            c->seek_last();
        }
        for (std::size_t i = 0; c->is_valid() && i < kInitialRecordCount; ++i) {
            ASSERT_OK(m_tree->put(*c, make_long_key(i * 2 + iteration), make_value(true)));
            ++num_records;
            for (std::size_t j = 0; c->is_valid() && j < step; ++j) {
                if (iteration == 0) {
                    c->next();
                } else {
                    c->previous();
                }
            }
        }
        ASSERT_OK(c->status());
        ASSERT_FALSE(c->is_valid());
        validate();
    }
}

TEST_F(CursorModificationTests, PutWithoutCursor)
{
    init_tree(*this, kInitNormal);
    std::unique_ptr<Cursor> c(m_tree->new_cursor());
    c->seek_first();
    ASSERT_TRUE(c->is_valid());

    for (InitFlag flag = kInitNormal + 1; flag <= kInitFlagMax; ++flag) {
        init_tree(*this, flag);
        ASSERT_TRUE(c->is_valid());
        ASSERT_EQ(make_normal_key(0), c->key());
        ASSERT_EQ(make_value('*', false), c->value());
    }
}

TEST_F(CursorModificationTests, EraseAllRecordsFromLeft)
{
    init_tree(*this, kInitLongKeys | kInitLongValues);
    std::unique_ptr<Cursor> c(m_tree->new_cursor());
    c->seek_first();
    for (std::size_t i = 0; i < kInitialRecordCount; ++i) {
        ASSERT_TRUE(c->is_valid());
        ASSERT_OK(m_tree->erase(*c));
    }
    ASSERT_FALSE(c->is_valid());
    validate();
}

TEST_F(CursorModificationTests, EraseAllRecordsFromRight)
{
    init_tree(*this, kInitLongKeys | kInitLongValues);
    std::unique_ptr<Cursor> c(m_tree->new_cursor());
    for (std::size_t i = 0; i < kInitialRecordCount; ++i) {
        c->seek_last();
        // Cursor immediately falls off the edge of the key range.
        ASSERT_OK(m_tree->erase(*c));
        ASSERT_FALSE(c->is_valid());
    }
    validate();
}

TEST_F(CursorModificationTests, SeekAndEraseForward)
{
    init_tree(*this, kInitLongKeys | kInitLongValues);
    auto num_records = kInitialRecordCount;
    while (num_records > 0) {
        const auto step = num_records / 4;
        std::unique_ptr<Cursor> c(m_tree->new_cursor());
        c->seek_first();
        while (num_records > 0 && c->is_valid()) {
            ASSERT_OK(m_tree->erase(*c));
            --num_records;
            for (std::size_t i = 0; c->is_valid() && i < step; ++i) {
                c->next();
            }
        }
        ASSERT_OK(c->status());
        ASSERT_FALSE(c->is_valid());
        validate();
    }
}

TEST_F(CursorModificationTests, SeekAndEraseBackward)
{
    init_tree(*this, kInitLongKeys | kInitLongValues);
    auto num_records = kInitialRecordCount;
    while (num_records > 0) {
        const auto step = num_records / 4;
        std::unique_ptr<Cursor> c(m_tree->new_cursor());
        c->seek_last();
        for (auto first = true; num_records > 0 && c->is_valid();) {
            ASSERT_OK(m_tree->erase(*c));
            if (first) {
                // Erasing the last record causes the cursor to immediately fall off the
                // edge of the key range.
                ASSERT_FALSE(c->is_valid());
                c->seek_last();
                first = false;
            }
            --num_records;
            for (std::size_t i = 0; c->is_valid() && i < step; ++i) {
                c->previous();
            }
        }
        ASSERT_OK(c->status());
        ASSERT_FALSE(c->is_valid());
        validate();
    }
}

TEST_F(CursorModificationTests, ModifyExistingRecords)
{
    for (std::size_t i = 0; i < kInitialRecordCount; ++i) {
        ASSERT_OK(m_tree->put(numeric_key(i), ""));
    }

    static constexpr std::size_t kSize = 25;
    for (std::size_t iteration = 0; iteration < kSize; ++iteration) {
        std::unique_ptr<Cursor> c(m_tree->new_cursor());
        if (iteration == 0) {
            c->seek_first();
        } else {
            c->seek_last();
        }
        while (c->is_valid()) {
            const std::string value((iteration + 1) * kSize, '*');
            ASSERT_OK(m_tree->put(*c, c->key(), value));

            if (iteration == 0) {
                c->next();
            } else {
                c->previous();
            }
        }
        ASSERT_OK(c->status());
        validate();
    }

    std::unique_ptr<Cursor> c(m_tree->new_cursor());
    c->seek_first();
    for (std::size_t i = 0; i < kInitialRecordCount; ++i) {
        ASSERT_TRUE(c->is_valid());
        ASSERT_EQ(c->key(), numeric_key(i));
        ASSERT_EQ(c->value(), std::string(kSize * kSize, '*'));
        c->next();
    }
    ASSERT_FALSE(c->is_valid());
    ASSERT_OK(c->status());
}

TEST_F(CursorModificationTests, UntrackedCursors)
{
    init_tree(*this, kInitLongValues);

    std::unique_ptr<Cursor> c1(m_tree->new_cursor());
    std::unique_ptr<Cursor> c2(m_tree->new_cursor());
    c1->seek_first();
    c2->seek_last();

    for (std::size_t i = 0; i < kInitialRecordCount; ++i) {
        ASSERT_OK(m_tree->erase(make_normal_key(i)));
    }

    ASSERT_TRUE(c1->is_valid());
    ASSERT_EQ(c1->key(), make_normal_key(0));
    ASSERT_TRUE(c2->is_valid());
    ASSERT_EQ(c2->key(), make_normal_key(kInitialRecordCount - 1));

    c1->next();
    c2->previous();

    ASSERT_FALSE(c1->is_valid());
    ASSERT_FALSE(c2->is_valid());

    c1->seek_first();
    c1->seek_last();

    ASSERT_FALSE(c1->is_valid());
    ASSERT_FALSE(c2->is_valid());
}

class VacuumTests : public MultiTreeTests
{
public:
    explicit VacuumTests() = default;

    ~VacuumTests() override = default;

    auto SetUp() -> void override
    {
        MultiTreeTests::SetUp();
        m_root = m_tree;
        create_tree(1);
        m_tree = multi_tree.at(1);
    }

    auto TearDown() -> void override
    {
        m_tree = m_root;
        MultiTreeTests::TearDown();
    }

    Tree *m_root;
};

TEST_F(VacuumTests, VacuumEmptyTree)
{
    ASSERT_OK(m_schema->vacuum());
    validate();
}

TEST_F(VacuumTests, VacuumFreelist)
{
    std::unique_ptr<Cursor> c(m_tree->new_cursor());
    for (InitFlag flag = kInitNormal; flag <= kInitFlagMax; ++flag) {
        init_tree(*this, flag);

        c->seek_first();
        while (c->is_valid()) {
            ASSERT_OK(m_tree->erase(*c));
        }

        ASSERT_OK(m_schema->vacuum());
        validate();
    }
}

TEST_F(VacuumTests, VacuumOverflowChains)
{
    const std::string kv[][2] = {
        {'_' + make_normal_key(1), make_value('a', true)},
        {'_' + make_long_key(2), make_value('b', false)},
        {'_' + make_long_key(3), make_value('c', true)},
    };
    init_tree(*this);
    ASSERT_OK(m_tree->put(kv[0][0], kv[0][1]));
    ASSERT_OK(m_tree->put(kv[1][0], kv[1][1]));
    ASSERT_OK(m_tree->put(kv[2][0], kv[2][1]));

    for (std::size_t i = 0; i < kInitialRecordCount; ++i) {
        ASSERT_OK(m_tree->erase(make_long_key(i)));
    }

    ASSERT_OK(m_schema->vacuum());
    validate();

    std::string value;
    ASSERT_OK(m_tree->get(kv[0][0], &value));
    ASSERT_EQ(value, kv[0][1]);
    ASSERT_OK(m_tree->get(kv[1][0], &value));
    ASSERT_EQ(value, kv[1][1]);
    ASSERT_OK(m_tree->get(kv[2][0], &value));
    ASSERT_EQ(value, kv[2][1]);
}

TEST_F(VacuumTests, VacuumPartialRange)
{
    std::unique_ptr<Cursor> c(m_tree->new_cursor());
    for (std::size_t i = 0; i < 2; ++i) {
        init_tree(*this, kInitLongKeys | (i == 0 ? 0 : kInitLongValues));

        c->seek_first();
        const auto batch_size = kInitialRecordCount / 3 * (i + 1);
        for (std::size_t n = 0; c->is_valid() && n < batch_size; ++n) {
            ASSERT_OK(m_tree->erase(*c));
        }

        ASSERT_OK(m_schema->vacuum());
        validate();

        for (std::size_t n = batch_size; n < kInitialRecordCount; ++n) {
            ASSERT_OK(m_tree->get(make_long_key(n), nullptr));
        }
    }
}

TEST_F(VacuumTests, VacuumSchemaTree)
{
    static constexpr std::size_t kSize = 1'234;
    // Add records to m_tree.
    init_tree(*this, kInitLongValues);
    std::unique_ptr<Cursor> c(m_schema->new_cursor());
    std::vector<Bucket> buckets;
    for (std::size_t i = 0; i < kSize; i += 2) {
        buckets.emplace_back();
        ASSERT_OK(m_schema->create_bucket(BucketOptions(), numeric_key(i), &buckets.back()));
        ASSERT_OK(m_schema->create_bucket(BucketOptions(), numeric_key(i + 1), nullptr));
    }
    c->seek_first();
    for (std::size_t i = 0; i < kSize; ++i) {
        ASSERT_OK(m_tree->erase(make_normal_key(i)));
    }
    ASSERT_OK(m_schema->vacuum());
}

} // namespace calicodb::test