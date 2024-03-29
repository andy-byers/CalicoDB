// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#include "bucket_impl.h"
#include "common.h"
#include "cursor_impl.h"
#include "encoding.h"
#include "freelist.h"
#include "logging.h"
#include "schema.h"
#include "status_internal.h"
#include "temp.h"
#include "test.h"
#include "tree.h"
#include "wal_internal.h"
#include <unordered_map>

namespace calicodb::test
{

static constexpr size_t kInitialRecordCount = 1'000;

static auto tree_cursor_cast(void *ptr) -> TreeCursor &
{
    return *static_cast<TreeCursor *>(ptr);
}

static auto tree_cursor_cast(Cursor &c) -> TreeCursor &
{
    return tree_cursor_cast(c.handle());
}

class TreeTestHarness
{
public:
    Env *m_env;
    Status m_status;
    Stats m_stat;
    Pager *m_pager = nullptr;
    File *m_file = nullptr;
    Tree *m_tree = nullptr;
    ObjectPtr<Wal> m_wal;
    mutable CursorImpl *m_c;

    TreeTestHarness()
        : m_env(new_temp_env(TEST_PAGE_SIZE))
    {
        EXPECT_OK(m_env->new_file("db", Env::kCreate, m_file));
        const WalOptionsExtra wal_options = {
            {m_env,
             m_file,
             &m_stat},
            nullptr,
            Options::kSyncNormal,
            Options::kLockNormal,
        };
        m_wal.reset(new_temp_wal(wal_options, TEST_PAGE_SIZE));
        EXPECT_TRUE(m_wal);
        const Pager::Parameters pager_param = {
            "db",
            "wal",
            m_file,
            m_env,
            m_wal.get(),
            nullptr,
            &m_status,
            &m_stat,
            nullptr,
            TEST_PAGE_SIZE,
            kMinFrameCount * 5,
            Options::kSyncNormal,
            Options::kLockNormal,
            false,
        };
        EXPECT_OK(Pager::open(pager_param, m_pager));
    }

    ~TreeTestHarness()
    {
        EXPECT_TRUE(m_status.is_ok());
        EXPECT_EQ(m_c, nullptr);
        Mem::delete_object(m_pager);
        delete m_file;
        delete m_env;
    }

    auto allocate(bool is_external, Id nearby, Node &node_out) -> Status
    {
        PageRef *ref;
        auto s = m_tree->allocate(Tree::kAllocateAny, nearby, ref);
        if (s.is_ok()) {
            if (ref->refs == 1) {
                CALICODB_EXPECT_FALSE(PointerMap::is_map(ref->page_id, TEST_PAGE_SIZE));
                node_out = Node::from_new_page(m_tree->node_options, *ref, is_external);
            } else {
                m_pager->release(ref);
                s = StatusBuilder::corruption("page %u is corrupted", ref->page_id.value);
            }
        }
        return s;
    }

    [[nodiscard]] static auto make_normal_key(size_t value)
    {
        return numeric_key(value);
    }

    [[nodiscard]] static auto make_long_key(size_t value)
    {
        const auto suffix = make_normal_key(value);
        const std::string key(TEST_PAGE_SIZE * 2 - suffix.size(), '0');
        return key + suffix;
    }

    [[nodiscard]] static auto make_value(char c, bool overflow = false)
    {
        auto size = TEST_PAGE_SIZE;
        if (overflow) {
            size /= 3;
        } else {
            size /= 20;
        }
        return std::string(size, c);
    }

    void open()
    {
        EXPECT_OK(m_pager->lock_reader(nullptr));
        EXPECT_OK(m_pager->begin_writer());
        m_tree = new Tree(*m_pager, m_stat, Id::root());
        m_c = new (std::nothrow) CursorImpl(*m_tree);
    }

    void close() const
    {
        delete m_c;
        m_c = nullptr;
        m_tree->deactivate_cursors(nullptr);
        m_pager->finish();
        delete m_tree;
    }

    void validate() const
    {
        m_tree->deactivate_cursors(nullptr);
        ASSERT_OK(m_tree->check_integrity());
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

    void SetUp() override
    {
        open();
    }

    void TearDown() override
    {
        close();
    }

    RandomGenerator random;
};

TEST_F(TreeTests, ConstructsAndDestructs)
{
    validate();
}

TEST_F(TreeTests, SearchLeaf)
{
    ASSERT_OK(m_tree->insert(tree_cursor_cast(*m_c), "b", "", false));
    ASSERT_OK(m_tree->insert(tree_cursor_cast(*m_c), "d", "", false));

    m_c->find("b");
    ASSERT_TRUE(m_c->is_valid());
    ASSERT_EQ(m_c->key(), "b");
    m_c->find("d");
    ASSERT_TRUE(m_c->is_valid());
    ASSERT_EQ(m_c->key(), "d");

    m_c->find("a");
    ASSERT_FALSE(m_c->is_valid());
    m_c->find("c");
    ASSERT_FALSE(m_c->is_valid());
    m_c->find("e");
    ASSERT_FALSE(m_c->is_valid());

    m_c->seek("b");
    ASSERT_TRUE(m_c->is_valid());
    ASSERT_EQ(m_c->key(), "b");
    m_c->seek("d");
    ASSERT_TRUE(m_c->is_valid());
    ASSERT_EQ(m_c->key(), "d");

    m_c->seek("a");
    ASSERT_TRUE(m_c->is_valid());
    ASSERT_EQ(m_c->key(), "b");
    m_c->seek("c");
    ASSERT_TRUE(m_c->is_valid());
    ASSERT_EQ(m_c->key(), "d");
    m_c->seek("e");
    ASSERT_FALSE(m_c->is_valid());
}

TEST_F(TreeTests, RecordsAreErased)
{
    ASSERT_OK(m_tree->insert(tree_cursor_cast(*m_c), "a", make_value('1'), false));
    m_c->find("a");
    ASSERT_OK(m_tree->erase(tree_cursor_cast(*m_c), false));
    m_c->find("a");
    ASSERT_FALSE(m_c->is_valid());
}

TEST_F(TreeTests, HandlesLargePayloads)
{
    ASSERT_OK(m_tree->insert(tree_cursor_cast(*m_c), make_long_key('1'), "1", false));
    ASSERT_OK(m_tree->insert(tree_cursor_cast(*m_c), "2", make_value('2', true), false));
    ASSERT_OK(m_tree->insert(tree_cursor_cast(*m_c), make_long_key('3'), make_value('3', true), false));

    m_c->find(make_long_key('1'));
    ASSERT_TRUE(m_c->is_valid());
    ASSERT_EQ(m_c->value(), "1");
    m_c->find("2");
    ASSERT_TRUE(m_c->is_valid());
    ASSERT_EQ(m_c->value(), make_value('2', true));
    m_c->find(make_long_key('3'));
    ASSERT_TRUE(m_c->is_valid());
    ASSERT_EQ(m_c->value(), make_value('3', true));

    m_c->find(make_long_key('1'));
    ASSERT_OK(m_tree->erase(tree_cursor_cast(*m_c), false));
    m_c->find("2");
    ASSERT_OK(m_tree->erase(tree_cursor_cast(*m_c), false));
    m_c->find(make_long_key('3'));
    ASSERT_OK(m_tree->erase(tree_cursor_cast(*m_c), false));
}

TEST_F(TreeTests, LongVsShortKeys)
{
    for (int i = 0; i < 2; ++i) {
        const auto actual_key_len = i == 0 ? 1U : TEST_PAGE_SIZE * 2 - 1;
        const auto search_key_len = TEST_PAGE_SIZE * 2 - actual_key_len;
        ASSERT_OK(m_tree->insert(tree_cursor_cast(*m_c), std::string(actual_key_len, 'a'), make_value('1', true), false));
        ASSERT_OK(m_tree->insert(tree_cursor_cast(*m_c), std::string(actual_key_len, 'b'), make_value('2', true), false));
        ASSERT_OK(m_tree->insert(tree_cursor_cast(*m_c), std::string(actual_key_len, 'c'), make_value('3', true), false));

        m_c->seek(std::string(search_key_len, i == 0 ? 'A' : 'a'));
        ASSERT_TRUE(m_c->is_valid());
        ASSERT_EQ(std::string(actual_key_len, 'a'), m_c->key());
        ASSERT_EQ(make_value('1', true), m_c->value());
        m_c->seek(std::string(search_key_len, i == 0 ? 'a' : 'b'));
        ASSERT_TRUE(m_c->is_valid());
        ASSERT_EQ(std::string(actual_key_len, 'b'), m_c->key());
        ASSERT_EQ(make_value('2', true), m_c->value());
        m_c->seek(std::string(search_key_len, i == 0 ? 'b' : 'c'));
        ASSERT_TRUE(m_c->is_valid());
        ASSERT_EQ(std::string(actual_key_len, 'c'), m_c->key());
        ASSERT_EQ(make_value('3', true), m_c->value());

        m_c->find(std::string(actual_key_len, 'a'));
        ASSERT_OK(m_tree->erase(tree_cursor_cast(*m_c), false));
        m_c->find(std::string(actual_key_len, 'b'));
        ASSERT_OK(m_tree->erase(tree_cursor_cast(*m_c), false));
        m_c->find(std::string(actual_key_len, 'c'));
        ASSERT_OK(m_tree->erase(tree_cursor_cast(*m_c), false));
    }
}

TEST_F(TreeTests, GetNonexistentKeys)
{
    // Missing 0
    ASSERT_OK(m_tree->insert(tree_cursor_cast(*m_c), make_long_key(1), make_value('1', true), false));
    // Missing 2
    ASSERT_OK(m_tree->insert(tree_cursor_cast(*m_c), make_long_key(3), make_value('3', true), false));
    ASSERT_OK(m_tree->insert(tree_cursor_cast(*m_c), make_long_key(4), make_value('4', true), false));
    ASSERT_OK(m_tree->insert(tree_cursor_cast(*m_c), make_long_key(5), make_value('5', true), false));
    // Missing 6
    ASSERT_OK(m_tree->insert(tree_cursor_cast(*m_c), make_long_key(7), make_value('7', true), false));
    ASSERT_OK(m_tree->insert(tree_cursor_cast(*m_c), make_long_key(8), make_value('8', true), false));
    ASSERT_OK(m_tree->insert(tree_cursor_cast(*m_c), make_long_key(9), make_value('9', true), false));
    // Missing 10

    m_c->find(make_long_key(0));
    ASSERT_FALSE(m_c->is_valid());
    m_c->find(make_long_key(2));
    ASSERT_FALSE(m_c->is_valid());
    m_c->find(make_long_key(6));
    ASSERT_FALSE(m_c->is_valid());
    m_c->find(make_long_key(10));
    ASSERT_FALSE(m_c->is_valid());

    m_c->find(make_long_key(1));
    ASSERT_TRUE(m_c->is_valid());
    ASSERT_EQ(m_c->value(), make_value('1', true));
    m_c->find(make_long_key(3));
    ASSERT_TRUE(m_c->is_valid());
    ASSERT_EQ(m_c->value(), make_value('3', true));
    m_c->find(make_long_key(5));
    ASSERT_TRUE(m_c->is_valid());
    ASSERT_EQ(m_c->value(), make_value('5', true));
    m_c->find(make_long_key(7));
    ASSERT_TRUE(m_c->is_valid());
    ASSERT_EQ(m_c->value(), make_value('7', true));
    m_c->find(make_long_key(9));
    ASSERT_TRUE(m_c->is_valid());
    ASSERT_EQ(m_c->value(), make_value('9', true));
}

TEST_F(TreeTests, ResolvesOverflowsOnLeftmostPosition1)
{
    for (size_t i = 0; i < 100; ++i) {
        ASSERT_OK(m_tree->insert(tree_cursor_cast(*m_c), numeric_key(99 - i), make_value('*'), false));
    }
    validate();
}

TEST_F(TreeTests, ResolvesOverflowsOnLeftmostPosition2)
{
    for (size_t i = 0; i < 100; ++i) {
        ASSERT_OK(m_tree->insert(tree_cursor_cast(*m_c), make_long_key(99 - i), make_value('*', true), false));
        validate();
    }
}

TEST_F(TreeTests, ResolvesOverflowsOnRightmostPosition1)
{
    for (size_t i = 0; i < 100; ++i) {
        ASSERT_OK(m_tree->insert(tree_cursor_cast(*m_c), numeric_key(i), make_value('*'), false));
    }
    validate();
}

TEST_F(TreeTests, ResolvesOverflowsOnRightmostPosition2)
{
    for (size_t i = 0; i < 100; ++i) {
        ASSERT_OK(m_tree->insert(tree_cursor_cast(*m_c), make_long_key(i), make_value('*', true), false));
    }
    validate();
}

TEST_F(TreeTests, ResolvesOverflowsOnMiddlePosition1)
{
    for (size_t i = 0, j = 99; i < j; ++i, --j) {
        ASSERT_OK(m_tree->insert(tree_cursor_cast(*m_c), numeric_key(i), make_value('*'), false));
        ASSERT_OK(m_tree->insert(tree_cursor_cast(*m_c), numeric_key(j), make_value('*'), false));
    }
    validate();
}

TEST_F(TreeTests, ResolvesOverflowsOnMiddlePosition2)
{
    for (size_t i = 0, j = 99; i < j; ++i, --j) {
        ASSERT_OK(m_tree->insert(tree_cursor_cast(*m_c), make_long_key(i), make_value('*', true), false));
        ASSERT_OK(m_tree->insert(tree_cursor_cast(*m_c), make_long_key(j), make_value('*', true), false));
    }
    validate();
}

using InitFlag = uint32_t;
static constexpr InitFlag kInitNormal = 0;
static constexpr InitFlag kInitLongKeys = 1;
static constexpr InitFlag kInitLongValues = 2;
static constexpr InitFlag kInitFlagMax = 3;
static auto init_tree(TreeTestHarness &test, InitFlag flags = kInitNormal)
{
    for (size_t i = 0; i < kInitialRecordCount; ++i) {
        ASSERT_OK(test.m_tree->insert(tree_cursor_cast(*test.m_c),
                                      flags & kInitLongKeys ? TreeTestHarness::make_long_key(i)
                                                            : TreeTestHarness::make_normal_key(i),
                                      TreeTestHarness::make_value('*', flags & kInitLongValues), false));
    }
    test.validate();
    test.m_tree->deactivate_cursors(nullptr);
}

TEST_F(TreeTests, PrintStructure)
{
    String empty, normal_keys, long_keys;
    ASSERT_OK(m_tree->print_structure(empty));
    init_tree(*this, kInitNormal);
    ASSERT_OK(m_tree->print_structure(normal_keys));
    init_tree(*this, kInitLongKeys);
    ASSERT_OK(m_tree->print_structure(long_keys));
    // empty may or may not be empty...
    ASSERT_FALSE(normal_keys.is_empty());
    ASSERT_FALSE(long_keys.is_empty());
}

TEST_F(TreeTests, PrintRecords)
{
    String empty, normal_keys, long_keys;
    ASSERT_OK(m_tree->print_nodes(empty));
    init_tree(*this, kInitNormal);
    ASSERT_OK(m_tree->print_nodes(normal_keys));
    init_tree(*this, kInitLongKeys);
    ASSERT_OK(m_tree->print_nodes(long_keys));
    // empty may or may not be empty...
    ASSERT_FALSE(normal_keys.is_empty());
    ASSERT_FALSE(long_keys.is_empty());
}

TEST_F(TreeTests, ResolvesUnderflowsOnRightmostPosition)
{
    init_tree(*this, kInitLongKeys);
    for (size_t i = 0; i < kInitialRecordCount; ++i) {
        m_c->find(make_long_key(kInitialRecordCount - i - 1));
        ASSERT_OK(m_tree->erase(tree_cursor_cast(*m_c), false));
    }
    validate();
}

TEST_F(TreeTests, ResolvesUnderflowsOnLeftmostPosition)
{
    init_tree(*this, kInitLongKeys);
    for (size_t i = 0; i < kInitialRecordCount; ++i) {
        m_c->find(make_long_key(i));
        ASSERT_OK(m_tree->erase(tree_cursor_cast(*m_c), false));
    }
    validate();
}

TEST_F(TreeTests, ResolvesUnderflowsOnMiddlePosition)
{
    init_tree(*this, kInitLongKeys);
    for (size_t i = 0, j = kInitialRecordCount - 1; i < j; ++i, --j) {
        m_c->find(make_long_key(i));
        ASSERT_OK(m_tree->erase(tree_cursor_cast(*m_c), false));
        m_c->find(make_long_key(j));
        ASSERT_OK(m_tree->erase(tree_cursor_cast(*m_c), false));
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
        char key[2];
        put_u16(key, static_cast<uint16_t>(kInitialRecordCount - i - 1));
        ASSERT_OK(m_tree->insert(tree_cursor_cast(*m_c), {key, sizeof(key)}, "v", false));
    }
    for (unsigned i = 0; i < kInitialRecordCount; ++i) {
        const auto key = random.Generate(TEST_PAGE_SIZE);
        ASSERT_OK(m_tree->insert(tree_cursor_cast(*m_c), key, "v", false));
    }
    validate();
}

TEST_F(TreeTests, AllowsEmptyKey)
{
    for (InitFlag flag = kInitNormal; flag <= kInitFlagMax; ++flag) {
        ASSERT_OK(m_tree->insert(tree_cursor_cast(*m_c), "", "value", false));
        init_tree(*this, flag);
        m_c->find("");
        ASSERT_TRUE(m_c->is_valid());
        ASSERT_EQ("value", m_c->value());
        m_c->find("");
        ASSERT_OK(m_tree->erase(tree_cursor_cast(*m_c), false));
    }
}

TEST_F(TreeTests, AlreadyErased1)
{
    TreeCursor c(*m_tree);
    ASSERT_OK(m_tree->insert(c, "a", "1", false));
    c.read_record();

    // Erase normal record out from under c.
    m_c->find("a");
    ASSERT_TRUE(m_c->is_valid());
    ASSERT_OK(m_tree->erase(tree_cursor_cast(*m_c), false));
    ASSERT_NOK(m_tree->erase(c, true));
}

static auto allocate_some_pages(Pager &pager)
{
    PageRef *p1, *p2, *p3;
    ASSERT_OK(pager.allocate(p1));
    ASSERT_OK(pager.allocate(p2));
    ASSERT_OK(pager.allocate(p3));
    pager.release(p1);
    pager.release(p2);
    pager.release(p3);
}

TEST_F(TreeTests, AlreadyErased2)
{
    TreeCursor c(*m_tree);
    char root_id[sizeof(uint32_t)];
    put_u32(root_id, 123);
    allocate_some_pages(*m_pager);
    ASSERT_OK(m_tree->insert(c, "a", Slice(root_id, sizeof(root_id)), true));
    c.read_record();

    // Erase bucket record out from under c.
    m_c->find("a");
    ASSERT_TRUE(m_c->is_valid());
    ASSERT_TRUE(m_c->is_bucket());
    ASSERT_OK(m_tree->erase(tree_cursor_cast(*m_c), true));
    ASSERT_NOK(m_tree->erase(c, true));
}

TEST_F(TreeTests, AlreadyErased3)
{
    TreeCursor c(*m_tree);
    allocate_some_pages(*m_pager);
    ASSERT_OK(m_tree->insert(c, "a", "1", false));
    c.read_record();

    // Replace normal record with bucket record.
    m_c->find("a");
    ASSERT_TRUE(m_c->is_valid());
    ASSERT_FALSE(m_c->is_bucket());
    ASSERT_OK(m_tree->erase(tree_cursor_cast(*m_c), false));
    char root_id[sizeof(uint32_t)];
    put_u32(root_id, 123);
    ASSERT_OK(m_tree->insert(tree_cursor_cast(*m_c), "a", Slice(root_id, sizeof(root_id)), true));

    ASSERT_NOK(m_tree->erase(c, false));
}

class TreeSanityChecks
    : public TreeTestHarness,
      public testing::TestWithParam<uint32_t>
{
public:
    void SetUp() override
    {
        open();
    }
    void TearDown() override
    {
        close();
    }

    auto random_chunk(bool overflow, bool nonzero = true)
    {
        return random.Generate(random.Next(nonzero, TEST_PAGE_SIZE * overflow + 12));
    }

    auto random_write() -> std::pair<std::string, std::string>
    {
        const auto key = random_chunk(overflow_keys);
        const auto val = random_chunk(overflow_values, false);
        EXPECT_OK(m_tree->insert(tree_cursor_cast(*m_c), key, val, false));
        return {key.to_string(), val.to_string()};
    }

    const bool overflow_keys = GetParam() & 0b10;
    const bool overflow_values = GetParam() & 0b01;
    const size_t record_count =
        kInitialRecordCount * 5 +
        kInitialRecordCount * 5 * !overflow_keys +
        kInitialRecordCount * 5 * !overflow_values;
    RandomGenerator random;
};

TEST_P(TreeSanityChecks, ReadAndWrite)
{
    std::unordered_map<std::string, std::string> records;
    for (size_t i = 0; i < record_count; ++i) {
        const auto [k, v] = random_write();
        records[k] = v;
    }
    validate();

    for (const auto &[key, value] : records) {
        m_c->find(key);
        ASSERT_TRUE(m_c->is_valid());
        ASSERT_EQ(m_c->value(), value);
    }
}

TEST_P(TreeSanityChecks, Erase)
{
    std::unordered_map<std::string, std::string> records;
    for (size_t iteration = 0; iteration < 3; ++iteration) {
        for (size_t i = 0; i < record_count; ++i) {
            const auto [k, v] = random_write();
            records[k] = v;
        }
        for (const auto &[key, value] : records) {
            m_c->find(key);
            ASSERT_OK(m_tree->erase(tree_cursor_cast(*m_c), false));
        }
        records.clear();
        validate();
    }
}

TEST_P(TreeSanityChecks, SmallRecords)
{
    std::unordered_map<std::string, std::string> records;
    for (size_t iteration = 0; iteration < 3; ++iteration) {
        for (size_t i = 0; i < record_count * 10; ++i) {
            const auto key = numeric_key<6>(i);
            ASSERT_OK(m_tree->insert(tree_cursor_cast(*m_c), key, "", false));
            records[key] = "";
        }
        validate();

        for (const auto &[key, value] : records) {
            m_c->find(key);
            ASSERT_OK(m_tree->erase(tree_cursor_cast(*m_c), false));
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
    testing::Values(
        0b00,
        0b01,
        0b10,
        0b11));

class RemoteComparisonTests
    : public TreeTestHarness,
      public testing::TestWithParam<uint32_t>
{
public:
    void SetUp() override
    {
        open();
    }

    void TearDown() override
    {
        close();
    }

    void random_write(size_t k)
    {
        // The part of the key necessary to determine ordering relationships may be located on
        // an overflow page.
        const auto prefix_size = m_random.Next(0, 64) + m_base_size;
        auto key = std::string(prefix_size, '0') + numeric_key(k);
        EXPECT_OK(m_tree->insert(tree_cursor_cast(*m_c), key, key, false));
        m_keys.push_back(std::move(key));
    }

    void check_records()
    {
        for (const auto &key : m_keys) {
            m_c->find(key);
            ASSERT_TRUE(m_c->is_valid());
            ASSERT_EQ(m_c->key(), key);
            ASSERT_EQ(m_c->value(), key);
        }
    }

    const size_t m_base_size = GetParam();
    std::vector<std::string> m_keys;
    RandomGenerator m_random;
};

TEST_P(RemoteComparisonTests, Comparisons)
{
    for (size_t i = 0; i < 1'024; ++i) {
        random_write(i);
    }
    validate();
    check_records();
}

INSTANTIATE_TEST_SUITE_P(
    SmallerThanPage,
    RemoteComparisonTests,
    testing::Range(1U, TEST_PAGE_SIZE, 16));

INSTANTIATE_TEST_SUITE_P(
    LargerThanPage,
    RemoteComparisonTests,
    testing::Range(TEST_PAGE_SIZE / 2, TEST_PAGE_SIZE * 2, 32));

class EmptyTreeCursorTests : public TreeTests
{
protected:
    ~EmptyTreeCursorTests() override = default;
};

TEST_F(EmptyTreeCursorTests, EmptyTreeBehavior)
{
    auto cursor = std::make_unique<CursorImpl>(*m_tree);
    ASSERT_TRUE(cursor);
    cursor->seek_first();
    ASSERT_FALSE(cursor->is_valid());
    cursor->seek_last();
    ASSERT_FALSE(cursor->is_valid());
    cursor->seek("42");
    ASSERT_FALSE(cursor->is_valid());
}

class CursorTests
    : public TreeTestHarness,
      public testing::TestWithParam<uint32_t>
{
protected:
    ~CursorTests() override
    {
        close();
        m_schema->close_trees();
        delete m_schema;
    }
    void SetUp() override
    {
        open();
        m_schema = new Schema(*m_pager, m_stat);
        init_tree(*this, kInitLongValues);
    }

    auto make_cursor() -> std::unique_ptr<CursorImpl>
    {
        switch (GetParam()) {
            case 0:
            case 1:
                return std::unique_ptr<CursorImpl>(new (std::nothrow) CursorImpl(*m_tree));

                // TODO                return std::unique_ptr<Cursor>(&m_schema->cursor());
        }
        return nullptr;
    }

    Schema *m_schema = nullptr;
    RandomGenerator random;
};

TEST_P(CursorTests, AccountsForNodeBoundaries)
{
    for (size_t i = 0; i + 5 < kInitialRecordCount; i += 5) {
        m_c->find(numeric_key(i + 1));
        ASSERT_OK(m_tree->erase(tree_cursor_cast(*m_c), false));
        m_c->find(numeric_key(i + 2));
        ASSERT_OK(m_tree->erase(tree_cursor_cast(*m_c), false));
        m_c->find(numeric_key(i + 3));
        ASSERT_OK(m_tree->erase(tree_cursor_cast(*m_c), false));
        m_c->find(numeric_key(i + 4));
        ASSERT_OK(m_tree->erase(tree_cursor_cast(*m_c), false));
    }

    m_tree->deactivate_cursors(nullptr);
    auto cursor = make_cursor();
    for (size_t i = 0; i + 10 < kInitialRecordCount; i += 5) {
        cursor->seek(numeric_key(i + 1));
        ASSERT_EQ(numeric_key(i + 5), cursor->key());
        cursor->seek(numeric_key(i + 2));
        ASSERT_EQ(numeric_key(i + 5), cursor->key());
        cursor->seek(numeric_key(i + 3));
        ASSERT_EQ(numeric_key(i + 5), cursor->key());
        cursor->seek(numeric_key(i + 4));
        ASSERT_EQ(numeric_key(i + 5), cursor->key());
    }
}

TEST_P(CursorTests, SeeksForward)
{
    auto cursor = make_cursor();
    cursor->seek_first();
    size_t i = 0;

    while (cursor->is_valid()) {
        ASSERT_TRUE(cursor->is_valid());
        ASSERT_EQ(cursor->key(), numeric_key(i++));
        ASSERT_EQ(cursor->value(), make_value('*', true));
        cursor->next();
    }
    ASSERT_EQ(i, kInitialRecordCount);
}

TEST_P(CursorTests, SeeksForwardFromBoundary)
{
    auto cursor = make_cursor();
    cursor->seek(numeric_key(kInitialRecordCount / 4));
    while (cursor->is_valid()) {
        cursor->next();
    }
}

TEST_P(CursorTests, SeeksForwardToBoundary)
{
    auto cursor = make_cursor();
    auto bounds = make_cursor();
    cursor->seek_first();
    bounds->seek(numeric_key(kInitialRecordCount * 3 / 4));
    while (cursor->key() != bounds->key()) {
        ASSERT_TRUE(cursor->is_valid());
        cursor->next();
    }
}

TEST_P(CursorTests, SeeksForwardBetweenBoundaries)
{
    auto cursor = make_cursor();
    cursor->seek(numeric_key(kInitialRecordCount / 4));
    auto bounds = make_cursor();
    bounds->seek(numeric_key(kInitialRecordCount * 3 / 4));
    while (cursor->key() != bounds->key()) {
        ASSERT_TRUE(cursor->is_valid());
        cursor->next();
    }
}

TEST_P(CursorTests, SeeksBackward)
{
    auto cursor = make_cursor();
    cursor->seek_last();
    size_t i = 0;

    while (cursor->is_valid()) {
        ASSERT_EQ(cursor->key(), numeric_key(kInitialRecordCount - 1 - i++));
        ASSERT_EQ(cursor->value(), make_value('*', true));
        cursor->previous();
    }
    ASSERT_EQ(i, kInitialRecordCount);
}

TEST_P(CursorTests, SeeksBackwardFromBoundary)
{
    auto cursor = make_cursor();
    const auto bounds = kInitialRecordCount * 3 / 4;
    cursor->seek(numeric_key(bounds));
    for (size_t i = 0; i <= bounds; ++i) {
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
    bounds->seek(numeric_key(kInitialRecordCount / 4));
    while (cursor->key() != bounds->key()) {
        ASSERT_TRUE(cursor->is_valid());
        cursor->previous();
    }
}

TEST_P(CursorTests, SeeksBackwardBetweenBoundaries)
{
    auto cursor = make_cursor();
    auto bounds = make_cursor();
    cursor->seek(numeric_key(kInitialRecordCount * 3 / 4));
    bounds->seek(numeric_key(kInitialRecordCount / 4));
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
    for (size_t iteration = 0; iteration < 100; ++iteration) {
        const auto i = random.Next(kInitialRecordCount - 1);
        const auto key = numeric_key(i);
        cursor->seek(key);

        ASSERT_TRUE(cursor->is_valid());
        ASSERT_EQ(cursor->key(), key);

        for (size_t n = 0; n < random.Next(10); ++n) {
            cursor->next();

            if (const auto j = i + n + 1; j < kInitialRecordCount) {
                ASSERT_TRUE(cursor->is_valid());
                ASSERT_EQ(cursor->key(), numeric_key(j));
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
    for (size_t iteration = 0; iteration < 100; ++iteration) {
        const auto i = random.Next(kInitialRecordCount - 1);
        const auto key = numeric_key(i);
        cursor->seek(key);

        ASSERT_TRUE(cursor->is_valid());
        ASSERT_EQ(cursor->key(), key);

        for (size_t n = 0; n < random.Next(10); ++n) {
            cursor->previous();

            if (i > n) {
                ASSERT_TRUE(cursor->is_valid());
                ASSERT_EQ(cursor->key(), numeric_key(i - n - 1));
            } else {
                ASSERT_FALSE(cursor->is_valid());
                break;
            }
        }
    }
}

TEST_P(CursorTests, SeekOutOfRange)
{
    m_c->find(numeric_key(0));
    ASSERT_OK(m_tree->erase(tree_cursor_cast(*m_c), false));
    auto cursor = make_cursor();

    cursor->seek(numeric_key(0));
    ASSERT_TRUE(cursor->is_valid());
    ASSERT_EQ(cursor->key(), numeric_key(1));

    cursor->seek(numeric_key(kInitialRecordCount));
    ASSERT_FALSE(cursor->is_valid());
}

#if not NDEBUG
TEST_P(CursorTests, InvalidCursorDeathTest)
{
    auto cursor = make_cursor();
    ASSERT_DEATH((void)cursor->key(), "");
    ASSERT_DEATH((void)cursor->value(), "");
    ASSERT_DEATH((void)cursor->next(), "");
    ASSERT_DEATH((void)cursor->previous(), "");
}
#endif // NDEBUG

INSTANTIATE_TEST_SUITE_P(
    CursorTests,
    CursorTests,
    testing::Values(0, 1));

class MultiCursorTests : public TreeTests
{
protected:
    std::vector<CursorImpl *> m_cursors;

    void SetUp() override
    {
        TreeTests::SetUp();
        init_tree(*this, kInitLongValues);
    }

    void TearDown() override
    {
        while (!m_cursors.empty()) {
            del_cursor(0);
        }
        TreeTests::TearDown();
    }

    auto add_cursor() -> Cursor *
    {
        m_cursors.emplace_back(new (std::nothrow) CursorImpl(*m_tree));
        return m_cursors.back();
    }

    void del_cursor(size_t idx)
    {
        delete m_cursors.at(idx);
        m_cursors.erase(begin(m_cursors) + static_cast<std::ptrdiff_t>(idx));
    }
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
    m_c->find(make_normal_key(0));
    ASSERT_OK(m_tree->erase(tree_cursor_cast(*m_c), false));

    // Cursor isn't aware of modifications yet.
    ASSERT_EQ(cursor->key(), make_normal_key(0));
    ASSERT_EQ(cursor->value(), v0);

    ASSERT_OK(m_tree->insert(tree_cursor_cast(*m_c), make_normal_key(0), "value", false));
    ASSERT_EQ(cursor->key(), make_normal_key(0));
    ASSERT_EQ(cursor->value(), v0);
}

TEST_F(MultiCursorTests, CursorManagement)
{
    std::default_random_engine rng;
    for (size_t i = 1; i < 123; ++i) {
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
    for (size_t i = 1; i < kMinFrameCount * 10; ++i) {
        add_cursor();
    }
    for (auto *c : m_cursors) {
        c->seek_first();
    }
    for (size_t i = 0; i < m_cursors.size(); ++i) {
        for (size_t j = 0; m_cursors[i]->is_valid() && j < i; ++j) {
            // Spread the cursors out until too many page cache frames are occupied.
            m_cursors[i]->next();
        }
        ASSERT_OK(m_cursors[i]->status());
    }

    // Both put() and erase() cause live cursors to be saved.
    ASSERT_OK(m_tree->insert(tree_cursor_cast(*m_c), "key", "value", false));
    m_c->find("key");
    ASSERT_OK(m_tree->erase(tree_cursor_cast(*m_c), false));
}

TEST_F(MultiCursorTests, ModifyNodeWithCursors)
{
    m_c->seek_first();
    while (m_c->is_valid()) {
        ASSERT_OK(m_tree->erase(tree_cursor_cast(*m_c), false));
    }

    add_cursor();
    add_cursor();
    add_cursor();
    add_cursor();

    auto &c1 = *m_cursors.at(0);
    auto &c2 = *m_cursors.at(1);
    auto &c3 = *m_cursors.at(2);
    auto &c4 = *m_cursors.at(3);

    ASSERT_OK(m_tree->insert(tree_cursor_cast(c4), "a", make_value('1', true), false));
    ASSERT_OK(m_tree->insert(tree_cursor_cast(c4), "b", make_value('2', true), false));
    ASSERT_OK(m_tree->insert(tree_cursor_cast(c4), "c", make_value('3', true), false));

    c1.find("a");
    ASSERT_TRUE(c1.is_valid());
    c2.find("a");
    ASSERT_TRUE(c2.is_valid());
    c3.find("b");
    ASSERT_TRUE(c3.is_valid());

    const auto key_a = make_value('a', true);
    const auto key_b = make_value('b', true);
    const auto key_c = make_value('c', true);
    ASSERT_OK(m_tree->insert(tree_cursor_cast(c4), key_a, make_value('4', true), false));
    ASSERT_OK(m_tree->insert(tree_cursor_cast(c4), key_b, make_value('5', true), false));
    ASSERT_OK(m_tree->insert(tree_cursor_cast(c4), key_c, make_value('6', true), false));

    c4.find(key_a.c_str());
    ASSERT_TRUE(c4.is_valid());
    c4.previous();
    ASSERT_TRUE(c4.is_valid());
    ASSERT_EQ(c4.key(), c1.key());
    ASSERT_EQ(c4.key(), c2.key());

    c4.find(key_b.c_str());
    ASSERT_TRUE(c4.is_valid());
    c4.previous();
    ASSERT_TRUE(c4.is_valid());
    ASSERT_EQ(c4.key(), c3.key());
}

class PointerMapTests : public TreeTests
{
public:
    [[nodiscard]] auto map_size() -> size_t
    {
        return TEST_PAGE_SIZE / (sizeof(char) + sizeof(uint32_t));
    }
};

TEST_F(PointerMapTests, FirstPointerMapIsPage2)
{
    ASSERT_EQ(PointerMap::lookup(Id(1), TEST_PAGE_SIZE), Id(0));
    ASSERT_EQ(PointerMap::lookup(Id(2), TEST_PAGE_SIZE), Id(2));
    ASSERT_EQ(PointerMap::lookup(Id(3), TEST_PAGE_SIZE), Id(2));
    ASSERT_EQ(PointerMap::lookup(Id(4), TEST_PAGE_SIZE), Id(2));
    ASSERT_EQ(PointerMap::lookup(Id(5), TEST_PAGE_SIZE), Id(2));
}

TEST_F(PointerMapTests, ReadsAndWritesEntries)
{
    PageRef *page;
    ASSERT_OK(m_pager->allocate(page));
    m_pager->release(page);

    Status s;
    PointerMap::write_entry(*m_pager, Id(3), PointerMap::Entry{Id(33), kTreeNode}, s);
    ASSERT_OK(s);
    PointerMap::write_entry(*m_pager, Id(4), PointerMap::Entry{Id(44), kFreelistPage}, s);
    ASSERT_OK(s);
    PointerMap::write_entry(*m_pager, Id(5), PointerMap::Entry{Id(55), kOverflowLink}, s);
    ASSERT_OK(s);

    PointerMap::Entry entry_1, entry_2, entry_3;
    ASSERT_OK(PointerMap::read_entry(*m_pager, Id(3), entry_1));
    ASSERT_OK(PointerMap::read_entry(*m_pager, Id(4), entry_2));
    ASSERT_OK(PointerMap::read_entry(*m_pager, Id(5), entry_3));

    ASSERT_EQ(entry_1.back_ptr.value, 33);
    ASSERT_EQ(entry_2.back_ptr.value, 44);
    ASSERT_EQ(entry_3.back_ptr.value, 55);
    ASSERT_EQ(entry_1.type, kTreeNode);
    ASSERT_EQ(entry_2.type, kFreelistPage);
    ASSERT_EQ(entry_3.type, kOverflowLink);
}

TEST_F(PointerMapTests, PointerMapCanFitAllPointers)
{
    // PointerMap::find_map() expects the given pointer map page to be allocated already.
    for (size_t i = 0; i < map_size() * 2; ++i) {
        PageRef *page;
        ASSERT_OK(m_pager->allocate(page));
        m_pager->release(page);
    }

    for (size_t i = 0; i < map_size() + 10; ++i) {
        if (i != map_size()) {
            const Id id(i + 3);
            const PointerMap::Entry entry{Id(id.value + 1), kTreeNode};
            Status s;
            PointerMap::write_entry(*m_pager, id, entry, s);
            ASSERT_OK(s);
        }
    }
    for (size_t i = 0; i < map_size() + 10; ++i) {
        if (i != map_size()) {
            const Id id(i + 3);
            PointerMap::Entry entry;
            ASSERT_OK(PointerMap::read_entry(*m_pager, id, entry));
            ASSERT_EQ(entry.back_ptr.value - 1, id.value);
            ASSERT_EQ(entry.type, kTreeNode);
        }
    }
}

TEST_F(PointerMapTests, MapPagesAreRecognized)
{
    Id id(2);
    ASSERT_EQ(PointerMap::lookup(id, TEST_PAGE_SIZE), id);

    // Back pointers for the next "map.map_size()" pages are stored on page 2. The next pointermap page is
    // the page following the last page whose back pointer is on page 2. This pattern continues forever.
    for (size_t i = 0; i < 1'000'000; ++i) {
        id.value += static_cast<uint32_t>(map_size() + 1);
        ASSERT_EQ(PointerMap::lookup(id, TEST_PAGE_SIZE), id);
    }
}

TEST_F(PointerMapTests, FindsCorrectMapPages)
{
    size_t counter = 0;
    Id map_id(2);

    for (Id page_id(3); page_id.value <= 100 * map_size(); ++page_id.value) {
        if (counter++ == map_size()) {
            // Found a map page. Calls to find() with a page ID between this page and the next map page
            // should map to this page ID.
            map_id.value += static_cast<uint32_t>(map_size() + 1);
            counter = 0;
        } else {
            ASSERT_EQ(PointerMap::lookup(page_id, TEST_PAGE_SIZE), map_id);
        }
    }
}

TEST_F(PointerMapTests, LookupBeforeFirstMap)
{
    ASSERT_TRUE(PointerMap::lookup(Id(0), TEST_PAGE_SIZE).is_null());
    ASSERT_TRUE(PointerMap::lookup(Id(1), TEST_PAGE_SIZE).is_null());
}

TEST_F(PointerMapTests, ReadCorruption)
{
    PointerMap::Entry entry;
    ASSERT_NOK(PointerMap::read_entry(*m_pager, Id(0), entry));
    ASSERT_NOK(PointerMap::read_entry(*m_pager, Id(2), entry));
}

TEST_F(PointerMapTests, WriteCorruption)
{
    Status s, t;
    PointerMap::write_entry(*m_pager, Id(0), PointerMap::Entry(), s);
    ASSERT_NOK(s);
    PointerMap::write_entry(*m_pager, Id(2), PointerMap::Entry(), t);
    ASSERT_NOK(t);
}

class MultiTreeTests : public TreeTests
{
public:
    static constexpr size_t kN = 32;
    struct TreeWrapper {
        BucketPtr b;
        CursorPtr c;
        Tree *tree; // Owned by b
    };

    std::unique_ptr<Schema> m_schema;
    BucketPtr m_main;
    std::unordered_map<size_t, TreeWrapper> multi_tree;
    std::vector<std::string> payload_values;

    explicit MultiTreeTests()
        : payload_values(kInitialRecordCount)
    {
        for (auto &value : payload_values) {
            value = random.Generate(TEST_PAGE_SIZE / 2).to_string();
        }
    }

    ~MultiTreeTests() override = default;

    void SetUp() override
    {
        TreeTests::SetUp();
        m_schema = std::make_unique<Schema>(
            *m_pager,
            m_stat);
        m_main = std::make_unique<BucketImpl>(
            *m_schema,
            m_schema->main_tree());
    }

    void TearDown() override
    {
        multi_tree.clear();
        m_schema->close_trees();
        m_main.reset();
        m_schema.reset();
        m_pager->finish();
        TreeTests::TearDown();
    }

    auto tree_name(size_t tid)
    {
        // Makes sure keys for nested trees don't interfere with normal keys.
        return "tree_" + numeric_key(tid);
    }

    auto create_tree(size_t tid)
    {
        EXPECT_EQ(multi_tree.find(tid), end(multi_tree));

        Bucket *b;
        const auto name = tree_name(tid);
        EXPECT_OK(m_main->create_bucket(name, &b));
        auto *c = b->new_cursor();
        multi_tree.emplace(tid, TreeWrapper{BucketPtr(b), CursorPtr(c),
                                            &tree_cursor_cast(*c).tree()});
    }

    auto create_nested_tree(size_t parent_tid, size_t child_tid)
    {
        EXPECT_EQ(multi_tree.find(child_tid), end(multi_tree));
        auto itr = multi_tree.find(parent_tid);
        EXPECT_NE(itr, end(multi_tree));

        Bucket *b;
        const auto name = tree_name(child_tid);
        EXPECT_OK(itr->second.b->create_bucket(name, &b));
        auto *c = b->new_cursor();
        multi_tree.emplace(child_tid, TreeWrapper{BucketPtr(b), CursorPtr(c),
                                                  &tree_cursor_cast(*c).tree()});
    }

    auto fill_tree(size_t tid, bool shuffle = false)
    {
        auto &[b, c, tree] = multi_tree.at(tid);
        size_t indices[kInitialRecordCount];
        std::iota(std::begin(indices), std::end(indices), 0);
        if (shuffle) {
            std::default_random_engine rng(static_cast<unsigned int>(tid));
            std::shuffle(std::begin(indices), std::end(indices), rng);
        }
        for (auto idx : indices) {
            const auto key = make_long_key(idx);
            ASSERT_OK(b->put(key, ""));
        }
        for (auto idx : indices) {
            const auto key = make_long_key(idx);
            const auto value = payload_values[(idx + tid) % payload_values.size()];
            c->find(key);
            ASSERT_TRUE(c->is_valid());
            ASSERT_OK(b->put(*c, value));
            // Cursor is left on the modified record, even if there was a split.
            ASSERT_TRUE(c->is_valid());
            ASSERT_EQ(c->key(), key);
            ASSERT_EQ(c->value(), value);
        }
        ASSERT_OK(tree->check_integrity());
    }

    auto check_tree(size_t tid)
    {
        const auto &[b, c, tree] = multi_tree.at(tid);
        for (size_t i = 1; i < kInitialRecordCount; ++i) {
            c->find(make_long_key(i));
            ASSERT_TRUE(c->is_valid());
            ASSERT_EQ(c->value(), payload_values[(i + tid) % payload_values.size()]);
        }
    }

    auto clear_tree(size_t tid, bool only_clear_half = false)
    {
        const auto count = kInitialRecordCount >> only_clear_half;
        auto &[b, c, tree] = multi_tree.at(tid);
        for (size_t i = 0; i < count; ++i) {
            ASSERT_OK(b->erase(make_long_key(i)));
        }
    }

    auto drop_tree(size_t tid)
    {
        const auto name = tree_name(tid);
        ASSERT_NE(multi_tree.find(tid), end(multi_tree));

        // Allow buckets to be dropped while a handle is still open.
        ASSERT_OK(m_main->drop_bucket(name.c_str()));
        fill_tree(tid, true); // Handle is still usable
        multi_tree.erase(tid);
    }

    auto drop_nested_tree(size_t parent_tid, size_t child_tid)
    {
        EXPECT_NE(multi_tree.find(child_tid), end(multi_tree));
        auto itr = multi_tree.find(parent_tid);
        EXPECT_NE(itr, end(multi_tree));

        const auto key = tree_name(child_tid);
        // Allow buckets to be dropped while a handle is still open.
        ASSERT_OK(itr->second.b->drop_bucket(key.c_str()));
        fill_tree(child_tid, true); // Handle is still usable
    }

    auto close_nested_tree(size_t child_tid)
    {
        multi_tree.erase(child_tid);
    }

    void check_roots(size_t num_roots)
    {
        std::set<Id> roots;
        for (const auto &[name, tree] : multi_tree) {
            roots.insert(tree.tree->root());
        }
        ASSERT_EQ(roots.size(), num_roots);
        for (auto root : roots) {
            ASSERT_FALSE(PointerMap::is_map(root, TEST_PAGE_SIZE));
            // These tests shouldn't use more than a single pointer map page.
            ASSERT_EQ(PointerMap::lookup(root, TEST_PAGE_SIZE), Id(2));
            ASSERT_LE(root.value, 2 + num_roots);
        }
    }
};

TEST_F(MultiTreeTests, Create1)
{
    create_tree(0);
    check_roots(1);
}

TEST_F(MultiTreeTests, Create2)
{
    for (size_t i = 0; i < kN; ++i) {
        create_tree(i);
        check_roots(i + 1);
    }
}

TEST_F(MultiTreeTests, Create3)
{
    for (size_t i = 0; i < kN; ++i) {
        create_tree(i);
        fill_tree(i, i & 1);
        check_roots(i + 1);
    }
}

TEST_F(MultiTreeTests, Create4)
{
    for (size_t i = 0; i < kN; ++i) {
        create_tree(i);
        fill_tree(i, i & 1);
        clear_tree(i);
        check_roots(i + 1);
    }
}

TEST_F(MultiTreeTests, Create5)
{
    for (size_t i = 0; i < kN; ++i) {
        create_tree(i);
        fill_tree(i, i & 1);
        clear_tree(i, true);
        check_roots(i + 1);
    }
}

TEST_F(MultiTreeTests, TreesHaveIndependentKeys)
{
    create_tree(1);
    create_tree(2);

    auto &hello_tree = multi_tree.at(1);
    auto &world_tree = multi_tree.at(2);
    ASSERT_OK(hello_tree.b->put("same_key", "hello"));
    ASSERT_OK(world_tree.b->put("same_key", "world"));

    hello_tree.c->find("same_key");
    ASSERT_TRUE(hello_tree.c->is_valid());
    ASSERT_EQ(hello_tree.c->value(), "hello");
    world_tree.c->find("same_key");
    ASSERT_TRUE(world_tree.c->is_valid());
    ASSERT_EQ(world_tree.c->value(), "world");
}

TEST_F(MultiTreeTests, MultipleSplitsAndMergesA)
{
    for (size_t tid = 0; tid < kN; ++tid) {
        create_tree(tid);
    }
    for (size_t tid = 0; tid < kN; ++tid) {
        fill_tree(tid, tid & 1);
    }
    for (size_t tid = 0; tid < kN; ++tid) {
        check_tree(tid);
    }
    for (size_t tid = 0; tid < kN; ++tid) {
        clear_tree(tid);
    }
}

TEST_F(MultiTreeTests, MultipleSplitsAndMergesB)
{
    for (size_t tid = 0; tid < kN; ++tid) {
        create_tree(tid);
        fill_tree(tid, tid & 1);
        check_tree(tid);
        clear_tree(tid, tid & 2);
    }
}

TEST_F(MultiTreeTests, CannotDropNonexistentBucket)
{
    create_tree(0);
    auto &[b, c, tree] = multi_tree.at(0);
    ASSERT_TRUE(b->drop_bucket("nonexistent").is_invalid_argument());
}

TEST_F(MultiTreeTests, Drop1)
{
    create_tree(0);
    drop_tree(0);
    check_roots(0);
}

TEST_F(MultiTreeTests, Drop2)
{
    create_tree(0);
    fill_tree(0);
    drop_tree(0);
    check_roots(0);
}

TEST_F(MultiTreeTests, Drop3)
{
    create_tree(0);
    fill_tree(0, true);
    create_tree(1);
    drop_tree(0);
    fill_tree(1, false);
}

TEST_F(MultiTreeTests, Drop4)
{
    for (size_t i = 0; i < kN; ++i) {
        create_tree(i);
        if (i & 1) {
            fill_tree(i, i & 2);
        }
        drop_tree(i);
        check_roots(0);
    }
}

TEST_F(MultiTreeTests, Drop5)
{
    for (size_t i = 0; i < kN; ++i) {
        create_tree(i);
    }
    for (size_t i = 0; i < kN; ++i) {
        drop_tree(i);
        check_roots(kN - i - 1);
    }
}

TEST_F(MultiTreeTests, Drop6)
{
    for (size_t i = 0; i < 2; ++i) {
        create_tree(1);
        fill_tree(1);
        drop_tree(1);
    }
    check_roots(0);

    create_tree(1);
    fill_tree(1);
    check_tree(1);
    clear_tree(1);

    check_roots(1);
}

TEST_F(MultiTreeTests, DropSequential)
{
    for (size_t tid = 0; tid < kN; ++tid) {
        create_tree(tid);
        fill_tree(tid);
    }
    for (size_t tid = 0; tid < kN; ++tid) {
        drop_tree(tid);
    }
    for (size_t tid = 0; tid < kN; ++tid) {
        create_tree(tid);
        fill_tree(tid);
        check_tree(tid);
        clear_tree(tid);
    }
}

TEST_F(MultiTreeTests, DropRandom)
{
    std::vector<size_t> order(kN);
    std::iota(begin(order), end(order), 0);
    std::default_random_engine rng(42);
    std::shuffle(begin(order), end(order), rng);

    for (size_t tid = 0; tid < kN; ++tid) {
        create_tree(tid);
        fill_tree(tid);
    }
    for (auto tid : order) {
        drop_tree(tid);
    }
    for (auto tid : order) {
        create_tree(tid);
        fill_tree(tid);
        check_tree(tid);
        clear_tree(tid);
    }
}

TEST_F(MultiTreeTests, SavedCursors)
{
    std::vector<size_t> tids;
    std::vector<Cursor *> cs;

    for (int iteration = 0; iteration < 50; ++iteration) {
        // Create a new tree and add some records.
        tids.emplace_back(tids.size());
        create_tree(tids.back());
        for (size_t i = 0; i < kInitialRecordCount; ++i) {
            auto &[b, c, tree] = multi_tree.at(tids.back());
            const auto value = payload_values[i];
            ASSERT_OK(b->put(make_long_key(i), value));
        }
        // Advance or wrap the cursors, all of which should be live again (not "saved").
        for (auto *c : cs) {
            if (c->is_valid()) {
                c->next();
            } else if (c->status().is_ok()) {
                c->seek_first();
            } else {
                ASSERT_OK(c->status());
                break;
            }
        }
    }

    for (const auto *c : cs) {
        delete c;
    }
}

TEST_F(MultiTreeTests, VacuumRoots)
{
    for (size_t i = 0; i < kN; ++i) {
        create_tree(i);
        fill_tree(i);
    }

    create_tree(kN);
    size_t num_roots = kN + 1;
    for (size_t i = 0; i < kN; ++i) {
        if (i == kN / 3) {
            fill_tree(kN);
        } else if (i == 2 * kN / 3) {
            clear_tree(kN, true);
        }
        if (i & 1) {
            drop_tree(i);
            --num_roots;
        } else {
            clear_tree(i, i & 2);
        }
    }

    ASSERT_OK(m_schema->vacuum());
    check_roots(num_roots);
}

TEST_F(MultiTreeTests, CreateNested1)
{
    create_tree(0);
    create_nested_tree(0, 1);
    check_roots(2);
}

TEST_F(MultiTreeTests, CreateNested2)
{
    create_tree(0);
    create_nested_tree(0, 1);
    fill_tree(0);
    fill_tree(1);
    check_roots(2);

    check_tree(0);
    check_tree(1);
}

TEST_F(MultiTreeTests, CreateNested3)
{
    create_tree(0);
    // Fill first, so that some page in 0 must be moved to make room for the root page
    // for tree 1.
    fill_tree(0);
    create_nested_tree(0, 1);
    fill_tree(1);
    check_roots(2);

    check_tree(0);
    check_tree(1);
}

TEST_F(MultiTreeTests, CreateNested4)
{
    size_t tid = 1;
    create_tree(0);

    fill_tree(0);
    create_nested_tree(0, tid++);
    create_nested_tree(0, tid++);
    create_nested_tree(0, tid++);

    fill_tree(1);
    create_nested_tree(1, tid++);
    create_nested_tree(1, tid++);
    create_nested_tree(1, tid++);

    fill_tree(2);
    create_nested_tree(2, tid++);
    create_nested_tree(2, tid++);
    create_nested_tree(2, tid++);

    fill_tree(3);
    create_nested_tree(3, tid++);
    create_nested_tree(3, tid++);
    create_nested_tree(3, tid++);

    for (size_t i = 0; i < tid; ++i) {
        fill_tree(i);
        check_tree(i);
    }
    check_roots(tid);
}

TEST_F(MultiTreeTests, DropNested1)
{
    create_tree(0);
    create_nested_tree(0, 1);
    drop_nested_tree(0, 1);
    close_nested_tree(1);
    check_roots(1);
}

TEST_F(MultiTreeTests, DropNested2)
{
    create_tree(0);
    fill_tree(0);

    create_nested_tree(0, 1);
    create_nested_tree(1, 2);
    create_nested_tree(2, 3);

    drop_nested_tree(0, 1);
    drop_nested_tree(1, 2);
    drop_nested_tree(2, 3);

    close_nested_tree(1);
    close_nested_tree(2);
    close_nested_tree(3);

    check_roots(1);
}

TEST_F(MultiTreeTests, DropNested3)
{
    create_tree(0);
    fill_tree(0);
    for (size_t i = 0; i < kN - 1; ++i) {
        create_nested_tree(i, i + 1);
    }
    for (size_t i = 0; i < kN - 1; ++i) {
        drop_nested_tree(i, i + 1);
    }
    for (size_t i = 1; i < kN; ++i) {
        close_nested_tree(i);
    }
    check_roots(1);
}

TEST_F(MultiTreeTests, DropNested4)
{
    create_tree(0);
    fill_tree(0);
    for (size_t i = 0; i < kN - 1; ++i) {
        create_nested_tree(i, i + 1);
        drop_nested_tree(i, i + 1);
    }
    for (size_t i = 1; i < kN; ++i) {
        close_nested_tree(i);
    }
    check_roots(1);
}

template <class T>
class PermutationGenerator
{
    std::vector<T> m_values;
    std::vector<size_t> m_indices;

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
      public testing::TestWithParam<uint32_t>
{
public:
    static constexpr size_t kValueSizes[] = {10, 100, 500, TEST_PAGE_SIZE};
    ~RebalanceTests() override = default;

    void SetUp() override
    {
        open();
    }
    void TearDown() override
    {
        close();
    }

    struct RecordInfo {
        size_t key = 0;
        size_t value_size = 0;

        auto operator<(const RecordInfo &rhs) const -> bool
        {
            return key < rhs.key;
        }
    };

    void run(const std::vector<size_t> &size_idx)
    {
        std::vector<RecordInfo> info;
        info.reserve(size_idx.size());
        for (size_t i = 0; i < size_idx.size(); ++i) {
            info.emplace_back(RecordInfo{i, kValueSizes[size_idx[i]]});
        }
        PermutationGenerator<RecordInfo> generator(info);
        while (generator(info)) {
            size_t iteration = 0;
            for (size_t i = 0; i < GetParam(); ++i) {
                for (const auto &[k, value_size] : info) {
                    ASSERT_OK(m_tree->insert(tree_cursor_cast(*m_c),
                                             numeric_key(iteration * info.size() + k),
                                             m_random.Generate(value_size),
                                             false));
                }
                ++iteration;
            }
            validate();

            iteration = 0;
            for (size_t i = 0; i < GetParam(); ++i) {
                for (const auto &[k, _] : info) {
                    m_c->find(numeric_key(iteration * info.size() + k));
                    ASSERT_OK(m_tree->erase(tree_cursor_cast(*m_c), false));
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
    testing::Values(1, 2, 5));

TEST(SuffixTruncationTests, SuffixTruncation)
{
    const auto checked_truncate_suffix = [](const auto &lhs, const auto &rhs) {
        Slice prefix;
        EXPECT_EQ(0, truncate_suffix(lhs, rhs, prefix));
        // Allows lhs == rhs due to supporting duplicate keys. rhs is returned in this case.
        // Invariant: lhs <= prefix <= rhs
        EXPECT_FALSE(prefix.is_empty());
        EXPECT_LE(lhs, prefix);
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

    // Support for duplicate keys.
    ASSERT_EQ("10", checked_truncate_suffix("10", "10"));
}

TEST(SuffixTruncationTests, SuffixTruncationCorruption)
{
    Slice prefix;
    ASSERT_EQ(-1, truncate_suffix("43", "42", prefix));
}

class CursorModificationTests
    : public TreeTestHarness,
      public testing::Test
{
public:
    ~CursorModificationTests() override = default;

    void SetUp() override
    {
        open();
    }
    void TearDown() override
    {
        close();
    }

    void test_sequential_overwrite(size_t size_step, bool forward)
    {
        for (size_t i = 0; i < kInitialRecordCount; ++i) {
            ASSERT_OK(m_tree->insert(tree_cursor_cast(*m_c), numeric_key(i), "", false));
        }

        static constexpr size_t kIterations = 5;
        for (size_t iteration = 0; iteration < kIterations; ++iteration) {
            if (forward) {
                m_c->seek_first();
            } else {
                m_c->seek_last();
            }

            for (size_t i = 0; m_c->is_valid(); ++i) {
                const std::string value((iteration + 1) * size_step, '*');
                ASSERT_OK(m_tree->insert(tree_cursor_cast(*m_c), m_c->key(), value, false))
                    << iteration << ':' << i;

                if (forward) {
                    m_c->next();
                } else {
                    m_c->previous();
                }
            }
            ASSERT_OK(m_c->status());
            validate();
        }

        m_c->seek_first();
        for (size_t i = 0; i < kInitialRecordCount; ++i) {
            ASSERT_TRUE(m_c->is_valid());
            ASSERT_EQ(m_c->key(), numeric_key(i));
            ASSERT_EQ(m_c->value(), std::string(size_step * kIterations, '*'));
            m_c->next();
        }
        ASSERT_FALSE(m_c->is_valid());
        ASSERT_OK(m_c->status());
    }
};

TEST_F(CursorModificationTests, QuickCheck)
{
    for (std::ptrdiff_t i = 0; i < 2; ++i) {
        for (const auto *key : {"BB", "CC", "AA"}) {
            const auto *value = key + i;
            ASSERT_OK(m_tree->insert(tree_cursor_cast(*m_c), key, value, false));
            tree_cursor_cast(*m_c).read_record();
            ASSERT_TRUE(m_c->is_valid());
            ASSERT_EQ(Slice(key), m_c->key());
            ASSERT_EQ(Slice(value), m_c->value());
        }
    }

    for (const auto *key : {"AA", "BB", "CC"}) {
        ASSERT_TRUE(m_c->is_valid());
        ASSERT_EQ(Slice(key), m_c->key());
        ASSERT_EQ(Slice(key + 1), m_c->value());
        ASSERT_OK(m_tree->erase(tree_cursor_cast(*m_c), false));
        if (m_c->is_valid()) {
            tree_cursor_cast(*m_c).read_record();
        }
    }

    ASSERT_FALSE(m_c->is_valid());
}

TEST_F(CursorModificationTests, PayloadSlicesAreStable)
{
    TreeCursor c(*m_tree);
    ASSERT_OK(m_tree->insert(c, "key", "value", false));
    c.read_record();
    ASSERT_TRUE(c.is_valid());
    const auto stable_key = reinterpret_cast<uintptr_t>(c.key().data());
    const auto stable_value = reinterpret_cast<uintptr_t>(c.value().data());

    m_c->find("key");
    ASSERT_OK(m_tree->erase(tree_cursor_cast(*m_c), false));

    ASSERT_TRUE(c.is_valid());
    ASSERT_EQ(stable_key, reinterpret_cast<uintptr_t>(c.key().data()));
    ASSERT_EQ(stable_value, reinterpret_cast<uintptr_t>(c.value().data()));
}

TEST_F(CursorModificationTests, SeekAndPut)
{
    auto num_records = kInitialRecordCount;
    for (size_t i = 0; i < num_records; ++i) {
        ASSERT_OK(m_tree->insert(tree_cursor_cast(*m_c), make_long_key(i * 2), make_value('*'), false));
    }
    for (size_t iteration = 0; iteration < 2; ++iteration) {
        auto step = num_records / 10;
        if (iteration == 0) {
            m_c->seek_first();
        } else {
            m_c->seek_last();
        }
        for (size_t i = 0; m_c->is_valid() && i < kInitialRecordCount; ++i) {
            ASSERT_OK(m_tree->insert(tree_cursor_cast(*m_c), make_long_key(i * 2 + iteration), make_value('*', true), false));
            ++num_records;
            for (size_t j = 0; m_c->is_valid() && j < step; ++j) {
                if (iteration == 0) {
                    m_c->next();
                } else {
                    m_c->previous();
                }
            }
        }
        ASSERT_OK(m_c->status());
        ASSERT_FALSE(m_c->is_valid());
        validate();
    }
}

TEST_F(CursorModificationTests, EraseAllRecordsFromLeft)
{
    init_tree(*this, kInitLongKeys | kInitLongValues);
    m_c->seek_first();
    for (size_t i = 0; i < kInitialRecordCount; ++i) {
        ASSERT_TRUE(m_c->is_valid());
        ASSERT_OK(m_tree->erase(tree_cursor_cast(*m_c), false));
    }
    ASSERT_FALSE(m_c->is_valid());
    validate();
}

TEST_F(CursorModificationTests, EraseAllRecordsFromRight)
{
    init_tree(*this, kInitLongKeys | kInitLongValues);
    for (size_t i = 0; i < kInitialRecordCount; ++i) {
        m_c->seek_last();
        // Cursor immediately falls off the edge of the key range.
        ASSERT_OK(m_tree->erase(tree_cursor_cast(*m_c), false));
        ASSERT_FALSE(m_c->is_valid());
    }
    validate();
}

TEST_F(CursorModificationTests, EraseSecondToLast)
{
    init_tree(*this, kInitLongKeys | kInitLongValues);
    m_c->seek_last();
    for (size_t i = 0; i < kInitialRecordCount; ++i) {
        m_c->previous();
        if (!m_c->is_valid()) {
            ASSERT_EQ(i, kInitialRecordCount - 1);
            m_c->seek_first();
        }
        ASSERT_OK(m_tree->erase(tree_cursor_cast(*m_c), false));
        tree_cursor_cast(*m_c).read_record();
        CALICODB_EXPECT_EQ(m_c->is_valid(), i < kInitialRecordCount - 1);
    }
    ASSERT_FALSE(m_c->is_valid());
    validate();
}

TEST_F(CursorModificationTests, SizeDiscrepancy1)
{
    static constexpr size_t kN = 100;
    for (size_t iteration = 0; iteration < 5; ++iteration) {
        const auto offset = kN * iteration;
        const auto first_key = numeric_key(offset) + make_long_key(0);
        ASSERT_OK(m_tree->insert(tree_cursor_cast(*m_c), first_key, "first_value", false));
        for (size_t i = 0; i < kN; ++i) {
            ASSERT_OK(m_tree->insert(tree_cursor_cast(*m_c), numeric_key(i + offset + 1), make_value('*'), false));
            for (size_t j = 0; j <= i; ++j) {
                ASSERT_TRUE(m_c->is_valid());
                m_c->previous(); // Repositions the tree cursor and reads the payload
            }
            ASSERT_TRUE(m_c->is_valid());
            ASSERT_EQ(m_c->key(), first_key);
            ASSERT_EQ(m_c->value(), "first_value");
        }
        validate();
    }
}

TEST_F(CursorModificationTests, SizeDiscrepancy2)
{
    static constexpr size_t kN = 100;
    for (size_t iteration = 0; iteration < 5; ++iteration) {
        const auto offset = (iteration + 1) * kN;
        const auto last_key = numeric_key(offset - 1) + make_long_key(0);
        ASSERT_OK(m_tree->insert(tree_cursor_cast(*m_c), last_key, "last_value", false));
        for (size_t i = 0; i < kN; ++i) {
            ASSERT_OK(m_tree->insert(tree_cursor_cast(*m_c), numeric_key(offset - i - 1), make_value('*'), false));
            for (size_t j = 0; j <= i; ++j) {
                ASSERT_TRUE(m_c->is_valid());
                m_c->next(); // Repositions the tree cursor and reads the payload
            }
            ASSERT_TRUE(m_c->is_valid());
            ASSERT_EQ(m_c->key(), last_key);
            ASSERT_EQ(m_c->value(), "last_value");
        }
        validate();
    }
}

TEST_F(CursorModificationTests, SeekAndEraseForward)
{
    init_tree(*this, kInitLongKeys | kInitLongValues);
    auto num_records = kInitialRecordCount;
    while (num_records > 0) {
        const auto step = num_records / 4;
        m_c->seek_first();
        while (num_records > 0 && m_c->is_valid()) {
            ASSERT_OK(m_tree->erase(tree_cursor_cast(*m_c), false));
            --num_records;
            for (size_t i = 0; m_c->is_valid() && i < step; ++i) {
                m_c->next();
            }
        }
        ASSERT_OK(m_c->status());
        ASSERT_FALSE(m_c->is_valid());
        validate();
    }
}

TEST_F(CursorModificationTests, SeekAndEraseBackward)
{
    init_tree(*this, kInitLongKeys | kInitLongValues);
    auto num_records = kInitialRecordCount;
    while (num_records > 0) {
        const auto step = num_records / 4;
        m_c->seek_last();
        for (auto first = true; num_records > 0 && m_c->is_valid();) {
            ASSERT_OK(m_tree->erase(tree_cursor_cast(*m_c), false));
            if (first) {
                // Erasing the last record causes the cursor to immediately fall off the
                // edge of the key range.
                ASSERT_FALSE(m_c->is_valid());
                m_c->seek_last();
                first = false;
            }
            --num_records;
            for (size_t i = 0; m_c->is_valid() && i < step; ++i) {
                m_c->previous();
            }
        }
        ASSERT_OK(m_c->status());
        ASSERT_FALSE(m_c->is_valid());
        validate();
    }
}

TEST_F(CursorModificationTests, OverwriteForward1)
{
    test_sequential_overwrite(1, true);
}

TEST_F(CursorModificationTests, OverwriteForward2)
{
    test_sequential_overwrite(10, true);
}

TEST_F(CursorModificationTests, OverwriteForward3)
{
    test_sequential_overwrite(100, true);
}

TEST_F(CursorModificationTests, OverwriteForward4)
{
    test_sequential_overwrite(500, true);
}

TEST_F(CursorModificationTests, OverwriteBackward1)
{
    test_sequential_overwrite(1, false);
}

TEST_F(CursorModificationTests, OverwriteBackward2)
{
    test_sequential_overwrite(10, false);
}

TEST_F(CursorModificationTests, OverwriteBackward3)
{
    test_sequential_overwrite(100, false);
}

TEST_F(CursorModificationTests, OverwriteBackward4)
{
    test_sequential_overwrite(500, false);
}

TEST_F(CursorModificationTests, OverwriteRandom)
{
    for (size_t i = 0; i < kInitialRecordCount; ++i) {
        ASSERT_OK(m_tree->insert(tree_cursor_cast(*m_c), numeric_key(i), "", false));
    }

    static constexpr size_t kSize = 250;
    static constexpr size_t kNumIterations = 5;
    for (size_t iteration = 0; iteration < kNumIterations; ++iteration) {
        if (iteration == 0) {
            m_c->seek_first();
        } else {
            m_c->seek_last();
        }
        while (m_c->is_valid()) {
            const std::string value((iteration + 1) * kSize, '*');
            ASSERT_OK(m_tree->insert(tree_cursor_cast(*m_c), m_c->key(), value, false));

            if (iteration == 0) {
                m_c->next();
            } else {
                m_c->previous();
            }
        }
        ASSERT_OK(m_c->status());
        validate();
    }

    m_c->seek_first();
    for (size_t i = 0; i < kInitialRecordCount; ++i) {
        ASSERT_TRUE(m_c->is_valid());
        ASSERT_EQ(m_c->key(), numeric_key(i));
        ASSERT_EQ(m_c->value(), std::string(kSize * kNumIterations, '*'));
        m_c->next();
    }
    ASSERT_FALSE(m_c->is_valid());
    ASSERT_OK(m_c->status());
}

TEST_F(CursorModificationTests, OverwriteExactSize)
{
    static constexpr size_t kNumIterations = 5;
    for (size_t iteration = 0; iteration < kNumIterations; ++iteration) {
        std::string target(64U << iteration, static_cast<char>('0' + iteration));
        for (size_t i = 0; i < kInitialRecordCount; ++i) {
            put_u64(target.data(), i);
            ASSERT_OK(m_tree->insert(tree_cursor_cast(*m_c), numeric_key(i), target, false));
        }
        for (size_t i = 0; i < kInitialRecordCount; ++i) {
            put_u64(target.data(), i);
            m_c->seek(numeric_key(i));
            ASSERT_TRUE(m_c->is_valid());
            ASSERT_EQ(m_c->key(), numeric_key(i));
            ASSERT_EQ(m_c->value(), target);
            m_c->next();
        }
    }
    ASSERT_OK(m_c->status());
}

TEST_F(CursorModificationTests, UntrackedCursors)
{
    init_tree(*this, kInitLongValues);

    auto c1 = std::make_unique<CursorImpl>(*m_tree);
    auto c2 = std::make_unique<CursorImpl>(*m_tree);
    c1->seek_first();
    c2->seek_last();

    for (size_t i = 0; i < kInitialRecordCount; ++i) {
        m_c->find(make_normal_key(i));
        ASSERT_OK(m_tree->erase(tree_cursor_cast(*m_c), false));
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

class FreelistTests : public TreeTests
{
public:
    std::vector<Id> m_page_ids;
    std::vector<Id> m_ordering;
    std::default_random_engine m_rng;

    explicit FreelistTests()
        : m_rng(42)
    {
    }

    void SetUp() override
    {
        TreeTests::SetUp();
    }

    void shuffle_order()
    {
        std::shuffle(begin(m_ordering), end(m_ordering), m_rng);
    }

    static constexpr size_t kFreelistLen = TEST_PAGE_SIZE * 5;
    void populate_freelist(bool shuffle)
    {
        PageRef *page;
        for (size_t i = 0; i < kFreelistLen; ++i) {
            ASSERT_OK(m_tree->allocate(Tree::kAllocateAny, Id::null(), page));
            m_page_ids.push_back(page->page_id);
            m_pager->release(page);
        }
        m_ordering = m_page_ids;
        if (shuffle) {
            shuffle_order();
        }
        for (auto id : m_ordering) {
            ASSERT_OK(m_pager->acquire(id, page));
            ASSERT_OK(Freelist::add(*m_pager, page));
        }
    }

    auto test_pop_any()
    {
        PageRef *page;
        std::vector<Id> freelist_page_ids(m_page_ids.size());
        for (size_t i = 0; i < m_page_ids.size(); ++i) {
            ASSERT_OK(Freelist::remove(*m_pager, Freelist::kRemoveAny,
                                       Id::null(), page));
            ASSERT_NE(page, nullptr);
            freelist_page_ids[i] = page->page_id;
            m_pager->release(page);
        }
        std::sort(begin(freelist_page_ids), end(freelist_page_ids));
        ASSERT_EQ(freelist_page_ids, m_page_ids);
        ASSERT_OK(m_pager->commit());
    }

    auto test_pop_exact_found()
    {
        std::vector<Id> freelist_page_ids;
        shuffle_order();
        PageRef *page;
        for (auto exact : m_ordering) {
            freelist_page_ids.emplace_back(exact);
            ASSERT_OK(Freelist::remove(*m_pager, Freelist::kRemoveExact,
                                       freelist_page_ids.back(), page))
                << "failed to pop page " << exact.value;
            ASSERT_FALSE(freelist_page_ids.back().is_null());
            ASSERT_EQ(freelist_page_ids.back(), exact);
            m_pager->release(page);
        }
        std::sort(begin(freelist_page_ids), end(freelist_page_ids));
        ASSERT_EQ(freelist_page_ids, m_page_ids);
        ASSERT_OK(m_pager->commit());
    }

    auto test_pop_exact_not_found()
    {
        PageRef *page;
        for (size_t i = 0; i < m_ordering.size(); i += 2) {
            ASSERT_OK(Freelist::remove(*m_pager, Freelist::kRemoveExact,
                                       m_ordering[i], page))
                << "failed to pop page " << m_ordering[i].value;
            m_pager->release(page);
        }

        for (size_t i = 0; i < m_ordering.size(); i += 2) {
            auto s = Freelist::remove(*m_pager, Freelist::kRemoveExact,
                                      m_ordering[i], page);
            ASSERT_TRUE(s.is_corruption()) << s.message();
            ASSERT_EQ(page, nullptr);
        }
    }
};

TEST_F(FreelistTests, PopAnySequential)
{
    populate_freelist(false);
    test_pop_any();
}

TEST_F(FreelistTests, PopAnyRandom)
{
    populate_freelist(true);
    test_pop_any();
}

TEST_F(FreelistTests, PopExactSequentialFound)
{
    populate_freelist(false);
    test_pop_exact_found();
}

TEST_F(FreelistTests, PopExactRandomFound)
{
    populate_freelist(true);
    test_pop_exact_found();
}

TEST_F(FreelistTests, PopExactSequentialNotFound)
{
    populate_freelist(false);
    test_pop_exact_not_found();
}

TEST_F(FreelistTests, PopExactRandomNotFound)
{
    populate_freelist(true);
    test_pop_exact_not_found();
}

TEST_F(FreelistTests, FreelistCorruption)
{
    PageRef *page;
    ASSERT_OK(m_tree->allocate(Tree::kAllocateAny, Id::null(), page));
    page->page_id.value = m_pager->page_count() + 1;
    ASSERT_NOK(Freelist::add(*m_pager, page));
    auto *root = &m_pager->get_root();
    ASSERT_NOK(Freelist::add(*m_pager, root));
}

class VacuumTests : public MultiTreeTests
{
public:
    CursorImpl *m_root_c = nullptr;
    Tree *m_root = nullptr;

    explicit VacuumTests() = default;

    ~VacuumTests() override = default;

    void SetUp() override
    {
        MultiTreeTests::SetUp();
        m_root = m_tree;
        m_root_c = m_c;
        create_tree(1);
        m_tree = multi_tree.at(1).tree;
        m_c = static_cast<CursorImpl *>(multi_tree.at(1).c.get());
    }

    void TearDown() override
    {
        m_tree = m_root;
        m_c = m_root_c;
        MultiTreeTests::TearDown();
    }
};

TEST_F(VacuumTests, VacuumEmptyTree)
{
    ASSERT_OK(m_schema->vacuum());
    validate();
}

TEST_F(VacuumTests, VacuumFreelist)
{
    for (InitFlag flag = kInitNormal; flag <= kInitFlagMax; ++flag) {
        init_tree(*this, flag);

        m_c->seek_first();
        while (m_c->is_valid()) {
            ASSERT_OK(m_tree->erase(tree_cursor_cast(*m_c), false));
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
    ASSERT_OK(m_tree->insert(tree_cursor_cast(*m_c), kv[0][0], kv[0][1], false));
    ASSERT_OK(m_tree->insert(tree_cursor_cast(*m_c), kv[1][0], kv[1][1], false));
    ASSERT_OK(m_tree->insert(tree_cursor_cast(*m_c), kv[2][0], kv[2][1], false));

    for (size_t i = 0; i < kInitialRecordCount; ++i) {
        m_c->find(make_normal_key(i));
        ASSERT_OK(m_tree->erase(tree_cursor_cast(*m_c), false));
    }

    ASSERT_OK(m_schema->vacuum());
    validate();

    m_c->find(kv[0][0]);
    ASSERT_TRUE(m_c->is_valid());
    ASSERT_EQ(m_c->value(), kv[0][1]);
    m_c->find(kv[1][0]);
    ASSERT_TRUE(m_c->is_valid());
    ASSERT_EQ(m_c->value(), kv[1][1]);
    m_c->find(kv[2][0]);
    ASSERT_TRUE(m_c->is_valid());
    ASSERT_EQ(m_c->value(), kv[2][1]);
}

TEST_F(VacuumTests, VacuumPartialRange)
{
    for (size_t i = 0; i < 2; ++i) {
        init_tree(*this, kInitLongKeys | (i == 0 ? 0 : kInitLongValues));

        m_c->seek_first();
        const auto batch_size = kInitialRecordCount / 3 * (i + 1);
        for (size_t n = 0; m_c->is_valid() && n < batch_size; ++n) {
            ASSERT_OK(m_tree->erase(tree_cursor_cast(*m_c), false));
        }

        ASSERT_OK(m_schema->vacuum());
        validate();

        for (size_t n = batch_size; n < kInitialRecordCount; ++n) {
            m_c->find(make_long_key(n));
            ASSERT_TRUE(m_c->is_valid());
        }
    }
}

} // namespace calicodb::test