
#include <map>
#include <unordered_map>

#include <gtest/gtest.h>

#include "calico/cursor.h"
#include "pager/pager.h"
#include "tree/bplus_tree.h"
#include "tree/node_pool.h"
#include "utils/layout.h"
#include "utils/logging.h"

#include "fakes.h"
#include "pager/basic_pager.h"
#include "random.h"
#include "tools.h"
#include "validation.h"
#include "wal/basic_wal.h"

namespace {

using namespace cco;

class TestHarness: public testing::Test {
public:
    static constexpr Size PAGE_SIZE {0x100};
    static constexpr Size FRAME_COUNT {16};
    static constexpr auto CACHE_SIZE = PAGE_SIZE * FRAME_COUNT;

    TestHarness()
        : wal {std::make_unique<DisabledWriteAheadLog>()},
          store {std::make_unique<MockStorage>()}
    {
        store->delegate_to_real();
        EXPECT_CALL(*store, open_random_access_editor).Times(1);

        pager = *BasicPager::open({
            *store,
            *wal,
            create_sink(),
            FRAME_COUNT,
            PAGE_SIZE
        });
        mock = store->get_mock_random_access_editor(DATA_FILENAME);
        mock->delegate_to_real();
        CCO_EXPECT_NE(mock, nullptr);
    }

    Random random {0};
    std::unique_ptr<DisabledWriteAheadLog> wal;
    std::unique_ptr<MockStorage> store;
    std::unique_ptr<Pager> pager;
    MockRandomAccessEditor *mock {};
};

class TreeTests: public TestHarness {
public:
    static constexpr Size PAGE_SIZE = 0x100;

    TreeTests()
    {
        max_local = get_max_local(PAGE_SIZE);
        tree = *BPlusTree::open(
            *pager,
            create_sink(),
            PAGE_SIZE
        );

        using testing::AtLeast;
        EXPECT_CALL(*mock, read).Times(AtLeast(0));
        EXPECT_CALL(*mock, write).Times(AtLeast(0));
    }

    ~TreeTests() override = default;

    auto validate() const
    {
//        validate_ordering(*tree);
//        validate_siblings(*tree);
//        validate_links(*tree);
    }

    std::unique_ptr<Tree> tree;
    Size max_local {};
};
//
TEST_F(TreeTests, NewTreeSetUpCorrectly)
{
    ASSERT_EQ(pager->page_count(), 1);
    ASSERT_EQ(tree->cell_count(), 0);
    auto cursor = tree->find_minimum();
    ASSERT_FALSE(cursor.is_valid());
    ASSERT_TRUE(cursor.status().is_not_found());
    cursor = tree->find_maximum();
    ASSERT_FALSE(cursor.is_valid());
    ASSERT_TRUE(cursor.status().is_not_found());
}

TEST_F(TreeTests, InsertCell)
{
    tools::insert(*tree, "key", "value");
    ASSERT_TRUE(tools::find_exact(*tree, "key").is_valid());
}

TEST_F(TreeTests, OnlyAcceptsValidKeySizes)
{
    ASSERT_TRUE(tree->insert(stob(""), stob("value")).error().is_invalid_argument());
    ASSERT_TRUE(tree->insert(stob(std::string(max_local + 1, 'x')), stob("value")).error().is_invalid_argument());
}

TEST_F(TreeTests, RemoveRecord)
{
    std::string unused;
    tools::insert(*tree, "key", "value");
    ASSERT_TRUE(tools::erase(*tree, "key"));
    ASSERT_FALSE(tools::find_exact(*tree, "key").is_valid());
}

TEST_F(TreeTests, InsertBefore)
{
    tools::insert(*tree, "2", "b");
    tools::insert(*tree, "1", "a");
    validate();
}

TEST_F(TreeTests, InsertAfter)
{
    tools::insert(*tree, "1", "a");
    tools::insert(*tree, "2", "b");
    validate();
}

TEST_F(TreeTests, InsertBetween)
{
    tools::insert(*tree, "1", "a");
    tools::insert(*tree, "3", "c");
    tools::insert(*tree, "2", "b");
    validate();
}

TEST_F(TreeTests, OverflowChains)
{
    const auto value_a = random_string(random, max_local, max_local * 10);
    const auto value_b = random_string(random, max_local, max_local * 20);
    const auto value_c = random_string(random, max_local, max_local * 30);

    tools::insert(*tree, "1", value_a);
    tools::insert(*tree, "2", value_b);
    tools::insert(*tree, "3", value_c);

    ASSERT_TRUE(tools::contains(*tree, "1", value_a));
    ASSERT_TRUE(tools::contains(*tree, "2", value_b));
    ASSERT_TRUE(tools::contains(*tree, "3", value_c));
    validate();
}

auto insert_sequence(Tree &tree, Index start, Index n, Index step = 1)
{
    for (auto i = start; i < n; i += step)
        tools::insert(tree, make_key(i), "");
}

TEST_F(TreeTests, SequentialInserts)
{
    insert_sequence(*tree, 0, 500);
    validate();
}

TEST_F(TreeTests, CursorCannotMoveInEmptyTree)
{
    auto cursor = tree->find_minimum();
    cursor.increment();
    ASSERT_FALSE(cursor.is_valid());
    ASSERT_FALSE(cursor.is_minimum());
    ASSERT_FALSE(cursor.is_maximum());
    cursor.decrement();
    ASSERT_FALSE(cursor.is_valid());
    ASSERT_FALSE(cursor.is_minimum());
    ASSERT_FALSE(cursor.is_maximum());
}

TEST_F(TreeTests, CanFindExtrema)
{
    insert_sequence(*tree, 0, 200);

    auto minimum = tree->find_minimum();
    ASSERT_EQ(btos(minimum.key()), make_key(0));
    ASSERT_EQ(tools::find_exact(*tree, make_key(0)), minimum);
    ASSERT_EQ(tools::find(*tree, make_key(0)), minimum);

    auto maximum = tree->find_maximum();
    ASSERT_EQ(btos(maximum.key()), make_key(199));
    ASSERT_EQ(tools::find_exact(*tree, make_key(199)), maximum);
    ASSERT_EQ(tools::find(*tree, make_key(199)), maximum);
}

TEST_F(TreeTests, CanFind)
{
    insert_sequence(*tree, 0, 200, 2);

    auto cursor = tools::find(*tree, make_key(100));
    ASSERT_EQ(btos(cursor.key()), make_key(100));
    cursor = tools::find(*tree, make_key(101));
    ASSERT_EQ(btos(cursor.key()), make_key(102));

    cursor = tools::find_exact(*tree, make_key(101));
    ASSERT_FALSE(cursor.is_valid());
    cursor = tools::find_exact(*tree, make_key(102));
    ASSERT_EQ(btos(cursor.key()), make_key(102));
}

TEST_F(TreeTests, CopiedCursorsAreEqual)
{
    tools::insert(*tree, "a", "1");
    auto a = tree->find_minimum();
    auto b = a;

    ASSERT_EQ(a, b);
}

TEST_F(TreeTests, CopiedCursorsAreIndependent)
{
    tools::insert(*tree, "a", "1");
    tools::insert(*tree, "b", "2");
    auto a = tree->find_minimum();
    auto b = a;

    ASSERT_TRUE(b.increment());
    ASSERT_EQ(a.value(), "1");
    ASSERT_EQ(b.value(), "2");
}

TEST_F(TreeTests, OutOfRangeCursorsAreNotValid)
{
    tools::insert(*tree, "a", "1");

    auto cursor = tree->find_maximum();
    ASSERT_TRUE(cursor.increment());
    ASSERT_FALSE(cursor.is_valid());

    cursor = tree->find_minimum();
    ASSERT_TRUE(cursor.decrement());
    ASSERT_FALSE(cursor.is_valid());
}

TEST_F(TreeTests, CursorCannotMoveAfterInvalidation)
{
    tools::insert(*tree, "a", "1");
    auto cursor = tree->find_maximum();

    ASSERT_TRUE(cursor.is_valid());
    ASSERT_TRUE(cursor.increment());
    ASSERT_FALSE(cursor.is_valid());
    ASSERT_FALSE(cursor.increment());
    ASSERT_FALSE(cursor.decrement());

    cursor = tree->find_minimum();

    ASSERT_TRUE(cursor.is_valid());
    ASSERT_TRUE(cursor.decrement());
    ASSERT_FALSE(cursor.is_valid());
    ASSERT_FALSE(cursor.decrement());
    ASSERT_FALSE(cursor.increment());
}

TEST_F(TreeTests, CursorsOnSameRecordAreEqual)
{
    tools::insert(*tree, "a", "1");
    auto lhs = tree->find_minimum();
    auto rhs = tree->find_minimum();
    ASSERT_EQ(lhs, rhs);
}

TEST_F(TreeTests, CursorsOnDifferentRecordsAreNotEqual)
{
    tools::insert(*tree, "a", "1");
    tools::insert(*tree, "b", "2");
    auto lhs = tree->find_minimum();
    auto rhs = tree->find_maximum();
    ASSERT_NE(lhs, rhs);
}

TEST_F(TreeTests, CursorsFromEmptyTreeAreEqual)
{
    auto lhs = tree->find_minimum();
    auto rhs = tree->find_maximum();
    ASSERT_FALSE(lhs.is_valid());
    ASSERT_FALSE(rhs.is_valid());
    ASSERT_EQ(lhs, rhs);
}

TEST_F(TreeTests, SingleCellIsBothMinimumAndMaximum)
{
    tools::insert(*tree, "a", "1");

    ASSERT_EQ(tree->find_minimum(),
              tree->find_maximum());
}

TEST_F(TreeTests, InvalidCursorsAreEqual)
{
    tools::insert(*tree, "a", "1");
    auto lhs = tree->find_maximum();
    auto rhs = lhs;
    ASSERT_TRUE(lhs.increment());
    ASSERT_TRUE(rhs.increment());
    ASSERT_EQ(lhs, rhs);

    lhs = tree->find_minimum();
    rhs = lhs;
    ASSERT_TRUE(lhs.decrement());
    ASSERT_TRUE(rhs.decrement());
    ASSERT_EQ(lhs, rhs);

    // There should be no distinction between "one-past-the-end" and "one-before-the-beginning" positions.
    // Both are considered invalid and should compare equal.
    lhs = tree->find_minimum();
    rhs = tree->find_maximum();
    ASSERT_TRUE(lhs.decrement());
    ASSERT_TRUE(rhs.increment());
    ASSERT_EQ(lhs, rhs);
}

TEST_F(TreeTests, CanCopyInvalidCursor)
{
    tools::insert(*tree, "a", "1");
    auto lhs = tree->find_maximum();
    ASSERT_TRUE(lhs.increment());
    auto rhs = lhs;
    ASSERT_FALSE(rhs.is_valid());
    ASSERT_FALSE(rhs.is_maximum());

    lhs = tree->find_minimum();
    ASSERT_TRUE(lhs.decrement());
    rhs = lhs;
    ASSERT_FALSE(rhs.is_valid());
    ASSERT_FALSE(rhs.is_maximum());
}

TEST_F(TreeTests, ReverseSequentialInserts)
{
    for (Index i {}; i < 500; ++i)
        tools::insert(*tree, make_key(499 - i), "");
    validate();
    ASSERT_EQ(tree->cell_count(), 500);
}

TEST_F(TreeTests, AlternatingInsertsFromMiddle)
{
    for (Index i {}; i < 250; ++i) {
        tools::insert(*tree, make_key(249 - i), "");
        tools::insert(*tree, make_key(250 + i), "");
    }
    validate();
    ASSERT_EQ(tree->cell_count(), 500);
}

TEST_F(TreeTests, AlternatingInsertsFromEnds)
{
    for (Index i {}; i < 250; ++i) {
        tools::insert(*tree, make_key(i), "");
        tools::insert(*tree, make_key(499 - i), "");
    }
    validate();
    ASSERT_EQ(tree->cell_count(), 500);
}

TEST_F(TreeTests, ExternalRootFitsAtLeastThreeCells)
{
    // Generate maximally-sized cells.
    RecordGenerator::Parameters param;
    param.mean_key_size = max_local;
    param.mean_value_size = 10;
    param.spread = 0;

    for (const auto &[key, value]: RecordGenerator {param}.generate(random, 3)) {
        ASSERT_TRUE(tools::insert(*tree, key, value));
    }
}

TEST_F(TreeTests, RandomInserts)
{
    RecordGenerator::Parameters param;
    param.is_unique = true;

    RecordGenerator generator {param};
    const auto records = generator.generate(random, 500);
    for (const auto &[key, value]: records)
        tools::insert(*tree, key, value);

    validate();
    for (const auto &[key, value]: records) {
        const auto c = tools::find_exact(*tree, key);
        ASSERT_EQ(btos(c.key()), key);
        ASSERT_EQ(c.value(), value);
    }
    ASSERT_EQ(tree->cell_count(), records.size());
}

TEST_F(TreeTests, ModifiesValue)
{
    tools::insert(*tree, make_key(1), "a");
    tools::insert(*tree, make_key(1), "b");
    ASSERT_EQ(tools::find_exact(*tree, make_key(1)).value(), "b");
    ASSERT_EQ(tree->cell_count(), 1);
}

TEST_F(TreeTests, ModifySanityCheck)
{
    insert_sequence(*tree, 0, 1'000);

    for (Index i {0}; i < tree->cell_count(); ++i) {
        const auto key = make_key(i);
        auto cursor = tools::find_exact(*tree, key);
        if (auto value = cursor.value(); value.empty()) {
            tools::insert(*tree, key, std::string(123, 'x'));
        } else {
            tools::insert(*tree, key, value + value);
        }
    }
    validate();
    ASSERT_EQ(tree->cell_count(), 1'000);
}

TEST_F(TreeTests, CollapseForward)
{
    insert_sequence(*tree, 0, 200);
    for (Index i {}, n {tree->cell_count()}; i < n; ++i)
        tools::erase(*tree, make_key(i));
    ASSERT_EQ(tree->cell_count(), 0);
}

TEST_F(TreeTests, CollapseBackward)
{
    insert_sequence(*tree, 0, 200);
    for (Index i {}, n {tree->cell_count()}; i < n; ++i)
        tools::erase(*tree, make_key(n - i - 1));
    ASSERT_EQ(tree->cell_count(), 0);
}

TEST_F(TreeTests, CollapseAlternatingFromMiddle)
{
    insert_sequence(*tree, 0, 200);
    for (Index i {}, n {tree->cell_count()}; i < n / 2; ++i) {
        tools::erase(*tree, make_key(n/2 - i - 1));
        tools::erase(*tree, make_key(n/2 + i));
    }
    ASSERT_EQ(tree->cell_count(), 0);
}

TEST_F(TreeTests, CollapseAlternatingFromEnds)
{
    insert_sequence(*tree, 0, 200);
    for (Index i {}, n {tree->cell_count()}; i < n / 2; ++i) {
        tools::erase(*tree, make_key(i));
        tools::erase(*tree, make_key(n - i - 1));
    }
    ASSERT_EQ(tree->cell_count(), 0);
}

TEST_F(TreeTests, RemoveFromLeft)
{
    insert_sequence(*tree, 0, 1'000);

    // Remove the minimum key without relying on find_minimum().
    while (tree->cell_count())
        ASSERT_TRUE(tree->erase(tools::find(*tree, make_key(0))));
}

TEST_F(TreeTests, RemoveFromRight)
{
    insert_sequence(*tree, 0, 1'000);
    while (tree->cell_count())
        ASSERT_TRUE(tree->erase(tree->find_maximum()));
}

TEST_F(TreeTests, RemoveFromMiddle)
{
    static constexpr Size n {1'000};
    insert_sequence(*tree, 0, n);

    while (tree->cell_count()) {
        const auto key = make_key(tree->cell_count() / 2);

        if (auto c = tools::find(*tree, key); c.is_valid()) {
            ASSERT_TRUE(tree->erase(c));
        }
    }
}

TEST_F(TreeTests, RemoveFromRandomPosition)
{
    static constexpr Size n {1'000};
    insert_sequence(*tree, 0, n);

    while (tree->cell_count()) {
        const auto key = make_key(random.next_int(n));

        if (auto c = tools::find(*tree, key); c.is_valid()) {
            ASSERT_TRUE(tree->erase(c));
        }
    }
}

TEST_F(TreeTests, RemoveEverythingRepeatedly)
{
    std::unordered_map<std::string, std::string> records;
    static constexpr Size num_iterations = 3;
    static constexpr Size cutoff = 1'000;

    for (Index i {}; i < num_iterations; ++i) {
        while (tree->cell_count() < cutoff) {
            const auto key = random_string(random, 7, 10);
            const auto value = random_string(random, 20);
            tools::insert(*tree, key, value);
            records[key] = value;
        }
        Index counter {};
        for (const auto &[k, v]: records) {
            auto cursor = tools::find_exact(*tree, k);
            ASSERT_TRUE(cursor.is_valid());
            ASSERT_EQ(btos(cursor.key()), k);
            ASSERT_EQ(cursor.value(), v);
            CCO_EXPECT_TRUE(tools::erase(*tree, k));
            if (counter++ % 100 == 0)
                validate();
        }
        ASSERT_EQ(tree->cell_count(), 0);
        records.clear();
    }
}

TEST_F(TreeTests, ForwardPartialIteration)
{
    insert_sequence(*tree, 0, 200);
    auto i = tree->cell_count() / 5;

    for (auto cursor = tools::find_exact(*tree, make_key(i)); cursor.is_valid(); cursor++)
        ASSERT_EQ(btos(cursor.key()), make_key(i++));
}

TEST_F(TreeTests, ForwardBoundedIteration)
{
    insert_sequence(*tree, 0, 200);
    auto i = tree->cell_count() / 5;
    auto lower = tools::find_exact(*tree, make_key(i));
    auto upper = tools::find_exact(*tree, make_key(tree->cell_count() - i));

    for (; lower.is_valid() && lower != upper; lower++)
        ASSERT_EQ(btos(lower.key()), make_key(i++));
}

TEST_F(TreeTests, ReversePartialIteration)
{
    static constexpr Size count {1'000};
    insert_sequence(*tree, 0, count);
    auto i = count / 5;

    for (auto c = tools::find_exact(*tree, make_key(count - i - 1)); c.is_valid(); c--)
        ASSERT_EQ(btos(c.key()), make_key(count - i++ - 1));
}

TEST_F(TreeTests, ReverseBoundedIteration)
{
    auto i = tree->cell_count() / 5;

    auto lower = tools::find_exact(*tree, make_key(i));
    auto upper = tools::find_exact(*tree, make_key(tree->cell_count() - i - 1));

    for (; upper.is_valid() && upper != lower; upper--)
        ASSERT_EQ(btos(upper.key()), make_key(tree->cell_count() - i++ - 1));
}

TEST_F(TreeTests, SanityCheck)
{
    static constexpr Size NUM_ITERATIONS {5};
    static constexpr Size NUM_RECORDS {5'000};
    static constexpr Size MIN_SIZE {1'000};
    RecordGenerator::Parameters param;
    param.mean_key_size = 20;
    param.mean_value_size = 10;
    param.spread = 9;
    RecordGenerator generator {param};
    Random random {0};

    const auto remove_one = [&random, this](const std::string &key) {
        if (auto c = tools::find(*tree, key); c.is_valid()) {
            ASSERT_TRUE(tree->erase(c));
        } else if (random.next_int(1) == 0) {
            ASSERT_TRUE(tree->erase(tree->find_minimum()));
        } else {
            ASSERT_TRUE(tree->erase(tree->find_maximum()));
        }
    };

    for (Index iteration {}; iteration < NUM_ITERATIONS; ++iteration) {
        for (const auto &[key, value]: generator.generate(random, NUM_RECORDS)) {
            if (tree->cell_count() < MIN_SIZE || random.next_int(5) != 0) {
                tools::insert(*tree, key, value);
            } else {
                remove_one(key);
            }
        }
        while (tree->cell_count())
            remove_one(random_string(random, 1, 30));
    }
}

} // <anonymous>
