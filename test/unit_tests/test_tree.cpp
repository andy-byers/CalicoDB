
#include <array>
#include <filesystem>
#include <thread>
#include <unordered_map>

#include <gtest/gtest.h>
#include <gmock/gmock.h>

#include "pool/buffer_pool.h"
#include "pool/frame.h"
#include "pool/interface.h"
#include "page/cell.h"
#include "calico/options.h"
#include "utils/layout.h"
#include "utils/logging.h"
#include "db/cursor_impl.h"
#include "tree/tree.h"

#include "fakes.h"
#include "random.h"
#include "tools.h"
#include "unit_tests.h"

namespace {

using namespace calico;

template<class T> auto tree_insert(T &tree, const std::string &key, const std::string &value) -> void
{
    tree.insert(stob(key), stob(value));
    tree.set_payload(key, value);
}

template<class T> auto tree_lookup(T &tree, const std::string &key, std::string &result) -> bool
{
    if (const auto [node, index, found_eq] = tree.find_external(stob(key), false); found_eq) {
        result = tree.collect_value(node, index);
        return true;
    }
    return false;
}

template<class T>  auto tree_remove(T &tree, const std::string &key) -> bool
{
    return tree.remove(stob(key));
}

class MockTree: public Tree {
public:

};

class TestTree: public Tree {
public:
    friend class TreeTests;

    ~TestTree() override = default;

    TestTree(const Tree::Parameters &param)
        : Tree{param}
        , m_page_size{param.buffer_pool->page_size()}
        , m_max_local{get_max_local(m_page_size)} {}

    auto page_size() const -> Size
    {
        return m_page_size;
    }

    auto set_payload(const std::string &key, const std::string &value) -> void
    {
        m_payloads[key] = value;
    }

    auto delete_payload(const std::string &key) -> bool
    {
        if (auto itr = m_payloads.find(key); itr != m_payloads.end()) {
            m_payloads.erase(itr);
            return true;
        }
        return false;
    }

    auto node_contains(PID id, const std::string &key) -> bool
    {
        auto [node, index, found_eq] = find_ge(stob(key), false);
        return found_eq && node.id() == id;
    }

    auto contains_separator(PID id, const std::string &key) -> bool
    {
        auto [node, index, found_eq] = find_ge(stob(key), false);
        return found_eq && node.id() == id;
    }

    auto contains_record(PID id, const std::string &key) -> bool
    {
        auto [node, index, found_eq] = find_external(stob(key), false);
        if (found_eq && node.id() == id) {
            if (const auto itr = m_payloads.find(key); itr != m_payloads.end()) {
                EXPECT_EQ(itr->second, collect_value(node, index));
                return true;
            }
            ADD_FAILURE() << "unable to find \"" << key << "\" in bookkeeping map";
            return false;
        }
        EXPECT_EQ(found_eq, node.id() == id) << "Found the key in the wrong node";
        return false;
    }

    auto tree_contains(const std::string &key) -> bool
    {
        std::string result;
        if (tree_lookup(*this, key, result)) {
            const auto itr = m_payloads.find(key);
            EXPECT_NE(itr, m_payloads.end()) << "Key " << key << " hasn't been added to the tree";
            const auto same = result == itr->second;
            EXPECT_TRUE(same) << "Payload mismatch at key " << key;
            return same;
        }
        return false;
    }

    std::unordered_map<std::string, std::string> m_payloads;
    Random m_random {0};
    Size m_page_size {};
    Size m_max_local {};
};

class TreeBuilder {
public:
    explicit TreeBuilder(TestTree &tree)
        : m_tree {tree} {}

    auto page_size() const -> Size
    {
        return m_tree.page_size();
    }

    auto make_root_internal() -> void
    {
        auto root = m_tree.acquire_node(PID::root(), true);
        root.page().set_type(PageType::INTERNAL_NODE);
    }

    auto allocate_node(PageType node_type) -> PID
    {
        auto node = m_tree.allocate_node(node_type);
        const auto id = node.id();
        node.page().set_type(node_type);
        return id;
    }

    auto tree_insert(const std::string &key) -> void
    {
        tree_insert(key, m_tree.m_random.next_string(m_tree.m_max_local - key.size()));
    }

    auto tree_insert(const std::string &key, Index value_size) -> void
    {
        tree_insert(key, m_tree.m_random.next_string(value_size));
    }

    auto tree_insert(const std::string &key, const std::string &value) -> void
    {
        ::tree_insert(m_tree, key, value);
    }

    auto node_insert(PID id, const std::string &key) -> void
    {
        const auto value = m_tree.m_random.next_string(m_tree.m_max_local - key.size());
        node_insert(id, key, value);
    }

    auto node_insert(PID id, const std::string &key, Index value_size) -> void
    {
        const auto value = m_tree.m_random.next_string(value_size);
        node_insert(id, key, value);
    }

    auto node_insert(PID id, const std::string &key, const std::string &value) -> void
    {
        auto node = m_tree.acquire_node(id, true);
        ASSERT_TRUE(node.is_external());
        auto cell = m_tree.make_cell(stob(key), stob(value), true);

        if (!node.is_external())
            cell.set_left_child_id(PID{std::numeric_limits<uint32_t>::max()});

        node.insert(std::move(cell));
        ASSERT_FALSE(node.is_overflowing());
        m_tree.m_payloads[key] = value;
    }

    auto connect_parent_child(PID parent_id, PID child_id, Index index_of_child) -> void
    {
        auto parent = m_tree.acquire_node(parent_id, true);
        auto child = m_tree.acquire_node(child_id, true);
        parent.set_child_id(index_of_child, child_id);
        child.set_parent_id(parent_id);
    }

    auto connect_siblings(PID left_sibling_id, PID right_sibling_id) -> void
    {
        auto left_sibling = m_tree.acquire_node(left_sibling_id, true);
        left_sibling.set_right_sibling_id(right_sibling_id);
    }

    auto tree() -> TestTree&
    {
        return m_tree;
    }

private:
    TestTree &m_tree;
};

class TreeTests: public testing::Test {
public:

    TreeTests()
    {
        auto sink = logging::create_sink("", 0);
        m_max_local = get_max_local(m_page_size);
        std::filesystem::remove(m_path);
        auto file = std::make_unique<FaultyReadWriteMemory>();
        m_pool = std::make_unique<BufferPool>(BufferPool::Parameters{
            std::move(file),
            nullptr,
            nullptr,
            sink,
            LSN::null(),
            32,
            0,
            m_page_size,
            false,
        });
        m_tree = std::make_unique<TestTree>(Tree::Parameters{
            m_pool.get(),
            sink,
            PID::null(),
            0,
            0,
            0,
        });

        (void)m_tree->allocate_node(PageType::EXTERNAL_NODE);
    }

    ~TreeTests() override
    {
        m_pool->try_flush();
    }

    auto tree() -> TestTree&
    {
        return dynamic_cast<TestTree&>(*m_tree);
    }

    auto tree() const -> const TestTree&
    {
        return dynamic_cast<const TestTree&>(*m_tree);
    }

    auto validate() -> void
    {
        TreeValidator {tree()}.validate();
    }

    std::string m_path {"TestTree"};
    Size m_page_size {0x100};
    Random m_random {0};
    std::unique_ptr<IBufferPool> m_pool;
    std::unique_ptr<ITree> m_tree;
    Size m_max_local;
};

TEST_F(TreeTests, FreshTreeHasNoCells)
{
    ASSERT_EQ(tree().cell_count(), 0);
}

TEST_F(TreeTests, FreshTreeHasOneNode)
{
    ASSERT_EQ(m_pool->page_count(), 1);
    ASSERT_EQ(m_tree->node_count(), 1);
}

TEST_F(TreeTests, InsertRecord)
{
    tree_insert(tree(), "key", "value");
    ASSERT_TRUE(tree().node_contains(PID::root(), "key"));
}

TEST_F(TreeTests, InsertNonOverflowingRecord)
{
    tree_insert(tree(), "a", m_random.next_string(get_max_local(m_page_size) - 1));
    ASSERT_EQ(m_pool->page_count(), 1);
}

TEST_F(TreeTests, InsertOverflowingRecord)
{
    m_tree->insert(stob("a"), stob(m_random.next_string(get_max_local(m_page_size))));
    ASSERT_EQ(m_pool->page_count(), 2);
}

TEST_F(TreeTests, OnlyAcceptsValidKeySizes)
{
    ASSERT_THROW(tree_insert(tree(), "", "value"), std::invalid_argument);
    ASSERT_THROW(tree_insert(tree(), std::string(m_max_local + 1, 'x'), "value"), std::invalid_argument);
}

TEST_F(TreeTests, RemoveRecord)
{
    std::string unused;
    tree_insert(tree(), "key", "value");
    ASSERT_TRUE(tree_remove(tree(), "key"));
    ASSERT_FALSE(tree_lookup(tree(), "key", unused));
}

TEST_F(TreeTests, InsertBefore)
{
    tree_insert(tree(), "key_2", "value_2");
    tree_insert(tree(), "key_1", "value_1");
    ASSERT_TRUE(tree().node_contains(PID::root(), "key_1"));
    ASSERT_TRUE(tree().node_contains(PID::root(), "key_2"));
}

TEST_F(TreeTests, InsertAfter)
{
    tree_insert(tree(), "key_1", "value_1");
    tree_insert(tree(), "key_2", "value_2");
    ASSERT_TRUE(tree().node_contains(PID::root(), "key_1"));
    ASSERT_TRUE(tree().node_contains(PID::root(), "key_2"));
}

TEST_F(TreeTests, InsertBetween)
{
    tree_insert(tree(), "key_1", "value_1");
    tree_insert(tree(), "key_3", "value_3");
    tree_insert(tree(), "key_2", "value_2");
    ASSERT_TRUE(tree().node_contains(PID::root(), "key_1"));
    ASSERT_TRUE(tree().node_contains(PID::root(), "key_2"));
    ASSERT_TRUE(tree().node_contains(PID::root(), "key_3"));
}

TEST_F(TreeTests, OverflowChains)
{
    // These three inserts should need overflow chains.
    tree_insert(tree(), "key_a", random_string(m_random, m_max_local, m_max_local * 10));
    tree_insert(tree(), "key_b", random_string(m_random, m_max_local, m_max_local * 20));
    tree_insert(tree(), "key_c", random_string(m_random, m_max_local, m_max_local * 30));

    // We should be able to get all our data back.
    ASSERT_TRUE(tree().tree_contains("key_a"));
    ASSERT_TRUE(tree().tree_contains("key_b"));
    ASSERT_TRUE(tree().tree_contains("key_c"));
}

TEST_F(TreeTests, CanLookupMinimum)
{
    TreeBuilder builder {tree()};
    for (Index i {}; i < 500; ++i)
        builder.tree_insert(make_key(i));
    auto [node, index] = m_tree->find_local_min(m_tree->find_root(false));
    ASSERT_EQ(btos(node.read_key(index)), make_key(0));
}

TEST_F(TreeTests, CanLookupMaximum)
{
    TreeBuilder builder {tree()};
    for (Index i {}; i < 500; ++i)
        builder.tree_insert(make_key(i));
    auto [node, index] = m_tree->find_local_max(m_tree->find_root(false));
    ASSERT_EQ(btos(node.read_key(index)), make_key(499));
}

TEST_F(TreeTests, SequentialInserts)
{
    TreeBuilder builder {tree()};
    for (Index i{}; i < 500; ++i)
        builder.tree_insert(make_key(i));
    validate();
}

TEST_F(TreeTests, ReverseSequentialInserts)
{
    TreeBuilder builder {tree()};
    for (Index i {}; i < 500; ++i)
        builder.tree_insert(make_key(499 - i));
    validate();
}

TEST_F(TreeTests, AlternatingInsertsFromMiddle)
{
    TreeBuilder builder {tree()};
    for (Index i {}; i < 250; ++i) {
        builder.tree_insert(make_key(250 - i));
        builder.tree_insert(make_key(250 + i));
    }
    validate();
}

TEST_F(TreeTests, AlternatingInsertsFromEnds)
{
    TreeBuilder builder {tree()};
    for (Index i {}; i < 250; ++i) {
        builder.tree_insert(make_key(i));
        builder.tree_insert(make_key(500 - i));
    }
    validate();
}

static constexpr Size TEST_KEY_SIZE {30};

auto random_tree(Random &random, TreeBuilder &builder, Size n) -> void
{
    std::vector<Index> keys(n);
    std::iota(keys.begin(), keys.end(), 1);
    random.shuffle(keys);
    const auto max_size = 2 * get_max_local(builder.page_size());
    int i {};
    for (auto key: keys) {
        builder.tree_insert(make_key<TEST_KEY_SIZE>(key), random_string(random, 10L, max_size));
        i++;
    }
}

TEST_F(TreeTests, LookupPastEnd)
{
    TreeBuilder builder {tree()};
    random_tree(m_random, builder, 100);
    std::string result;
    const auto key = make_key(101);
    ASSERT_FALSE(tree_lookup(tree(), key, result));
}

TEST_F(TreeTests, LookupBeforeBeginning)
{
    TreeBuilder builder {tree()};
    random_tree(m_random, builder, 100);
    std::string result;
    const auto key = make_key(0);
    ASSERT_FALSE(tree_lookup(tree(), key, result));
}

TEST_F(TreeTests, InsertSanityCheck)
{
    TreeBuilder builder {tree()};
    random_tree(m_random, builder, 5'000);
    validate();
}

// TODO:
// TODO:
// TODO:
// TODO: parameterized tests for inserting/modifying the tree in various orders.
// TODO: then we'll need the same for the removes.
// TODO:
// TODO:
// TODO:
// TODO:

TEST_F(TreeTests, ModifySanityCheck)
{
    TreeBuilder builder {tree()};
    random_tree(m_random, builder, 1'000);
    for (Index i {1}; i <= 1'000; ++i) {
        const auto key = make_key<TEST_KEY_SIZE>(i);
        std::string value;
        {
            auto [node, index, found_eq] = tree().find_external(stob(key), true);
            ASSERT_TRUE(found_eq) << "Unable to find key " << key;
            value = tree().collect_value(node, index);
            value += value + value;
        }
        ASSERT_FALSE(tree().insert(stob(key), stob(value)));
    }
    validate();
}

TEST_F(TreeTests, ModifiesExistingValue)
{
    TreeBuilder builder{tree()};
    builder.tree_insert(make_key(1), "a");
    builder.tree_insert(make_key(1), "b");
    ASSERT_TRUE(tree().node_contains(PID{1}, make_key(1)));
}

auto setup_collapse_test(TestTree &tree, Size n)
{
    TreeBuilder builder{tree};
    for (Index i {}; i < n; ++i)
        builder.tree_insert(make_key<30>(i), std::to_string(i * i));
}

TEST_F(TreeTests, ExRotL)
{
    setup_collapse_test(tree(), 8);
    tree_remove(tree(), make_key<30>(0));
    validate();
}

TEST_F(TreeTests, ExRotR)
{
    setup_collapse_test(tree(), 8);
    tree_remove(tree(), make_key<30>(6));
    tree_remove(tree(), make_key<30>(7));
    validate();
}

TEST_F(TreeTests, ExMrgL)
{
    setup_collapse_test(tree(), 8);

    TreePrinter {tree()}.print(4);
    puts("");

    tree_remove(tree(), make_key<30>(0));
    tree_remove(tree(), make_key<30>(1));
    validate();
    TreePrinter {tree()}.print(4);
}

TEST_F(TreeTests, ExMrgR)
{
    setup_collapse_test(tree(), 8);

    TreePrinter {tree()}.print(4);
    puts("");

    tree_remove(tree(), make_key<30>(5));
    tree_remove(tree(), make_key<30>(6));
    tree_remove(tree(), make_key<30>(7));
    validate();
    TreePrinter {tree()}.print(4);
}

TEST_F(TreeTests, SmallCollapseForward)
{
    setup_collapse_test(tree(), 8);
    for (Index i {}; i < 8; ++i)
        tree_remove(tree(), make_key<30>(i));
    validate();
}

TEST_F(TreeTests, SmallCollapseBackward)
{
    setup_collapse_test(tree(), 8);
    for (Index i {}; i < 8; ++i)
        tree_remove(tree(), make_key<30>(7 - i));
    validate();
}

TEST_F(TreeTests, A)
{
    static constexpr Size n = 25000;
    TreePrinter {tree()}.print();
    setup_collapse_test(tree(), n);
    Index k = n + 10;
    for (Index i {}; i < n; ++i) {
        CALICO_EXPECT_TRUE(tree_remove(tree(), make_key<30>(i)));
        if (m_random.next_int(5) == 0) {
            tree_insert(tree(), make_key<10>(k++), std::string(m_random.next_int(1ULL, 5ULL), 'a'));
            tree_insert(tree(), make_key<30>(k++), std::string(m_random.next_int(1ULL, 5ULL), 'a'));
            tree_insert(tree(), make_key<10>(k++), std::string(m_random.next_int(1ULL, 5ULL), 'a'));
            tree_insert(tree(), make_key<30>(k++), std::string(m_random.next_int(1ULL, 5ULL), 'a'));
            if (m_random.next_int(5) == 0) {
                validate();
            }
        }
        //        puts("");
//        TreePrinter {tree()}.print();
    }
}

TEST_F(TreeTests, SanityCheck)
{
    std::unordered_map<std::string, std::string> payloads;
//    constexpr Size max_size = 100;
    constexpr Size n = 100'000;

    for (Index i {}; i < n; ++i) {
        const auto r = m_random.next_int(5);
        std::string key {};
        if (r == 0) {
            // Short key. Could already be in the tree: if so, we'll need to modify rather than insert.
            key = make_key<2>(m_random.next_int(16ULL));
        } else if (r == 1) {
            // Long key.
            key = make_key<30>(m_random.next_int(100'000'000ULL));
        } else {
            key = make_key<6>(m_random.next_int(100'000ULL));
        }
        // Value may need one or more overflow pages.
        const auto value = random_string(m_random, 5, m_max_local * 3);

        // Insert a key-value pair.
        tree_insert(tree(), key, value);
        payloads[key] = value;

        // Remove a key-value pair.
        const auto too_many_records = false; // tree().cell_count() > max_size;
        if (too_many_records || (m_random.next_int(5) < 3 && !payloads.empty())) {
            auto itr = payloads.begin();

            ASSERT_TRUE(tree_remove(tree(), itr->first))
                << "Unable to remove '" << itr->first << "': "
                << tree().cell_count() << " values remaining ";
            payloads.erase(itr);
        }
    }
    TreeValidator {tree()}.validate();

    for (const auto &[key, value]: payloads) {
        std::string result;
        ASSERT_TRUE(tree_lookup(tree(), key, result));
        ASSERT_EQ(result, value);
        ASSERT_TRUE(tree_remove(tree(), key))
            << "Unable to remove " << key << " from the tree";
    }
    auto root = tree().acquire_node(PID::root(), false);
    ASSERT_EQ(root.cell_count(), 0);
    ASSERT_TRUE(root.is_external());
}

TEST_F(TreeTests, RemoveEverythingRepeatedly)
{
    std::unordered_map<std::string, std::string> records;
    static constexpr Size num_iterations = 3;
    static constexpr Size cutoff = 1'500;

    for (Index i {}; i < num_iterations; ++i) {
        while (m_tree->cell_count() < cutoff) {
            const auto key = random_string(m_random, 7, 10);
            const auto value = random_string(m_random, 20);
            tree_insert(tree(), key, value);
            records[key] = value;
        }
        for (const auto &[k, v]: records) {
            std::string result;
            ASSERT_TRUE(tree_lookup(tree(), k, result));
            ASSERT_EQ(result, v);
            CALICO_EXPECT_TRUE(tree_remove(tree(), k));
        }
        //        ASSERT_EQ(m_tree->cell_count(), 0);
        records.clear();
    }
}

} // <anonymous>