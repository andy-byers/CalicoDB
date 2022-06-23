
#include <array>
#include <filesystem>
#include <thread>
#include <unordered_map>

#include <gtest/gtest.h>

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
    if (const auto [node, index, found_eq] = tree.find_ge(stob(key), false); found_eq) {
        result = tree.collect_value(node, index);
        return true;
    }
    return false;
}

template<class T>  auto tree_remove(T &tree, const std::string &key) -> bool
{
    return tree.remove(stob(key));
}

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
        auto cell = m_tree.make_cell(stob(key), stob(value));

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

auto external_root_overflow_test(TestTree &tree, Index excluded) -> void
{
    // TODO: This test is pretty fragile. I just had to fuss with the value size below after changing the node and file header sizes.
    ASSERT_LT(excluded, 5L);
    TreeBuilder builder {tree};
    const std::vector<Index> keys {10, 20, 30, 40, 50};
    const auto id = PID::root();

    for (Index i{}; i < keys.size(); ++i) {
        if (i != excluded)
            builder.node_insert(id, make_key(keys[i]), get_max_local(tree.page_size()) / 3 * 2);
    }
    // Cause the overflow.
    const auto key = make_key(keys[excluded]);
    std::string value {"value"};
    value.resize(get_max_local(tree.page_size()) - key.size());
    tree_insert(tree, key, value);

    // We should always end up with this structure:
    //             1:[c]
    //     2:[a, b]     3:[d, e]
    ASSERT_TRUE(tree.node_contains(PID{1}, make_key(keys[2])));
    ASSERT_TRUE(tree.node_contains(PID{2}, make_key(keys[0])));
    ASSERT_TRUE(tree.node_contains(PID{2}, make_key(keys[1])));
    ASSERT_TRUE(tree.node_contains(PID{3}, make_key(keys[3])));
    ASSERT_TRUE(tree.node_contains(PID{3}, make_key(keys[4])));
    TreeValidator {tree}.validate();
}

auto internal_root_overflow_test(TestTree &tree, Index child_index) -> void
{
    ASSERT_LT(child_index, 4L);
    TreeBuilder builder {tree};
    const auto pt = PID::root();
    const auto LL = builder.allocate_node(PageType::EXTERNAL_NODE);
    const auto cL = builder.allocate_node(PageType::EXTERNAL_NODE);
    const auto cr = builder.allocate_node(PageType::EXTERNAL_NODE);
    const auto rr = builder.allocate_node(PageType::EXTERNAL_NODE);

    builder.make_root_internal();
    builder.node_insert(pt, make_key(6));
    builder.node_insert(pt, make_key(12));
    builder.node_insert(pt, make_key(18));

    builder.node_insert(LL, make_key(1));
    builder.node_insert(LL, make_key(2));
    builder.node_insert(LL, make_key(3));
    builder.node_insert(LL, make_key(4));

    builder.node_insert(cL, make_key(7));
    builder.node_insert(cL, make_key(8));
    builder.node_insert(cL, make_key(9));
    builder.node_insert(cL, make_key(10));

    builder.node_insert(cr, make_key(13));
    builder.node_insert(cr, make_key(14));
    builder.node_insert(cr, make_key(15));
    builder.node_insert(cr, make_key(16));

    builder.node_insert(rr, make_key(19));
    builder.node_insert(rr, make_key(20));
    builder.node_insert(rr, make_key(21));
    builder.node_insert(rr, make_key(22));

    builder.connect_parent_child(pt, LL, 0);
    builder.connect_parent_child(pt, cL, 1);
    builder.connect_parent_child(pt, cr, 2);
    builder.connect_parent_child(pt, rr, 3);
    builder.connect_siblings(LL, cL);
    builder.connect_siblings(cL, cr);
    builder.connect_siblings(cr, rr);

    // Before the overflow:
    //                   1:[6,               12,                  18]
    //     2:[1, 2, 3, 4]     3:[7, 8, 9, 10]   4:[13, 14, 15, 16]   5:[19, 20, 21, 22]

    // Cause the overflow.
    constexpr Index keys[] {5, 11, 17, 23};
    const auto key = make_key(keys[child_index]);
    builder.tree_insert(key);
}

TEST_F(TreeTests, ExternalRootOverflowA)
{
    external_root_overflow_test(tree(), 0);
}

TEST_F(TreeTests, ExternalRootOverflowB)
{
    external_root_overflow_test(tree(), 1);
}

TEST_F(TreeTests, ExternalRootOverflowC)
{
    external_root_overflow_test(tree(), 2);
}

TEST_F(TreeTests, ExternalRootOverflowD)
{
    external_root_overflow_test(tree(), 3);
}

TEST_F(TreeTests, ExternalRootOverflowE)
{
    external_root_overflow_test(tree(), 4);
}

TEST_F(TreeTests, InternalRootOverflowA)
{
    // After this overflow:
    //                            1:[            12                  ]
    //             7:[3,        6]                                    8:[18]
    //     2:[1, 2]     6:[4, 5]  3:[7, 8, 9, 10]   4:[13, 14, 15, 16]      5:[19, 20, 21, 22]
    internal_root_overflow_test(tree(), 0);
    validate();
}

TEST_F(TreeTests, InternalRootOverflowB)
{
    // After this overflow:
    //                                  1:[            12            ]
    //                   7:[6,        9]                              8:[18]
    //     2:[1, 2, 3, 4]     3:[7, 8]  6:[10, 11]  4:[13, 14, 15, 16]      5:[19, 20, 21, 22]
    internal_root_overflow_test(tree(), 1);
    validate();
}

TEST_F(TreeTests, InternalRootOverflowC)
{
    // After this overflow:
    //                                          1:[        15        ]
    //                   7:[6,               12]                      8:[18]
    //     2:[1, 2, 3, 4]     3:[7, 8, 9, 10]   4:[13, 14]  6:[16, 17]      5:[19, 20, 21, 22]
    internal_root_overflow_test(tree(), 2);
    validate();
}

TEST_F(TreeTests, InternalRootOverflowD)
{
    // After this overflow:
    //                                                            1:[18]
    //                   7:[6,               12]                                  8:[21]
    //     2:[1, 2, 3, 4]     3:[7, 8, 9, 10]   4:[13, 14, 15, 16]      5:[19, 20]      6:[22, 23]
    internal_root_overflow_test(tree(), 3);
    validate();
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

auto random_tree(Random &random, TreeBuilder &builder, Size n) -> void
{
    std::vector<Index> keys(n);
    std::iota(keys.begin(), keys.end(), 1);
    random.shuffle(keys);
    const auto max_size = 2 * get_max_local(builder.page_size());
    int i {};
    for (auto key: keys) {
        builder.tree_insert(make_key(key), random_string(random, 10L, max_size));
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
    random_tree(m_random, builder, 1'000);
    validate();
}

auto setup_external_merge_test(TreeBuilder &builder) -> void
{
    //      1:[2,     4]
    // 2:[1]     3:[3]  4:[5]
    const auto pt = PID::root();
    const auto Lc = builder.allocate_node(PageType::EXTERNAL_NODE);
    const auto cc = builder.allocate_node(PageType::EXTERNAL_NODE);
    const auto rc = builder.allocate_node(PageType::EXTERNAL_NODE);

    builder.make_root_internal();
    builder.node_insert(Lc, make_key<1>(1));
    builder.node_insert(pt, make_key<1>(2));
    builder.node_insert(cc, make_key<1>(3));
    builder.node_insert(pt, make_key<1>(4));
    builder.node_insert(rc, make_key<1>(5));

    builder.connect_parent_child(pt, Lc, 0);
    builder.connect_parent_child(pt, cc, 1);
    builder.connect_parent_child(pt, rc, 2);

    builder.connect_siblings(Lc, cc);
    builder.connect_siblings(cc, rc);
}

TEST_F(TreeTests, LeftMergeExternal)
{
    //     1:[2,     4]       -->          1:[4]
    // 2:[]     3:[3]  4:[5]       2:[2, 3]     4:[5]
    TreeBuilder builder {tree()};
    setup_external_merge_test(builder);

    ASSERT_TRUE(tree_remove(tree(), make_key<1>(1)));
    ASSERT_TRUE(tree().node_contains(PID {1}, make_key<1>(4)));
    ASSERT_TRUE(tree().node_contains(PID {2}, make_key<1>(2)));
    ASSERT_TRUE(tree().node_contains(PID {2}, make_key<1>(3)));
    ASSERT_TRUE(tree().node_contains(PID {4}, make_key<1>(5)));
    validate();
}

TEST_F(TreeTests, LeftMergeExternal2)
{
    //     1:[2,     4]       -->          1:[4]
    // 2:[]     3:[3]  4:[5]       2:[2, 3]     4:[5]
    TreeBuilder builder {tree()};
    setup_external_merge_test(builder);
    ASSERT_TRUE(tree_remove(tree(), make_key<1>(1)));
    ASSERT_TRUE(tree_remove(tree(), make_key<1>(2)));
    ASSERT_TRUE(tree().node_contains(PID {1}, make_key<1>(3)));
    ASSERT_TRUE(tree().node_contains(PID {1}, make_key<1>(4)));
    ASSERT_TRUE(tree().node_contains(PID {1}, make_key<1>(5)));
    validate();
}

TEST_F(TreeTests, RightMergeExternal)
{
    //      1:[2,     4]      -->       1:[2]
    // 2:[1]     3:[3]  4:[]       2:[1]     3:[3, 4]
    TreeBuilder builder {tree()};
    setup_external_merge_test(builder);
    ASSERT_TRUE(tree_remove(tree(), make_key<1>(5)));
    ASSERT_TRUE(tree().node_contains(PID {1}, make_key<1>(2)));
    ASSERT_TRUE(tree().node_contains(PID {2}, make_key<1>(1)));
    ASSERT_TRUE(tree().node_contains(PID {3}, make_key<1>(3)));
    ASSERT_TRUE(tree().node_contains(PID {3}, make_key<1>(4)));
    validate();
}

TEST_F(TreeTests, RightMergeExternal2)
{
    //      1:[2,     4]      -->       1:[2]
    // 2:[1]     3:[3]  4:[]       2:[1]     3:[3, 4]
    TreeBuilder builder {tree()};
    setup_external_merge_test(builder);
    ASSERT_TRUE(tree_remove(tree(), make_key<1>(5)));
    ASSERT_TRUE(tree_remove(tree(), make_key<1>(4)));
    ASSERT_TRUE(tree().node_contains(PID{1}, make_key<1>(1)));
    ASSERT_TRUE(tree().node_contains(PID{1}, make_key<1>(2)));
    ASSERT_TRUE(tree().node_contains(PID{1}, make_key<1>(3)));
    validate();
}

template<Size KeyLength = 1> auto setup_fixes_root_after_merge_test(TreeBuilder &builder) -> void
{
    //      1:[5]
    // 2:[1]     3:[9]
    const auto pt = PID::root();
    const auto Lc = builder.allocate_node(PageType::EXTERNAL_NODE);
    const auto rc = builder.allocate_node(PageType::EXTERNAL_NODE);

    builder.make_root_internal();
    builder.node_insert(Lc, make_key<KeyLength>(1));
    builder.node_insert(pt, make_key<KeyLength>(5));
    builder.node_insert(rc, make_key<KeyLength>(9));

    builder.connect_parent_child(pt, Lc, 0);
    builder.connect_parent_child(pt, rc, 1);

    builder.connect_siblings(Lc, rc);
}

TEST_F(TreeTests, FixesRootAfterLeftMerge)
{
    //     1:[5]       -->   1:[5, 9]
    // 2:[]     3:[9]
    TreeBuilder builder{tree()};
    setup_fixes_root_after_merge_test(builder);
    ASSERT_TRUE(tree_remove(tree(), make_key<1>(1)));

    ASSERT_FALSE(tree().node_contains(PID::root(), make_key<1>(1)));
    ASSERT_TRUE(tree().node_contains(PID::root(), make_key<1>(5)));
    ASSERT_TRUE(tree().node_contains(PID::root(), make_key<1>(9)));
}

TEST_F(TreeTests, FixesRootAfterRightMerge)
{
    //      1:[5]      -->   1:[1, 5]
    // 2:[1]     3:[]
    TreeBuilder builder{tree()};
    setup_fixes_root_after_merge_test(builder);
    ASSERT_TRUE(tree_remove(tree(), make_key<1>(9)));

    ASSERT_TRUE(tree().node_contains(PID{1}, make_key<1>(1)));
    ASSERT_TRUE(tree().node_contains(PID{1}, make_key<1>(5)));
    ASSERT_FALSE(tree().node_contains(PID{1}, make_key<1>(9)));
}

//TEST_F(TreeTests, LeftMergeWithChildSplit)
//{
//    //      1:[5]               -->          1:[10]
//    // 2:[1]     3:[9, 10, 11]       2:[5, 9]      3:[11]
//    TreeBuilder builder {tree()};
//    setup_fixes_root_after_merge_test<2>(builder);
//    builder.node_insert(PID {3}, make_key<2>(10));
//    builder.node_insert(PID {3}, make_key<2>(11));
//    ASSERT_TRUE(tree_remove(tree(), make_key<2>(1)));
//
//    TreePrinter {tree()}.print();
//
//    ASSERT_TRUE(tree().node_contains(PID {2}, make_key<2>(5)));
//    ASSERT_TRUE(tree().node_contains(PID {2}, make_key<2>(9)));
//    ASSERT_TRUE(tree().node_contains(PID {1}, make_key<2>(10)));
//    ASSERT_TRUE(tree().node_contains(PID {3}, make_key<2>(11)));
//    validate();
//}

TEST_F(TreeTests, RightMergeWithChildSplit)
{
    //            1:[5]      -->           1:[2]
    // 2:[0, 1, 2]     3:[9]       2:[0, 1]     3:[5]
    auto builder = TreeBuilder{tree()};
    setup_fixes_root_after_merge_test(builder);
    builder.node_insert(PID {2}, make_key<1>(0));
    builder.node_insert(PID {2}, make_key<1>(2));
    ASSERT_TRUE(tree_remove(tree(), make_key<1>(9)));

    ASSERT_TRUE(tree().node_contains(PID {2}, make_key<1>(0)));
    ASSERT_TRUE(tree().node_contains(PID {2}, make_key<1>(1)));
    ASSERT_TRUE(tree().node_contains(PID {1}, make_key<1>(2)));
    ASSERT_TRUE(tree().node_contains(PID {3}, make_key<1>(5)));
    validate();
}

TEST_F(TreeTests, LeftRotationExternal)
{
    //      1:[5]                   -->       1:[9]
    // 2:[1]     3:[9, 10, 11, 12]       2:[5]     3:[10, 11, 12]
    auto builder = TreeBuilder{tree()};
    setup_fixes_root_after_merge_test<2>(builder);
    builder.node_insert(PID {3}, make_key<2>(10));
    builder.node_insert(PID {3}, make_key<2>(11));
    builder.node_insert(PID {3}, make_key<2>(12));
    ASSERT_TRUE(tree_remove(tree(), make_key<2>(1)));

    ASSERT_TRUE(tree().node_contains(PID {2}, make_key<2>(5)));
    ASSERT_TRUE(tree().node_contains(PID {1}, make_key<2>(9)));
    ASSERT_TRUE(tree().node_contains(PID {3}, make_key<2>(10)));
    ASSERT_TRUE(tree().node_contains(PID {3}, make_key<2>(11)));
    ASSERT_TRUE(tree().node_contains(PID {3}, make_key<2>(12)));
    validate();
}

TEST_F(TreeTests, RightRotationExternal)
{
    //               1:[5]      -->             1:[3]
    // 2:[0, 1, 2, 3]     3:[]       2:[0, 1, 2]     3:[5]
    auto builder = TreeBuilder{tree()};
    setup_fixes_root_after_merge_test(builder);
    builder.node_insert(PID {2}, make_key<1>(0));
    builder.node_insert(PID {2}, make_key<1>(2));
    builder.node_insert(PID {2}, make_key<1>(3));
    ASSERT_TRUE(tree_remove(tree(), make_key<1>(9)));

    ASSERT_TRUE(tree().node_contains(PID {2}, make_key<1>(0)));
    ASSERT_TRUE(tree().node_contains(PID {2}, make_key<1>(1)));
    ASSERT_TRUE(tree().node_contains(PID {2}, make_key<1>(2)));
    ASSERT_TRUE(tree().node_contains(PID {1}, make_key<1>(3)));
    ASSERT_TRUE(tree().node_contains(PID {3}, make_key<1>(5)));
    validate();
}

auto setup_internal_merge_test(TreeBuilder &builder) -> void
{
    const auto pt = PID::root();
    const auto pL = builder.allocate_node(PageType::INTERNAL_NODE);
    const auto pr = builder.allocate_node(PageType::INTERNAL_NODE);
    const auto LL = builder.allocate_node(PageType::EXTERNAL_NODE);
    const auto Lr = builder.allocate_node(PageType::EXTERNAL_NODE);
    const auto rL = builder.allocate_node(PageType::EXTERNAL_NODE);
    const auto rr = builder.allocate_node(PageType::EXTERNAL_NODE);

    //            1:[   4   ]
    //      2:[2]            3:[6]
    // 4:[1]     5:[3]  6:[5]     7:[7]
    builder.make_root_internal();
    builder.node_insert(pt, make_key(4));
    builder.node_insert(pL, make_key(2));
    builder.node_insert(pr, make_key(6));
    builder.node_insert(LL, make_key(1));
    builder.node_insert(Lr, make_key(3));
    builder.node_insert(rL, make_key(5));
    builder.node_insert(rr, make_key(7));

    builder.connect_parent_child(pt, pL, 0);
    builder.connect_parent_child(pt, pr, 1);
    builder.connect_parent_child(pL, LL, 0);
    builder.connect_parent_child(pL, Lr, 1);
    builder.connect_parent_child(pr, rL, 0);
    builder.connect_parent_child(pr, rr, 1);
    builder.connect_siblings(LL, Lr);
    builder.connect_siblings(Lr, rL);
    builder.connect_siblings(rL, rr);
}

TEST_F(TreeTests, LeftMergeInternal)
{
    //           1:[   4   ]                     1:[     4     ]                         1:[4,     6]
    //     2:[2]            3:[6]       -->  2:[]               3:[6]       -->  4:[2, 3]     6:[5]  7:[7]
    // 4:[]     5:[3]  6:[5]     7:[7]           4:[2, 3]  6:[5]     7:[7]
    TreeBuilder builder{tree()};
    setup_internal_merge_test(builder);

    ASSERT_TRUE(tree_remove(tree(), make_key(1)));
    validate();
}

//              1:[4]
//    2:[2]               3:[6]
//4:[]     5:[3]     6:[5]     7:[7]

//               1:[4]
//    2:[]                 3:[6]
//        5:[2,3]     6:[5]     7:[7]

TEST_F(TreeTests, RightMergeInternal)
{
    //            1:[   4   ]                           1:[   4   ]                        1:[2,     4]
    //      2:[2]            3:[6]      -->       2:[2]            3:[]          -->  4:[1]     5:[3]  6:[5, 6]
    // 4:[1]     5:[3]  6:[5]     7:[]       4:[1]     5:[3]           6:[5, 6]
    TreeBuilder builder{tree()};
    setup_internal_merge_test(builder);
    ASSERT_TRUE(tree_remove(tree(), make_key(7)));
    validate();
}

TEST_F(TreeTests, ModifiesExistingValue)
{
    TreeBuilder builder{tree()};
    builder.tree_insert(make_key(1), "a");
    builder.tree_insert(make_key(1), "b");
    ASSERT_TRUE(tree().node_contains(PID{1}, make_key(1)));
}

auto setup_remove_special_cases_test(TreeBuilder &builder) -> void
{
    // When a cell is removed from an internal node (I), Tree::remove() proceeds as follows. First, the cell to be removed (T),
    // is replaced with the cell (R) immediately preceding it in the B-Tree ordering. Note that R is to be found in an external
    // node (E). R could be in a node any number of levels down from T. If R is larger than T, then node I might overflow. In this
    // case we must split node I before continuing. This operation should always be safe, since we only touch node I and nodes that
    // are at lower levels than I, including possibly the root.
    //
    // Tree Structure:
    //                    1:[4,                 8,                   12,                    16,                    20]
    //          2:[2]                3:[6]              4:[10]                 5:[14]                 6:[18]                7:[22]
    //     8:[1]     9:[3]     10:[5]     11:[7]  12:[9]      13:[11]   14:[13]      15:[15]   16:[17]      17:[19]  18:[21]      19:[23]
    const auto small_size = get_max_local(builder.page_size()) / 7 * 3 + 2;
    for (Index i {}; i < 6; ++i)
        builder.allocate_node(PageType::INTERNAL_NODE);
    for (Index i {}; i < 12; ++i)
        builder.allocate_node(PageType::EXTERNAL_NODE);

    builder.make_root_internal();
    builder.node_insert(PID {1}, make_key(4), small_size);
    builder.node_insert(PID {1}, make_key(8), small_size);
    builder.node_insert(PID {1}, make_key(12), small_size);
    builder.node_insert(PID {1}, make_key(16), small_size);
    builder.node_insert(PID {1}, make_key(20), small_size);

    builder.node_insert(PID {2}, make_key(2));
    builder.node_insert(PID {3}, make_key(6));
    builder.node_insert(PID {4}, make_key(10));
    builder.node_insert(PID {5}, make_key(14));
    builder.node_insert(PID {6}, make_key(18));
    builder.node_insert(PID {7}, make_key(22));
    builder.node_insert(PID {8}, make_key(1));
    builder.node_insert(PID {9}, make_key(3));
    builder.node_insert(PID {10}, make_key(5));
    builder.node_insert(PID {11}, make_key(7));
    builder.node_insert(PID {12}, make_key(9));
    builder.node_insert(PID {13}, make_key(11));
    builder.node_insert(PID {14}, make_key(13));
    builder.node_insert(PID {15}, make_key(15));
    builder.node_insert(PID {16}, make_key(17));
    builder.node_insert(PID {17}, make_key(19));
    builder.node_insert(PID {18}, make_key(21));
    builder.node_insert(PID {19}, make_key(23));

    builder.connect_parent_child(PID {1}, PID {2}, 0);
    builder.connect_parent_child(PID {1}, PID {3}, 1);
    builder.connect_parent_child(PID {1}, PID {4}, 2);
    builder.connect_parent_child(PID {1}, PID {5}, 3);
    builder.connect_parent_child(PID {1}, PID {6}, 4);
    builder.connect_parent_child(PID {1}, PID {7}, 5);
    builder.connect_parent_child(PID {2}, PID {8}, 0);
    builder.connect_parent_child(PID {2}, PID {9}, 1);
    builder.connect_parent_child(PID {3}, PID {10}, 0);
    builder.connect_parent_child(PID {3}, PID {11}, 1);
    builder.connect_parent_child(PID {4}, PID {12}, 0);
    builder.connect_parent_child(PID {4}, PID {13}, 1);
    builder.connect_parent_child(PID {5}, PID {14}, 0);
    builder.connect_parent_child(PID {5}, PID {15}, 1);
    builder.connect_parent_child(PID {6}, PID {16}, 0);
    builder.connect_parent_child(PID {6}, PID {17}, 1);
    builder.connect_parent_child(PID {7}, PID {18}, 0);
    builder.connect_parent_child(PID {7}, PID {19}, 1);

    builder.connect_siblings(PID {8}, PID {9});
    builder.connect_siblings(PID {9}, PID {10});
    builder.connect_siblings(PID {10}, PID {11});
    builder.connect_siblings(PID {11}, PID {12});
    builder.connect_siblings(PID {12}, PID {13});
    builder.connect_siblings(PID {13}, PID {14});
    builder.connect_siblings(PID {14}, PID {15});
    builder.connect_siblings(PID {15}, PID {16});
    builder.connect_siblings(PID {16}, PID {17});
    builder.connect_siblings(PID {17}, PID {18});
    builder.connect_siblings(PID {18}, PID {19});
}

// TODO: Get these working again. The merge routine was changed and these tests are fragile since they depends on the tree structure.
//auto remove_special_cases_test(TestTree &tree, Index key) -> void
//{
//    // Tree Structure:
//    //                    1:[4,                 8,                   12,                    16,                    20]
//    //          2:[2]                3:[6]              4:[10]                 5:[14]                 6:[18]                7:[22]
//    //     8:[1]     9:[3]     10:[5]     11:[7]  12:[9]      13:[11]   14:[13]      15:[15]   16:[17]      17:[19]  18:[21]      19:[23]
//
//    ASSERT_TRUE(key == 4 || key == 8 || key == 12 || key == 16 || key == 20)
//        << "key " << key << " should be in the root";
//
//    TreeBuilder builder{tree};
//    setup_remove_special_cases_test(builder);
//
//    ASSERT_TRUE(tree_remove(tree, make_key(key)));
//    TreeValidator validator {tree};
//    validator.validate();
//
//    // If the root overflowed, it will have a single cell in it. TODO: Maybe use a mock. This seems hacky.
//    auto root = tree.acquire_node(PID::root(), true);
//    ASSERT_EQ(root.cell_count(), 1);
//}
//
//TEST_F(TreeTests, RemoveSpecialCaseA)
//{
//    remove_special_cases_test(tree(), 4);
//}
//
//TEST_F(TreeTests, RemoveSpecialCaseB)
//{
//    remove_special_cases_test(tree(), 8);
//}
//
//TEST_F(TreeTests, RemoveSpecialCaseC)
//{
//    remove_special_cases_test(tree(), 12);
//}
//
//TEST_F(TreeTests, RemoveSpecialCaseD)
//{
//    remove_special_cases_test(tree(), 16);
//}
//
//TEST_F(TreeTests, RemoveSpecialCaseE)
//{
//    remove_special_cases_test(tree(), 20);
//}

auto run_internal_overflow_after_modify_test(TestTree &tree, Index key_index) -> void
{
    const std::vector<Index> keys {4, 8, 12, 16, 20};
    const auto key = make_key(keys.at(key_index));
    TreeBuilder builder {tree};
    setup_remove_special_cases_test(builder);

    auto [node, index, found_eq] = tree.find_ge(stob(key), true);
    const auto space_in_node = node.usable_space();
    const auto value = tree.collect_value(node, index) +
                       std::string(space_in_node + 1, 'x');
    node.take();
    tree.insert(stob(key), stob(value));
    TreeValidator {tree}.validate();
}

TEST_F(TreeTests, InternalOverflowAfterModifyA)
{
    run_internal_overflow_after_modify_test(tree(), 0);
}

TEST_F(TreeTests, InternalOverflowAfterModifyB)
{
    run_internal_overflow_after_modify_test(tree(), 1);
}

TEST_F(TreeTests, InternalOverflowAfterModifyC)
{
    run_internal_overflow_after_modify_test(tree(), 2);
}

TEST_F(TreeTests, InternalOverflowAfterModifyD)
{
    run_internal_overflow_after_modify_test(tree(), 3);
}

TEST_F(TreeTests, InternalOverflowAfterModifyE)
{
    run_internal_overflow_after_modify_test(tree(), 4);
}

TEST_F(TreeTests, SanityCheck)
{
    std::unordered_map<std::string, std::string> payloads;
    constexpr Size max_size = 100;
    constexpr Size n = 1'000;

    for (Index i {}; i < n; ++i) {
        const auto r = m_random.next_int(5);
        std::string key {};
        if (r == 0) {
            // Short key. Could already be in the tree: if so, we'll need to modify rather than insert.
            key = m_random.next_string(1);
        } else if (r == 1) {
            // Long key.
            key = random_string(m_random, m_max_local / 2, m_max_local);
        } else {
            key = random_string(m_random, 3, 8);
        }
        // Value may need one or more overflow pages.
        const auto value = random_string(m_random, 5, m_max_local * 3);

        // Insert a key-value pair.
        tree_insert(tree(), key, value);
        payloads[key] = value;

        TreeValidator {tree()}.validate();

        // Remove a key-value pair.
        const auto too_many_records = tree().cell_count() > max_size;
        if (too_many_records || (m_random.next_int(5) < 3 && !payloads.empty())) {
            auto itr = payloads.begin();

            ASSERT_TRUE(tree_remove(tree(), itr->first))
                << "Unable to remove '" << itr->first << "': "
                << tree().cell_count() << " values remaining ";
            payloads.erase(itr);
        }

        TreeValidator {tree()}.validate();
    }
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
        for (const auto &[k, v]: records)
            tree_remove(tree(), k);
        ASSERT_EQ(m_tree->cell_count(), 0);
        records.clear();
    }
}

} // <anonymous>