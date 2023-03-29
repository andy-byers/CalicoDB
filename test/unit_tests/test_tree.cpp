// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#include "logging.h"
#include "tree.h"
#include "unit_tests.h"
#include <gtest/gtest.h>

namespace calicodb
{

static constexpr std::size_t kInitialRecordCount = 100;

class NodeSlotTests
    : public TestWithPager,
      public testing::Test
{
};

TEST_F(NodeSlotTests, SlotsAreConsistent)
{
    std::string backing(0x200, '\x00');
    std::string scratch(0x200, '\x00');

    Id freelist_head;
    Freelist freelist {*pager, freelist_head};

    Node node;
    ASSERT_OK(NodeManager::allocate(*pager, freelist, node, scratch, true));

    node.insert_slot(0, 2);
    node.insert_slot(1, 4);
    node.insert_slot(1, 3);
    node.insert_slot(0, 1);

    node.set_slot(0, node.get_slot(0) + 1);
    node.set_slot(1, node.get_slot(1) + 1);
    node.set_slot(2, node.get_slot(2) + 1);
    node.set_slot(3, node.get_slot(3) + 1);

    ASSERT_EQ(node.get_slot(0), 2);
    ASSERT_EQ(node.get_slot(1), 3);
    ASSERT_EQ(node.get_slot(2), 4);
    ASSERT_EQ(node.get_slot(3), 5);

    node.remove_slot(0);
    ASSERT_EQ(node.get_slot(0), 3);
    node.remove_slot(0);
    ASSERT_EQ(node.get_slot(0), 4);
    node.remove_slot(0);
    ASSERT_EQ(node.get_slot(0), 5);
    node.remove_slot(0);
}

class ComponentTests
    : public TestWithPager,
      public testing::Test
{
public:
    ~ComponentTests() override = default;

    auto SetUp() -> void override
    {
        collect_scratch.resize(kPageSize);
        cell_scratch.resize(kPageSize);
        node_scratch.resize(kPageSize);
        freelist = std::make_unique<Freelist>(*pager, freelist_head);

        Node root;
        ASSERT_OK(NodeManager::allocate(*pager, *freelist, root, node_scratch, true));
        NodeManager::release(*pager, std::move(root));
    }

    auto acquire_node(Id page_id, bool writable = false)
    {
        Node node;
        EXPECT_OK(NodeManager::acquire(*pager, page_id, node, node_scratch, writable));
        return node;
    }

    auto release_node(Node node) const
    {
        pager->release(std::move(node).take());
    }

    Id freelist_head;
    std::unique_ptr<Freelist> freelist;
    std::string node_scratch;
    std::string cell_scratch;
};

TEST_F(ComponentTests, CollectsPayload)
{
    auto root = acquire_node(Id::root(), true);
    ASSERT_OK(PayloadManager::emplace(*pager, *freelist, collect_scratch.data(), root, "hello", "world", 0));
    const auto cell = read_cell(root, 0);
    Slice slice;
    ASSERT_OK(PayloadManager::collect_key(*pager, scratch, cell, &slice));
    ASSERT_EQ(slice, "hello");
    ASSERT_OK(PayloadManager::collect_value(*pager, scratch, cell, &slice));
    ASSERT_EQ(slice, "world");
    root.TEST_validate();
    release_node(std::move(root));
}

TEST_F(ComponentTests, CollectsPayloadWithOverflow)
{
    auto root = acquire_node(Id::root(), true);
    const auto key = random.Generate(kPageSize * 100).to_string();
    const auto value = random.Generate(kPageSize * 100).to_string();
    ASSERT_OK(PayloadManager::emplace(*pager, *freelist, collect_scratch.data(), root, key, value, 0));
    const auto cell = read_cell(root, 0);
    Slice slice;
    ASSERT_OK(PayloadManager::collect_key(*pager, scratch, cell, &slice));
    ASSERT_EQ(slice, key);
    ASSERT_OK(PayloadManager::collect_value(*pager, scratch, cell, &slice));
    ASSERT_EQ(slice, value);
    root.TEST_validate();
    release_node(std::move(root));
}

TEST_F(ComponentTests, PromotedCellHasCorrectSize)
{
    auto root = acquire_node(Id::root(), true);
    const auto key = random.Generate(kPageSize * 100).to_string();
    const auto value = random.Generate(kPageSize * 100).to_string();
    std::string emplace_scratch(kPageSize, '\0');
    ASSERT_OK(PayloadManager::emplace(*pager, *freelist, nullptr, root, key, value, 0));
    auto cell = read_cell(root, 0);
    ASSERT_OK(PayloadManager::promote(*pager, *freelist, emplace_scratch.data() + 20, cell, Id::root()));
    release_node(std::move(root));

    Node internal;
    ASSERT_OK(NodeManager::allocate(*pager, *freelist, internal, node_scratch, false));
    write_cell(internal, 0, cell);
    cell = read_cell(internal, 0);

    // Needs to consult overflow pages for the key.
    Slice slice;
    ASSERT_OK(PayloadManager::collect_key(*pager, collect_scratch, cell, &slice));
    ASSERT_EQ(slice, key);
    internal.TEST_validate();
    release_node(std::move(internal));
}

static auto run_promotion_test(ComponentTests &test, std::size_t key_size, std::size_t value_size)
{
    auto root = test.acquire_node(Id::root(), true);
    const auto key = test.random.Generate(key_size).to_string();
    const auto value = test.random.Generate(value_size).to_string();
    std::string emplace_scratch(test.kPageSize, '\0');
    ASSERT_OK(PayloadManager::emplace(*test.pager, *test.freelist, emplace_scratch.data() + 10, root, key, value, 0));
    auto external_cell = read_cell(root, 0);
    ASSERT_EQ(external_cell.size, varint_length(key.size()) + varint_length(value.size()) + external_cell.local_size + external_cell.has_remote * Id::kSize);
    auto internal_cell = external_cell;
    ASSERT_OK(PayloadManager::promote(*test.pager, *test.freelist, emplace_scratch.data() + 10, internal_cell, Id::root()));
    ASSERT_EQ(internal_cell.size, Id::kSize + varint_length(key.size()) + internal_cell.local_size + internal_cell.has_remote * Id::kSize);
    test.release_node(std::move(root));
}

TEST_F(ComponentTests, CellIsPromoted)
{
    run_promotion_test(*this, 10, 10);
}

TEST_F(ComponentTests, PromotionCopiesOverflowKeyButIgnoresOverflowValue)
{
    run_promotion_test(*this, kPageSize, kPageSize);
    PointerMap::Entry old_head;
    ASSERT_OK(PointerMap::read_entry(*pager, Id(3), old_head));
    ASSERT_EQ(old_head.type, PointerMap::kOverflowHead);
    ASSERT_EQ(old_head.back_ptr, Id::root());

    // 1 overflow page needed for the key, and 1 for the value.
    PointerMap::Entry new_head;
    ASSERT_OK(PointerMap::read_entry(*pager, Id(5), new_head));
    ASSERT_EQ(new_head.type, PointerMap::kOverflowHead);
    ASSERT_EQ(new_head.back_ptr, Id::root());
}

TEST_F(ComponentTests, NodeIteratorHandlesOverflowKeys)
{
    std::vector<std::string> keys;
    for (std::size_t i = 0; i < 3; ++i) {
        auto root = acquire_node(Id::root(), true);
        auto key = random.Generate(kPageSize).to_string();
        const auto value = random.Generate(kPageSize).to_string();
        key[0] = static_cast<char>(i);
        ASSERT_OK(PayloadManager::emplace(*pager, *freelist, nullptr, root, key, value, i));
        ASSERT_FALSE(root.overflow.has_value());
        release_node(std::move(root));
        keys.emplace_back(key);
    }
    auto root = acquire_node(Id::root(), true);
    std::string lhs_key, rhs_key;
    NodeIterator itr {root, {
                                pager.get(),
                                &lhs_key,
                                &rhs_key,
                            }};
    std::size_t i = 0;
    for (const auto &key : keys) {
        ASSERT_OK(itr.seek(key));
        ASSERT_EQ(itr.index(), i++);
    }
    release_node(std::move(root));
}

class NodeTests
    : public TestWithPager,
      public testing::Test
{
public:
    NodeTests()
        : node_scratch(kPageSize, '\0'),
          cell_scratch(kPageSize, '\0'),
          freelist {*pager, freelist_head}
    {
    }

    [[nodiscard]] auto get_node(bool is_external) -> Node
    {
        Node node;
        EXPECT_OK(NodeManager::allocate(*pager, freelist, node, node_scratch, is_external));
        return node;
    }

    auto write_record(Node &node, const Slice &key, const Slice &value, std::size_t index) -> void
    {
        ASSERT_OK(PayloadManager::emplace(*pager, freelist, cell_scratch.data(), node, key, value, index));
    }

    [[nodiscard]] auto find_index(Node &node, const Slice &key, std::size_t *out) -> bool
    {
        Slice slice;
        for (std::size_t i = 0; i < node.header.cell_count; ++i) {
            const auto cell = read_cell(node, i);
            EXPECT_OK(PayloadManager::collect_key(*pager, collect_scratch, cell, &slice));
            if (key == slice) {
                *out = i;
                return true;
            }
        }
        return false;
    }

    [[nodiscard]] auto read_record(Node &node, const Slice &key) -> std::string
    {
        std::size_t index;
        if (find_index(node, key, &index)) {
            Slice slice;
            EXPECT_OK(PayloadManager::collect_value(*pager, collect_scratch, read_cell(node, index), &slice));
            return slice.to_string();
        }
        ADD_FAILURE() << "key \"" << key.to_string() << "\" was not found";
        return "";
    }

    auto erase_record(Node &node, const Slice &key) -> void
    {
        std::size_t index;
        if (find_index(node, key, &index)) {
            erase_cell(node, index);
            return;
        }
        ADD_FAILURE() << "key \"" << key.to_string() << "\" was not found";
    }

    Id freelist_head;
    Freelist freelist;
    std::string node_scratch;
    std::string cell_scratch;
    tools::RandomGenerator random;
};

class BlockAllocatorTests : public NodeTests
{
public:
    explicit BlockAllocatorTests()
        : node {get_node(true)}
    {
    }

    ~BlockAllocatorTests() override = default;

    auto reserve_for_test(std::size_t n) -> void
    {
        ASSERT_LT(n, node.page.size() - FileHeader::kSize - NodeHeader::kSize)
            << "reserve_for_test(" << n << ") leaves no room for possible headers";
        size = n;
        base = node.page.size() - n;
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
    (void)get_node(true);
    ASSERT_EQ(get_node(true).page.id(), Id(3));
}

TEST_F(NodeTests, NonRootFits4Records)
{
    (void)get_node(true);
    auto node = get_node(true);
    write_record(node, std::string(kPageSize, 'a'), "1", 0);
    write_record(node, std::string(kPageSize, 'b'), "2", 1);
    write_record(node, std::string(kPageSize, 'c'), "3", 2);
    write_record(node, std::string(kPageSize, 'd'), "4", 3);
    node.TEST_validate();

    ASSERT_EQ(node.header.cell_count, 4);
    ASSERT_GE(node.gap_size, (16 - varint_length(kPageSize) - varint_length(1)) * 4)
        << "node cannot account for maximally-sized varints";
}

TEST_F(NodeTests, ReadsAndWrites)
{
    const auto b = random.Generate(kPageSize * 2).to_string();
    const auto c = random.Generate(kPageSize * 3).to_string();
    const auto _1 = random.Generate(kPageSize * 4).to_string();
    const auto _3 = random.Generate(kPageSize * 5).to_string();

    auto node = get_node(true);
    write_record(node, "a", _1, 0);
    write_record(node, b, "2", 1);
    write_record(node, c, _3, 2);

    ASSERT_EQ(read_record(node, "a"), _1);
    ASSERT_EQ(read_record(node, b), "2");
    ASSERT_EQ(read_record(node, c), _3);
}

TEST_F(NodeTests, NodeFreeBlockList)
{
    const std::size_t kMaxExtra = 10;
    auto node = get_node(true);
    std::size_t extra = kMaxExtra;

    while (!node.overflow.has_value()) {
        write_record(node, "x", random.Generate(extra), node.header.cell_count);
        if (--extra == 0) {
            extra = kMaxExtra;
        }
    }
    node.overflow.reset();
    while (node.header.cell_count != 0) {
        erase_cell(node, 0);
    }
    ASSERT_EQ(node.header.cell_count, 0);

    extra = kMaxExtra;
    while (!node.overflow.has_value()) {
        write_record(node, "x", random.Generate(extra), node.header.cell_count);
        if (--extra == 0) {
            extra = kMaxExtra;
        }
    }
}

TEST_F(NodeTests, CellTransfer)
{
    const auto b = random.Generate(kPageSize * 2).to_string();
    const auto _1 = random.Generate(kPageSize * 4).to_string();

    auto node_1 = get_node(true);
    auto node_2 = get_node(true);
    write_record(node_1, "a", _1, 0);
    write_record(node_1, b, "2", 1);
    const auto cell_a = read_cell(node_1, 0);
    const auto cell_b = read_cell(node_1, 1);
    // Cells will share the same overflow chains.
    write_cell(node_2, 0, cell_a);
    write_cell(node_2, 1, cell_b);

    ASSERT_EQ(node_1.gap_size + FileHeader::kSize, node_2.gap_size);
    ASSERT_EQ(read_record(node_2, "a"), _1);
    ASSERT_EQ(read_record(node_2, b), "2");
}

TEST_F(NodeTests, CellPromote)
{
    const auto b = random.Generate(kPageSize * 2).to_string();
    const auto _1 = random.Generate(kPageSize * 4).to_string();

    auto node_1 = get_node(true);
    auto node_2 = get_node(false);
    write_record(node_1, "a", _1, 0);
    write_record(node_1, b, "2", 1);

    auto cell_a = read_cell(node_1, 0);
    ASSERT_OK(PayloadManager::promote(*pager, freelist, cell_scratch.data() + Id::kSize, cell_a, node_2.page.id()));
    ASSERT_FALSE(cell_a.has_remote) << "overflow value was copied for promoted cell";
    write_cell(node_2, 0, cell_a);

    auto cell_b = read_cell(node_1, 1);
    ASSERT_OK(PayloadManager::promote(*pager, freelist, cell_scratch.data() + Id::kSize, cell_b, node_2.page.id()));
    ASSERT_TRUE(cell_b.has_remote) << "overflow key was not copied for promoted cell";
    write_cell(node_2, 1, cell_b);

    auto c = read_cell(node_2, 0);
    Slice key;
    EXPECT_OK(PayloadManager::collect_key(*pager, collect_scratch, read_cell(node_2, 0), &key));
    ASSERT_EQ(key, "a");

    EXPECT_OK(PayloadManager::collect_key(*pager, collect_scratch, read_cell(node_2, 1), &key));
    ASSERT_EQ(key, b);
}

TEST_F(NodeTests, Defragmentation)
{
    auto node = get_node(true);
    write_record(node, "e", "5", 0);
    write_record(node, "d", "4", 0);
    write_record(node, "c", "3", 0);
    write_record(node, "b", "", 0);
    write_record(node, "a", "1", 0);

    erase_record(node, "b");
    erase_record(node, "d");

    ASSERT_NE(node.header.frag_count, 0);
    ASSERT_NE(node.header.free_start, 0);
    BlockAllocator::defragment(node);
    ASSERT_EQ(node.header.frag_count, 0);
    ASSERT_EQ(node.header.free_start, 0);

    ASSERT_EQ(read_record(node, "a"), "1");
    ASSERT_EQ(read_record(node, "c"), "3");
    ASSERT_EQ(read_record(node, "e"), "5");
}

struct TreeTestParameters {
    std::size_t page_size = 0;
    std::size_t extra = 0;
};

class TreeTests
    : public TestWithPager,
      public testing::TestWithParam<TreeTestParameters>
{
public:
    TreeTests()
        : param {GetParam()},
          collect_scratch(param.page_size, '\x00'),
          root_id {Id::root()}
    {
    }

    auto SetUp() -> void override
    {
        ASSERT_OK(Tree::create(*pager, Id::root(), freelist_head, &root_id));
        tree = std::make_unique<Tree>(*pager, root_id, freelist_head, nullptr);
    }

    [[nodiscard]] auto make_long_key(std::size_t value) const
    {
        const auto suffix = tools::integral_key<6>(value);
        const std::string key(param.page_size * 2 - suffix.size(), '0');
        return key + suffix;
    }

    [[nodiscard]] auto make_value(char c, bool overflow = false) const
    {
        std::size_t size {param.page_size};
        if (overflow) {
            size /= 3;
        } else {
            size /= 20;
        }
        return std::string(size, c);
    }

    Id freelist_head;
    TreeTestParameters param;
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
    ASSERT_OK(tree->put("a", make_value('x'), &exists));
    ASSERT_FALSE(exists);
    ASSERT_OK(tree->put("b", make_value('2'), &exists));
    ASSERT_FALSE(exists);
    ASSERT_OK(tree->put("a", make_value('1'), &exists));
    ASSERT_TRUE(exists);

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
    ASSERT_TRUE(tree->erase("a").is_not_found());
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
    for (std::size_t i {}, j {kInitialRecordCount - 1}; i < j; ++i, --j) {
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
        char key[3] {};
        put_u16(key, kInitialRecordCount - i - 1);
        ASSERT_OK(tree->put({key, 2}, "v"));
    }
    for (unsigned i = 0; i < kInitialRecordCount; ++i) {
        const auto key = random.Generate(GetParam().page_size);
        ASSERT_OK(tree->put(key, "v"));
    }
    tree->TEST_validate();
}

TEST_P(TreeTests, AllowsNonInsertOperationsOnEmptyKeys)
{
    std::string value;
    ASSERT_OK(tree->put("key", "value"));
    ASSERT_TRUE(tree->get("", &value).is_not_found());
    ASSERT_TRUE(tree->erase("").is_not_found());
}

#if not NDEBUG
TEST_P(TreeTests, InsertEmptyKeyDeathTest)
{
    ASSERT_DEATH((void)tree->put("", "value"), kExpectationMatcher);
}
#endif // NDEBUG

INSTANTIATE_TEST_SUITE_P(
    TreeTests,
    TreeTests,
    ::testing::Values(
        TreeTestParameters {kMinPageSize},
        TreeTestParameters {kMinPageSize * 2},
        TreeTestParameters {kMaxPageSize / 2},
        TreeTestParameters {kMaxPageSize}));

class TreeSanityChecks : public TreeTests
{
public:
    auto random_chunk(bool overflow, bool nonzero = true)
    {
        return random.Generate(random.Next(nonzero, param.page_size * overflow + 12));
    }

    auto random_write() -> Record
    {
        const auto key = random_chunk(overflow_keys);
        const auto val = random_chunk(overflow_values, false);
        EXPECT_OK(tree->put(key, val));
        return {key.to_string(), val.to_string()};
    }

    bool overflow_keys = GetParam().extra & 0b10;
    bool overflow_values = GetParam().extra & 0b01;
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

    for (const auto &[key, value] : records) {
        std::string result;
        ASSERT_OK(tree->get(key, &result));
        ASSERT_EQ(result, value);
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

// "extra" parameter bits:
//     0b01: Use overflowing values
//     0b10: Use overflowing keys
INSTANTIATE_TEST_SUITE_P(
    TreeSanityChecks,
    TreeSanityChecks,
    ::testing::Values(
        TreeTestParameters {kMinPageSize, 0b00},
        TreeTestParameters {kMinPageSize, 0b01},
        TreeTestParameters {kMinPageSize, 0b10},
        TreeTestParameters {kMinPageSize, 0b11},
        TreeTestParameters {kMaxPageSize, 0b00},
        TreeTestParameters {kMaxPageSize, 0b01},
        TreeTestParameters {kMaxPageSize, 0b10},
        TreeTestParameters {kMaxPageSize, 0b11}));

class EmptyTreeCursorTests : public TreeTests
{
protected:
    auto SetUp() -> void override
    {
        TreeTests::SetUp();
    }
};

TEST_P(EmptyTreeCursorTests, KeyAndValueUseSeparateMemory)
{
    std::unique_ptr<Cursor> cursor {CursorInternal::make_cursor(*tree)};
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
    ::testing::Values(TreeTestParameters {kMinPageSize}));

class CursorTests : public TreeTests
{
protected:
    auto SetUp() -> void override
    {
        TreeTests::SetUp();
        add_initial_records(*this);
    }
};

TEST_P(CursorTests, KeyAndValueUseSeparateMemory)
{
    std::unique_ptr<Cursor> cursor {CursorInternal::make_cursor(*tree)};
    cursor->seek_first();
    ASSERT_TRUE(cursor->is_valid());
    const auto k = cursor->key();
    const auto v = cursor->value();
    ASSERT_NE(k, v);
}

TEST_P(CursorTests, SeeksForward)
{
    std::unique_ptr<Cursor> cursor {CursorInternal::make_cursor(*tree)};
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
    std::unique_ptr<Cursor> cursor {CursorInternal::make_cursor(*tree)};
    cursor->seek(make_long_key(kInitialRecordCount / 4));
    while (cursor->is_valid()) {
        cursor->next();
    }
}

TEST_P(CursorTests, SeeksForwardToBoundary)
{
    std::unique_ptr<Cursor> cursor {CursorInternal::make_cursor(*tree)};
    std::unique_ptr<Cursor> bounds {CursorInternal::make_cursor(*tree)};
    cursor->seek_first();
    bounds->seek(make_long_key(kInitialRecordCount * 3 / 4));
    while (cursor->key() != bounds->key()) {
        ASSERT_TRUE(cursor->is_valid());
        cursor->next();
    }
}

TEST_P(CursorTests, SeeksForwardBetweenBoundaries)
{
    std::unique_ptr<Cursor> cursor {CursorInternal::make_cursor(*tree)};
    cursor->seek(make_long_key(kInitialRecordCount / 4));
    std::unique_ptr<Cursor> bounds {CursorInternal::make_cursor(*tree)};
    bounds->seek(make_long_key(kInitialRecordCount * 3 / 4));
    while (cursor->key() != bounds->key()) {
        ASSERT_TRUE(cursor->is_valid());
        cursor->next();
    }
}

TEST_P(CursorTests, SeeksBackward)
{
    std::unique_ptr<Cursor> cursor {CursorInternal::make_cursor(*tree)};
    cursor->seek_last();
    std::size_t i = 0;
    while (cursor->is_valid()) {
        ASSERT_EQ(cursor->key().to_string(), make_long_key(kInitialRecordCount - 1 - i++));
        ASSERT_EQ(cursor->value().to_string(), make_value('v'));
        cursor->previous();
    }
    ASSERT_EQ(i, kInitialRecordCount);
}

TEST_P(CursorTests, SeeksBackwardFromBoundary)
{
    std::unique_ptr<Cursor> cursor {CursorInternal::make_cursor(*tree)};
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
    std::unique_ptr<Cursor> cursor {CursorInternal::make_cursor(*tree)};
    cursor->seek_last();
    std::unique_ptr<Cursor> bounds {CursorInternal::make_cursor(*tree)};
    bounds->seek(make_long_key(kInitialRecordCount / 4));
    while (cursor->key() != bounds->key()) {
        ASSERT_TRUE(cursor->is_valid());
        cursor->previous();
    }
}

TEST_P(CursorTests, SeeksBackwardBetweenBoundaries)
{
    std::unique_ptr<Cursor> cursor {CursorInternal::make_cursor(*tree)};
    std::unique_ptr<Cursor> bounds {CursorInternal::make_cursor(*tree)};
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
    std::unique_ptr<Cursor> cursor {CursorInternal::make_cursor(*tree)};
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
    std::unique_ptr<Cursor> cursor {CursorInternal::make_cursor(*tree)};
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

#if not NDEBUG
TEST_P(CursorTests, InvalidCursorDeathTest)
{
    std::unique_ptr<Cursor> cursor {CursorInternal::make_cursor(*tree)};
    ASSERT_DEATH((void)cursor->key(), kExpectationMatcher);
    ASSERT_DEATH((void)cursor->value(), kExpectationMatcher);
    ASSERT_DEATH((void)cursor->next(), kExpectationMatcher);
    ASSERT_DEATH((void)cursor->previous(), kExpectationMatcher);
}
#endif // NDEBUG

INSTANTIATE_TEST_SUITE_P(
    CursorTests,
    CursorTests,
    ::testing::Values(
        TreeTestParameters {kMinPageSize},
        TreeTestParameters {kMinPageSize * 2},
        TreeTestParameters {kMaxPageSize / 2},
        TreeTestParameters {kMaxPageSize}));

class PointerMapTests : public TreeTests
{
public:
    [[nodiscard]] auto map_size() -> std::size_t
    {
        return pager->page_size() / (sizeof(char) + Id::kSize);
    }
};

TEST_P(PointerMapTests, FirstPointerMapIsPage2)
{
    ASSERT_EQ(PointerMap::lookup(*pager, Id(0)), Id(0));
    ASSERT_EQ(PointerMap::lookup(*pager, Id(1)), Id(0));
    ASSERT_EQ(PointerMap::lookup(*pager, Id(2)), Id(2));
    ASSERT_EQ(PointerMap::lookup(*pager, Id(3)), Id(2));
    ASSERT_EQ(PointerMap::lookup(*pager, Id(4)), Id(2));
    ASSERT_EQ(PointerMap::lookup(*pager, Id(5)), Id(2));
}

TEST_P(PointerMapTests, ReadsAndWritesEntries)
{
    std::string buffer(pager->page_size(), '\0');
    Page map_page;
    map_page.TEST_populate(Id(2), buffer.data(), buffer.size(), true);

    ASSERT_OK(PointerMap::write_entry(*pager, Id(3), PointerMap::Entry {Id(33), PointerMap::kTreeNode}));
    ASSERT_OK(PointerMap::write_entry(*pager, Id(4), PointerMap::Entry {Id(44), PointerMap::kFreelistLink}));
    ASSERT_OK(PointerMap::write_entry(*pager, Id(5), PointerMap::Entry {Id(55), PointerMap::kOverflowLink}));

    PointerMap::Entry entry_1, entry_2, entry_3;
    ASSERT_OK(PointerMap::read_entry(*pager, Id(3), entry_1));
    ASSERT_OK(PointerMap::read_entry(*pager, Id(4), entry_2));
    ASSERT_OK(PointerMap::read_entry(*pager, Id(5), entry_3));

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
        ASSERT_OK(pager->allocate(page));
        pager->release(std::move(page));
    }

    for (std::size_t i = 0; i < map_size() + 10; ++i) {
        if (i != map_size()) {
            const Id id(i + 3);
            const PointerMap::Entry entry {id, PointerMap::kTreeNode};
            ASSERT_OK(PointerMap::write_entry(*pager, id, entry));
        }
    }
    for (std::size_t i = 0; i < map_size() + 10; ++i) {
        if (i != map_size()) {
            const Id id(i + 3);
            PointerMap::Entry entry;
            ASSERT_OK(PointerMap::read_entry(*pager, id, entry));
            ASSERT_EQ(entry.back_ptr.value, id.value);
            ASSERT_EQ(entry.type, PointerMap::kTreeNode);
        }
    }
}

TEST_P(PointerMapTests, MapPagesAreRecognized)
{
    Id id(2);
    ASSERT_EQ(PointerMap::lookup(*pager, id), id);

    // Back pointers for the next "map.map_size()" pages are stored on page 2. The next pointermap page is
    // the page following the last page whose back pointer is on page 2. This pattern continues forever.
    for (std::size_t i = 0; i < 1'000'000; ++i) {
        id.value += map_size() + 1;
        ASSERT_EQ(PointerMap::lookup(*pager, id), id);
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
            ASSERT_EQ(PointerMap::lookup(*pager, page_id), map_id);
        }
    }
}

INSTANTIATE_TEST_SUITE_P(
    PointerMapTests,
    PointerMapTests,
    ::testing::Values(
        TreeTestParameters {kMinPageSize},
        TreeTestParameters {kMinPageSize * 2},
        TreeTestParameters {kMaxPageSize / 2},
        TreeTestParameters {kMaxPageSize}));

class VacuumTests : public TreeTests
{
public:
    auto SetUp() -> void override
    {
        TreeTests::SetUp();
        freelist = std::make_unique<Freelist>(*pager, freelist_head);
        cell_scratch.resize(kPageSize);
        node_scratch.resize(kPageSize);
    }

    auto acquire_node(Id pid, bool is_writable = false)
    {
        Node node;
        EXPECT_OK(NodeManager::acquire(*pager, pid, node, node_scratch, is_writable));
        return node;
    }

    auto allocate_node(bool is_external)
    {
        Node node;
        EXPECT_OK(NodeManager::allocate(*pager, *freelist, node, node_scratch, is_external));
        return node;
    }

    auto release_node(Node node) const
    {
        NodeManager::release(*pager, std::move(node));
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
            keys.emplace_back(random.Generate(max_key_size).to_string());
            (void)tree->put(keys.back(), random.Generate(max_value_size));
            ASSERT_NE(PointerMap::lookup(*pager, Id(pager->page_count())), Id(pager->page_count()));
        }
        for (const auto &key : keys) {
            ASSERT_OK(tree->erase(key));
        }

        bool vacuumed;
        Id target(pager->page_count());
        TableSet table_set;
        do {
            ASSERT_OK(tree->vacuum_one(target, table_set, &vacuumed));
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

            TableSet table_set;
            Id target(pager->page_count());
            for (;;) {
                bool vacuumed = false;
                ASSERT_OK(tree->vacuum_one(target, table_set, &vacuumed));
                if (!vacuumed) {
                    break;
                }
                tree->TEST_validate();
                --target.value;
            }

            ASSERT_OK(pager->truncate(target.value));

            auto *cursor = CursorInternal::make_cursor(*tree);
            for (const auto &[key, value] : map) {
                cursor->seek(key);
                ASSERT_TRUE(cursor->is_valid());
                ASSERT_EQ(cursor->key(), key);
                ASSERT_EQ(cursor->value(), value);
            }
            delete cursor;
        }
    }

    std::string node_scratch;
    std::string cell_scratch;
    std::unique_ptr<Freelist> freelist;
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

    ASSERT_OK(freelist->push(std::move(node_5.page)));
    ASSERT_OK(freelist->push(std::move(node_4.page)));
    ASSERT_OK(freelist->push(std::move(node_3.page)));

    PointerMap::Entry entry;
    ASSERT_OK(PointerMap::read_entry(*pager, Id(5), entry));
    ASSERT_EQ(entry.type, PointerMap::kFreelistLink);
    ASSERT_EQ(entry.back_ptr, Id(4));

    ASSERT_OK(PointerMap::read_entry(*pager, Id(4), entry));
    ASSERT_EQ(entry.type, PointerMap::kFreelistLink);
    ASSERT_EQ(entry.back_ptr, Id(3));

    ASSERT_OK(PointerMap::read_entry(*pager, Id(3), entry));
    ASSERT_EQ(entry.type, PointerMap::kFreelistLink);
    ASSERT_EQ(entry.back_ptr, Id::null());
}

TEST_P(VacuumTests, OverflowChainRegistersBackPointers)
{
    // Creates an overflow chain of length 2, rooted at the second cell on the root page.
    std::string overflow_data(kPageSize * 2, 'x');
    ASSERT_OK(tree->put("a", overflow_data));

    PointerMap::Entry head_entry, tail_entry;
    ASSERT_OK(PointerMap::read_entry(*pager, Id(3), head_entry));
    ASSERT_OK(PointerMap::read_entry(*pager, Id(4), tail_entry));

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
        ASSERT_OK(pager->allocate(page_4));
        ASSERT_EQ(page_4.id().value, 4);
        write_next_id(node_3.page, Id(3));
        write_next_id(page_4, Id(3));
        ASSERT_OK(freelist->push(std::move(page_4)));
        ASSERT_OK(freelist->push(std::move(node_3.page)));
    }

    ASSERT_OK(tree->put("a", std::string(3 * kPageSize / 2, 'x')));

    Page page_3;
    Page page_4;
    ASSERT_OK(pager->acquire(Id(3), page_3));
    ASSERT_OK(pager->acquire(Id(4), page_4));
    ASSERT_EQ(read_next_id(page_3), Id(4));
    ASSERT_EQ(read_next_id(page_4), Id::null());
    pager->release(std::move(page_3));
    pager->release(std::move(page_4));
}

TEST_P(VacuumTests, VacuumsFreelistInOrder)
{
    TableSet table_set;
    auto node_3 = allocate_node(true);
    auto node_4 = allocate_node(true);
    auto node_5 = allocate_node(true);
    ASSERT_EQ(node_5.page.id().value, 5);

    // Page Types:     N   P   3   2   1
    // Page Contents: [1] [2] [3] [4] [5]
    // Page IDs:       1   2   3   4   5
    ASSERT_OK(freelist->push(std::move(node_3.page)));
    ASSERT_OK(freelist->push(std::move(node_4.page)));
    ASSERT_OK(freelist->push(std::move(node_5.page)));

    // Page Types:     N   P   2   1
    // Page Contents: [1] [2] [3] [4] [X]
    // Page IDs:       1   2   3   4   5
    bool vacuumed = false;
    ASSERT_OK(tree->vacuum_one(Id(5), table_set, &vacuumed));
    ASSERT_TRUE(vacuumed);

    PointerMap::Entry entry;
    ASSERT_OK(PointerMap::read_entry(*pager, Id(4), entry));
    ASSERT_EQ(entry.type, PointerMap::kFreelistLink);
    ASSERT_EQ(entry.back_ptr, Id::null());

    // Page Types:     N   P   1
    // Page Contents: [1] [2] [3] [X] [X]
    // Page IDs:       1   2   3   4   5
    ASSERT_OK(tree->vacuum_one(Id(4), table_set, &vacuumed));
    ASSERT_TRUE(vacuumed);
    ASSERT_OK(PointerMap::read_entry(*pager, Id(3), entry));
    ASSERT_EQ(entry.type, PointerMap::kFreelistLink);
    ASSERT_EQ(entry.back_ptr, Id::null());

    // Page Types:     N   P
    // Page Contents: [1] [2] [X] [X] [X]
    // Page IDs:       1   2   3   4   5
    ASSERT_OK(tree->vacuum_one(Id(3), table_set, &vacuumed));
    ASSERT_TRUE(vacuumed);
    ASSERT_TRUE(freelist->is_empty());

    // Page Types:     N
    // Page Contents: [1] [X] [X] [X] [X]
    // Page IDs:       1   2   3   4   5
    ASSERT_OK(tree->vacuum_one(Id(2), table_set, &vacuumed));
    ASSERT_TRUE(vacuumed);

    // Page Types:     N
    // Page Contents: [1]
    // Page IDs:       1
    ASSERT_OK(pager->truncate(1));
    ASSERT_EQ(pager->page_count(), 1);
}

TEST_P(VacuumTests, VacuumsFreelistInReverseOrder)
{
    TableSet table_set;
    auto node_3 = allocate_node(true);
    auto node_4 = allocate_node(true);
    auto node_5 = allocate_node(true);

    // Page Types:     N   P   1   2   3
    // Page Contents: [a] [b] [c] [d] [e]
    // Page IDs:       1   2   3   4   5
    ASSERT_OK(freelist->push(std::move(node_5.page)));
    ASSERT_OK(freelist->push(std::move(node_4.page)));
    ASSERT_OK(freelist->push(std::move(node_3.page)));

    // Step 1:
    //     Page Types:     N   P       1   2
    //     Page Contents: [a] [b] [e] [d] [e]
    //     Page IDs:       1   2   3   4   5

    // Step 2:
    //     Page Types:     N   P   2   1
    //     Page Contents: [a] [b] [e] [d] [ ]
    //     Page IDs:       1   2   3   4   5
    bool vacuumed = false;
    ASSERT_OK(tree->vacuum_one(Id(5), table_set, &vacuumed));
    ASSERT_TRUE(vacuumed);
    PointerMap::Entry entry;
    ASSERT_OK(PointerMap::read_entry(*pager, Id(4), entry));
    ASSERT_EQ(entry.back_ptr, Id::null());
    ASSERT_EQ(entry.type, PointerMap::kFreelistLink);
    {
        Page page;
        ASSERT_OK(pager->acquire(Id(4), page));
        ASSERT_EQ(read_next_id(page), Id(3));
        pager->release(std::move(page));
    }

    // Page Types:     N   P   1
    // Page Contents: [a] [b] [e] [ ] [ ]
    // Page IDs:       1   2   3   4   5
    ASSERT_OK(tree->vacuum_one(Id(4), table_set, &vacuumed));
    ASSERT_TRUE(vacuumed);
    ASSERT_OK(PointerMap::read_entry(*pager, Id(3), entry));
    ASSERT_EQ(entry.type, PointerMap::kFreelistLink);
    ASSERT_EQ(entry.back_ptr, Id::null());

    // Page Types:     N   P
    // Page Contents: [a] [b] [ ] [ ] [ ]
    // Page IDs:       1   2   3   4   5
    ASSERT_OK(tree->vacuum_one(Id(3), table_set, &vacuumed));
    ASSERT_TRUE(vacuumed);
    ASSERT_TRUE(freelist->is_empty());

    // Page Types:     N
    // Page Contents: [a] [ ] [ ] [ ] [ ]
    // Page IDs:       1   2   3   4   5
    ASSERT_OK(tree->vacuum_one(Id(2), table_set, &vacuumed));
    ASSERT_TRUE(vacuumed);

    // Page Types:     N
    // Page Contents: [a]
    // Page IDs:       1
    ASSERT_OK(pager->truncate(1));
    ASSERT_EQ(pager->page_count(), 1);
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
    std::default_random_engine rng(42);
    TableSet table_set;

    for (std::size_t iteration = 0; iteration < 1'000; ++iteration) {
        std::vector<Node> nodes;
        for (std::size_t i = 0; i < kFrameCount - 1; ++i) {
            nodes.emplace_back(allocate_node(true));
        }

        std::shuffle(begin(nodes), end(nodes), rng);

        for (auto &node : nodes) {
            ASSERT_OK(freelist->push(std::move(node.page)));
        }

        // This will vacuum the whole freelist, as well as the pointer map page on page 2.
        Id target(pager->page_count());
        bool vacuumed = false;
        for (std::size_t i = 0; i < kFrameCount; ++i) {
            ASSERT_OK(tree->vacuum_one(target, table_set, &vacuumed));
            ASSERT_TRUE(vacuumed);
            --target.value;
        }
        ASSERT_OK(tree->vacuum_one(target, table_set, &vacuumed));
        ASSERT_FALSE(vacuumed);
        ASSERT_OK(pager->truncate(1));
        ASSERT_EQ(pager->page_count(), 1);
    }
}

static auto vacuum_and_validate(VacuumTests &test, const std::string &value)
{
    TableSet table_set;
    bool vacuumed;
    ASSERT_EQ(test.pager->page_count(), 6);
    ASSERT_OK(test.tree->vacuum_one(Id(6), table_set, &vacuumed));
    ASSERT_TRUE(vacuumed);
    ASSERT_OK(test.tree->vacuum_one(Id(5), table_set, &vacuumed));
    ASSERT_TRUE(vacuumed);
    ASSERT_OK(test.pager->truncate(4));
    ASSERT_OK(test.pager->flush());
    ASSERT_EQ(test.pager->page_count(), 4);

    std::string result;
    ASSERT_OK(test.tree->get("a", &result));
    ASSERT_EQ(result, "value");
    ASSERT_OK(test.tree->get("b", &result));
    ASSERT_EQ(result, value);
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

    ASSERT_OK(freelist->push(std::move(node_3.page)));
    ASSERT_OK(freelist->push(std::move(node_4.page)));

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
    vacuum_and_validate(*this, overflow_data);

    PointerMap::Entry head_entry, tail_entry;
    ASSERT_OK(PointerMap::read_entry(*pager, Id(3), head_entry));
    ASSERT_OK(PointerMap::read_entry(*pager, Id(4), tail_entry));

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
    ASSERT_OK(freelist->push(std::move(node_5.page)));
    ASSERT_OK(freelist->push(std::move(node_6.page)));

    std::string overflow_data(3 * kPageSize / 2, 'x');
    ASSERT_OK(tree->put("a", "value"));
    ASSERT_OK(tree->put("b", overflow_data));

    // Page Types:     n   p   2   1   B   A
    // Page Contents: [a] [b] [c] [d] [e] [f]
    // Page IDs:       1   2   3   4   5   6
    ASSERT_OK(freelist->push(std::move(node_3.page)));
    ASSERT_OK(freelist->push(std::move(node_4.page)));

    // Page Types:     n   p   1   A   B
    // Page Contents: [a] [b] [c] [f] [e] [ ]
    // Page IDs:       1   2   3   4   5   6

    // Page Types:     n   p   B   A
    // Page Contents: [a] [b] [e] [f] [ ] [ ]
    // Page IDs:       1   2   3   4   5   6

    // Page Types:     n   p   B   A
    // Page Contents: [a] [b] [e] [f]
    // Page IDs:       1   2   3   4
    vacuum_and_validate(*this, overflow_data);

    PointerMap::Entry head_entry, tail_entry;
    ASSERT_OK(PointerMap::read_entry(*pager, Id(4), head_entry));
    ASSERT_OK(PointerMap::read_entry(*pager, Id(3), tail_entry));

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
        ASSERT_OK(freelist->push(std::move(reserved.back().page)));
        reserved.pop_back();
    }

    TableSet table_set;
    bool vacuumed;
    ASSERT_EQ(pager->page_count(), 12);
    ASSERT_OK(tree->vacuum_one(Id(12), table_set, &vacuumed));
    ASSERT_OK(tree->vacuum_one(Id(11), table_set, &vacuumed));
    ASSERT_OK(tree->vacuum_one(Id(10), table_set, &vacuumed));
    ASSERT_OK(tree->vacuum_one(Id(9), table_set, &vacuumed));
    ASSERT_OK(tree->vacuum_one(Id(8), table_set, &vacuumed));
    ASSERT_OK(pager->truncate(7));
    ASSERT_EQ(pager->page_count(), 7);

    auto *cursor = CursorInternal::make_cursor(*tree);
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
    ASSERT_OK(freelist->push(std::move(node_3.page)));
    ASSERT_OK(freelist->push(std::move(node_4.page)));

    // Page Types:     n   p   1   n   n
    // Page Contents: [a] [b] [c] [f] [e] [ ]
    // Page IDs:       1   2   3   4   5   6

    // Page Types:     n   p   n   n
    // Page Contents: [a] [b] [e] [f] [ ] [ ]
    // Page IDs:       1   2   3   4   5   6

    // Page Types:     n   p   n   n
    // Page Contents: [a] [b] [e] [f]
    // Page IDs:       1   2   3   4
    ASSERT_EQ(pager->page_count(), 6)
        << "test was incorrectly initialized (check the key and value sizes)";
    bool vacuumed = false;
    TableSet table_set;
    ASSERT_OK(tree->vacuum_one(Id(6), table_set, &vacuumed));
    ASSERT_TRUE(vacuumed);
    ASSERT_OK(tree->vacuum_one(Id(5), table_set, &vacuumed));
    ASSERT_TRUE(vacuumed);
    ASSERT_OK(pager->truncate(4));

    auto *cursor = CursorInternal::make_cursor(*tree);
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
    ::testing::Values(
        TreeTestParameters {kMinPageSize},
        TreeTestParameters {kMinPageSize * 2},
        TreeTestParameters {kMaxPageSize / 2},
        TreeTestParameters {kMaxPageSize}));

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
        ++last_tree_id.value;
        EXPECT_OK(Tree::create(*pager, last_tree_id, freelist_head, &root));
        root_ids.emplace_back(Id::root(), root);
        multi_tree.emplace_back(std::make_unique<Tree>(*pager, root_ids.back().page_id, freelist_head, nullptr));
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
    std::list<LogicalPageId> root_ids;
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
    ::testing::Values(
        TreeTestParameters {kMinPageSize}));

} // namespace calicodb