#include "cursor_impl.h"
#include "memory.h"
#include "node.h"
#include "tree.h"
#include "unit_tests.h"
#include <gtest/gtest.h>

namespace calicodb
{

static constexpr std::size_t initial_record_count {100};

class NodeMetaManager
{
    NodeMeta m_external_meta;
    NodeMeta m_internal_meta;

public:
    explicit NodeMetaManager(std::size_t page_size)
    {
        m_external_meta.min_local = compute_min_local(page_size);
        m_external_meta.max_local = compute_max_local(page_size);
        m_internal_meta.min_local = m_external_meta.min_local;
        m_internal_meta.max_local = m_external_meta.max_local;

        m_external_meta.cell_size = external_cell_size;
        m_external_meta.parse_cell = parse_external_cell;

        m_internal_meta.cell_size = internal_cell_size;
        m_internal_meta.parse_cell = parse_internal_cell;
    }

    [[nodiscard]] auto operator()(bool is_external) const -> const NodeMeta &
    {
        return is_external ? m_external_meta : m_internal_meta;
    }
};

class NodeSlotTests
    : public TestWithPager,
      public testing::Test
{
};

TEST_F(NodeSlotTests, SlotsAreConsistent)
{
    std::string backing(0x200, '\x00');
    std::string scratch(0x200, '\x00');

    Page page;
    ASSERT_OK(pager->allocate(page));

    Node node;
    node.header.cell_start = node.page.size();
    node.page = std::move(page);
    node.scratch = scratch.data();
    node.initialize();
    const auto space = usable_space(node);

    node.insert_slot(0, 2);
    node.insert_slot(1, 4);
    node.insert_slot(1, 3);
    node.insert_slot(0, 1);
    ASSERT_EQ(usable_space(node), space - 8);

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
    ASSERT_EQ(usable_space(node), space);
}

class ComponentTests
    : public TestWithPager,
      public testing::Test
{
public:
    ~ComponentTests() override = default;

    auto SetUp() -> void override
    {
        collect_scratch.resize(PAGE_SIZE);

        pointers = std::make_unique<PointerMap>(*pager);
        freelist = std::make_unique<Freelist>(*pager, *pointers);
        overflow = std::make_unique<OverflowList>(*pager, *freelist, *pointers);
        payloads = std::make_unique<PayloadManager>(meta(true), *overflow);

        Node root;
        EXPECT_OK(pager->allocate(root.page));
        root.scratch = scratch.data();
        root.header.read(root.page);
        root.header.is_external = true;
        root.initialize();
        pager->release(std::move(root).take());
    }

    auto acquire_node(Id pid, bool writable = false)
    {
        Node node;
        EXPECT_OK(pager->acquire(pid, node.page));
        node.scratch = scratch.data();
        node.meta = &meta(node.header.is_external);
        if (writable) {
            pager->upgrade(node.page);
        }
        node.header.read(node.page);
        node.initialize();
        return node;
    }

    auto release_node(Node node) const
    {
        pager->release(std::move(node).take());
    }

    NodeMetaManager meta {PAGE_SIZE};
    std::unique_ptr<PointerMap> pointers;
    std::unique_ptr<Freelist> freelist;
    std::unique_ptr<OverflowList> overflow;
    std::unique_ptr<PayloadManager> payloads;
};

TEST_F(ComponentTests, EmplacesCell)
{
    auto root = acquire_node(Id::root(), true);
    auto *start = root.page.data() + 0x100;
    const auto *end = emplace_cell(start, 1, 1, "a", "1");
    const auto cell_size = static_cast<std::size_t>(end - start);
    const auto cell = read_cell_at(root, 0x100);
    ASSERT_EQ(cell.size, cell_size);
    ASSERT_EQ(cell.key_size, 1);
    ASSERT_EQ(cell.local_size, 2);
    ASSERT_EQ(cell.has_remote, false);
    const Slice key {cell.key, 1};
    const Slice val {cell.key + 1, 1};
    ASSERT_EQ(key, "a");
    ASSERT_EQ(val, "1");
    release_node(std::move(root));
}

TEST_F(ComponentTests, EmplacesCellWithOverflowValue)
{
    auto root = acquire_node(Id::root(), true);
    const auto value = random.Generate(PAGE_SIZE).to_string();
    const auto local = Slice {value}.truncate(root.meta->min_local - 1);
    auto *start = root.page.data() + 0x100;
    const auto *end = emplace_cell(start, 1, value.size(), "a", local, Id {123});
    const auto cell_size = static_cast<std::size_t>(end - start);
    const auto cell = read_cell_at(root, 0x100);
    ASSERT_EQ(cell.size, cell_size);
    ASSERT_EQ(cell.key_size, 1);
    ASSERT_EQ(cell.local_size, root.meta->min_local);
    ASSERT_EQ(cell.has_remote, true);
    const Slice key {cell.key, 1};
    const Slice val {cell.key + 1, cell.local_size - cell.key_size};
    ASSERT_EQ(key, "a");
    ASSERT_EQ(val, local);
    ASSERT_EQ(read_overflow_id(cell), Id {123});
    release_node(std::move(root));
}

TEST_F(ComponentTests, EmplacesCellWithOverflowKey)
{
    auto root = acquire_node(Id::root(), true);
    const auto long_key = random.Generate(PAGE_SIZE).to_string();
    const auto local = Slice {long_key}.truncate(root.meta->max_local);
    auto *start = root.page.data() + 0x100;
    auto *end = emplace_cell(start, long_key.size(), 1, local, "", Id {123});
    const auto cell_size = static_cast<std::size_t>(end - start);
    const auto cell = read_cell_at(root, 0x100);
    ASSERT_EQ(cell.size, cell_size);
    ASSERT_EQ(cell.key_size, long_key.size());
    ASSERT_EQ(cell.local_size, root.meta->max_local);
    ASSERT_EQ(cell.has_remote, true);
    const Slice key {cell.key, cell.local_size};
    ASSERT_EQ(key, local);
    ASSERT_EQ(read_overflow_id(cell), Id {123});
    release_node(std::move(root));
}

TEST_F(ComponentTests, CollectsPayload)
{
    auto root = acquire_node(Id::root(), true);
    ASSERT_OK(payloads->emplace(collect_scratch.data(), root, "hello", "world", 0));
    const auto cell = read_cell(root, 0);
    Slice slice;
    ASSERT_OK(payloads->collect_key(scratch, cell, slice));
    ASSERT_EQ(slice, "hello");
    ASSERT_OK(payloads->collect_value(scratch, cell, slice));
    ASSERT_EQ(slice, "world");
    root.TEST_validate();
    release_node(std::move(root));
}

TEST_F(ComponentTests, CollectsPayloadWithOverflowValue)
{
    auto root = acquire_node(Id::root(), true);
    const auto value = random.Generate(PAGE_SIZE * 100).to_string();
    ASSERT_OK(payloads->emplace(collect_scratch.data(), root, "hello", value, 0));
    const auto cell = read_cell(root, 0);
    Slice slice;
    ASSERT_OK(payloads->collect_key(scratch, cell, slice));
    ASSERT_EQ(slice, "hello");
    ASSERT_OK(payloads->collect_value(scratch, cell, slice));
    ASSERT_EQ(slice, value);
    root.TEST_validate();
    release_node(std::move(root));
}

TEST_F(ComponentTests, CollectsPayloadWithOverflowKey)
{
    auto root = acquire_node(Id::root(), true);
    const auto key = random.Generate(PAGE_SIZE * 100).to_string();
    ASSERT_OK(payloads->emplace(collect_scratch.data(), root, key, "world", 0));
    const auto cell = read_cell(root, 0);
    Slice slice;
    ASSERT_OK(payloads->collect_key(scratch, cell, slice));
    ASSERT_EQ(slice, key);
    ASSERT_OK(payloads->collect_value(scratch, cell, slice));
    ASSERT_EQ(slice, "world");
    root.TEST_validate();
    release_node(std::move(root));
}

TEST_F(ComponentTests, CollectsPayloadWithOverflowKeyValue)
{
    auto root = acquire_node(Id::root(), true);
    const auto key = random.Generate(PAGE_SIZE * 100).to_string();
    const auto value = random.Generate(PAGE_SIZE * 100).to_string();
    ASSERT_OK(payloads->emplace(collect_scratch.data(), root, key, value, 0));
    const auto cell = read_cell(root, 0);
    Slice slice;
    ASSERT_OK(payloads->collect_key(scratch, cell, slice));
    ASSERT_EQ(slice, key);
    ASSERT_OK(payloads->collect_value(scratch, cell, slice));
    ASSERT_EQ(slice, value);
    root.TEST_validate();
    release_node(std::move(root));
}

TEST_F(ComponentTests, CollectsMultiple)
{
    std::vector<std::string> data;
    auto root = acquire_node(Id::root(), true);
    for (std::size_t i {}; i < 3; ++i) {
        const auto key = random.Generate(PAGE_SIZE * 10);
        const auto value = random.Generate(PAGE_SIZE * 10);
        ASSERT_OK(payloads->emplace(collect_scratch.data(), root, key, value, i));
        data.emplace_back(key.to_string());
        data.emplace_back(value.to_string());
    }
    for (std::size_t i {}; i < 3; ++i) {
        const auto &key = data[2 * i];
        const auto &value = data[2 * i + 1];
        const auto cell = read_cell(root, i);
        Slice slice;
        ASSERT_OK(payloads->collect_key(scratch, cell, slice));
        ASSERT_EQ(slice, key);
        ASSERT_OK(payloads->collect_value(scratch, cell, slice));
        ASSERT_EQ(slice, value);
    }
    root.TEST_validate();
    release_node(std::move(root));
}

TEST_F(ComponentTests, PromotesCell)
{
    auto root = acquire_node(Id::root(), true);
    const auto key = random.Generate(PAGE_SIZE * 100).to_string();
    const auto value = random.Generate(PAGE_SIZE * 100).to_string();
    std::string emplace_scratch(PAGE_SIZE, '\0');
    ASSERT_OK(payloads->emplace(emplace_scratch.data() + 10, root, key, value, 0));
    auto cell = read_cell(root, 0);
    ASSERT_OK(payloads->promote(emplace_scratch.data() + 10, cell, Id::root()));
    // Needs to consult overflow pages for the key.
    Slice slice;
    ASSERT_OK(payloads->collect_key(collect_scratch, cell, slice));
    ASSERT_EQ(slice, key);
    root.TEST_validate();
    release_node(std::move(root));
}

TEST_F(ComponentTests, PromotedCellSize)
{
    auto root = acquire_node(Id::root(), true);
    const auto key = random.Generate(PAGE_SIZE * 100).to_string();
    const auto value = random.Generate(PAGE_SIZE * 100).to_string();
    std::string emplace_scratch(PAGE_SIZE, '\0');
    ASSERT_OK(payloads->emplace(nullptr, root, key, value, 0));
    auto cell = read_cell(root, 0);
    ASSERT_OK(payloads->promote(emplace_scratch.data() + 20, cell, Id::root()));
    erase_cell(root, 0);

    root.header.is_external = false;
    root.meta = &meta(false);
    write_cell(root, 0, cell);
    cell = read_cell(root, 0);

    // Needs to consult overflow pages for the key.
    Slice slice;
    ASSERT_OK(payloads->collect_key(collect_scratch, cell, slice));
    ASSERT_EQ(slice, key);
    root.TEST_validate();
    release_node(std::move(root));
}

static auto run_promotion_test(ComponentTests &test, std::size_t key_size, std::size_t value_size)
{
    auto root = test.acquire_node(Id::root(), true);
    const auto key = test.random.Generate(key_size).to_string();
    const auto value = test.random.Generate(value_size).to_string();
    std::string emplace_scratch(test.PAGE_SIZE, '\0');
    ASSERT_OK(test.payloads->emplace(emplace_scratch.data() + 10, root, key, value, 0));
    auto external_cell = read_cell(root, 0);
    ASSERT_EQ(external_cell.size, varint_length(key.size()) + varint_length(value.size()) + external_cell.local_size + external_cell.has_remote * 8);
    auto internal_cell = external_cell;
    ASSERT_OK(test.payloads->promote(emplace_scratch.data() + 10, internal_cell, Id::root()));
    ASSERT_EQ(internal_cell.size, sizeof(Id) + varint_length(key.size()) + internal_cell.local_size + internal_cell.has_remote * 8);
    test.release_node(std::move(root));
}

TEST_F(ComponentTests, CellIsPromoted)
{
    run_promotion_test(*this, 10, 10);
}

TEST_F(ComponentTests, PromotionCopiesOverflowKey)
{
    run_promotion_test(*this, PAGE_SIZE, 10);
    PointerMap::Entry old_head;
    ASSERT_OK(pointers->read_entry(Id {3}, old_head));
    ASSERT_EQ(old_head.type, PointerMap::OverflowHead);
    ASSERT_EQ(old_head.back_ptr, Id::root());

    // Copy of the overflow key.
    PointerMap::Entry new_head;
    ASSERT_OK(pointers->read_entry(Id {4}, new_head));
    ASSERT_EQ(new_head.type, PointerMap::OverflowHead);
    ASSERT_EQ(new_head.back_ptr, Id::root());
}

TEST_F(ComponentTests, PromotionIgnoresOverflowValue)
{
    run_promotion_test(*this, 10, PAGE_SIZE);
    PointerMap::Entry old_head;
    ASSERT_OK(pointers->read_entry(Id {3}, old_head));
    ASSERT_EQ(old_head.type, PointerMap::OverflowHead);
    ASSERT_EQ(old_head.back_ptr, Id::root());

    // No overflow key, so the chain didn't need to be copied.
    PointerMap::Entry nothing;
    ASSERT_OK(pointers->read_entry(Id {4}, nothing));
    ASSERT_EQ(nothing.back_ptr, Id::null());
}

TEST_F(ComponentTests, PromotionCopiesOverflowKeyButIgnoresOverflowValue)
{
    run_promotion_test(*this, PAGE_SIZE, PAGE_SIZE);
    PointerMap::Entry old_head;
    ASSERT_OK(pointers->read_entry(Id {3}, old_head));
    ASSERT_EQ(old_head.type, PointerMap::OverflowHead);
    ASSERT_EQ(old_head.back_ptr, Id::root());

    // 1 overflow page needed for the key, and 1 for the value.
    PointerMap::Entry new_head;
    ASSERT_OK(pointers->read_entry(Id {5}, new_head));
    ASSERT_EQ(new_head.type, PointerMap::OverflowHead);
    ASSERT_EQ(new_head.back_ptr, Id::root());
}

TEST_F(ComponentTests, NodeIterator)
{
    for (std::size_t i {}; i < 4; ++i) {
        auto root = acquire_node(Id::root(), true);
        const auto key = tools::integral_key<2>((i + 1) * 2);
        ASSERT_OK(payloads->emplace(nullptr, root, key, "", i));
        release_node(std::move(root));
    }
    auto root = acquire_node(Id::root(), true);
    std::string lhs_key, rhs_key;
    NodeIterator itr {root, {
                                overflow.get(),
                                &lhs_key,
                                &rhs_key,
                            }};
    bool exact;
    for (std::size_t i : {1, 6, 3, 2, 8, 4, 5, 7}) {
        ASSERT_OK(itr.seek(tools::integral_key<2>(i), &exact));
        ASSERT_EQ(exact, i % 2 == 0);
    }
    ASSERT_OK(itr.seek(tools::integral_key<2>(10), &exact));
    ASSERT_FALSE(exact);
    release_node(std::move(root));
}

TEST_F(ComponentTests, NodeIteratorHandlesOverflowKeys)
{
    std::vector<std::string> keys;
    for (std::size_t i {}; i < 3; ++i) {
        auto root = acquire_node(Id::root(), true);
        auto key = random.Generate(PAGE_SIZE).to_string();
        const auto value = random.Generate(PAGE_SIZE).to_string();
        key[0] = static_cast<char>(i);
        ASSERT_OK(payloads->emplace(nullptr, root, key, value, i));
        ASSERT_FALSE(root.overflow.has_value());
        release_node(std::move(root));
        keys.emplace_back(key);
    }
    auto root = acquire_node(Id::root(), true);
    std::string lhs_key, rhs_key;
    NodeIterator itr {root, {
                                overflow.get(),
                                &lhs_key,
                                &rhs_key,
                            }};
    std::size_t i {};
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
    static constexpr auto PAGE_SIZE = MINIMUM_PAGE_SIZE;

    NodeTests()
        : collect_scratch(PAGE_SIZE, '\x00')
    {
    }

    auto SetUp() -> void override
    {
        tree = std::make_unique<BPlusTree>(*pager);

        // Root page setup.
        Node root;
        BPlusTreeInternal internal {*tree};
        ASSERT_OK(internal.allocate_root(root));
        internal.release(std::move(root));
        ASSERT_TRUE(pager->flush({}).is_ok());
    }

    auto TearDown() -> void override
    {
        validate();
    }

    auto validate() const -> void
    {
        tree->TEST_check_nodes();
        tree->TEST_check_links();
        tree->TEST_check_order();
    }

    std::string collect_scratch;
    std::unique_ptr<BPlusTree> tree;
};

class ExternalRootSplitTests : public NodeTests
{
public:
    ExternalRootSplitTests() = default;

    auto SetUp() -> void override
    {
        // <cell_pointer> + <value_size> + <key_size> + <key> + <value> + <overflow_id>
        static constexpr std::size_t min_cell_size {2 + 1 + 1 + 1 + 0 + 0};

        NodeTests::SetUp();
        BPlusTreeInternal internal {*tree};
        bool done {};
        Node root;

        ASSERT_OK(internal.acquire(root, Id::root()));
        auto space = usable_space(root);
        internal.release(std::move(root));

        for (std::size_t i {}; space >= min_cell_size; ++i) {
            ASSERT_LT(i, 0x100);
            keys.emplace_back(1, static_cast<char>(i));
            space -= min_cell_size;
        }

        // Create the worst case for splitting.
        for (const auto &key : keys) {
            ASSERT_OK(tree->insert(key, ""));
        }
        ASSERT_OK(internal.acquire(root, Id::root()));
        ASSERT_TRUE(root.header.is_external);
        ASSERT_LT(usable_space(root), min_cell_size);
        internal.release(std::move(root));
    }

    auto TearDown() -> void override
    {
        Node root;
        BPlusTreeInternal internal {*tree};
        ASSERT_OK(internal.acquire(root, Id::root()));
        ASSERT_FALSE(root.header.is_external);
        internal.release(std::move(root));

        for (const auto &key : keys) {
            assert_contains({key, ""});
        }

        NodeTests::TearDown();
    }

    virtual auto large_payload(const std::string &base_key) -> Record
    {
        std::string key(PAGE_SIZE * 2, '\0');
        mem_copy(key, base_key);
        return {key, key};
    }

    auto assert_contains(const Record &record) -> void
    {
        SearchResult slot;
        ASSERT_OK(tree->search(record.key, slot));
        ASSERT_TRUE(slot.exact);

        Slice key, value;
        const auto cell = read_cell(slot.node, slot.index);
        ASSERT_OK(tree->collect_key(collect_scratch, cell, key));
        ASSERT_EQ(key, record.key);
        ASSERT_OK(tree->collect_value(collect_scratch, cell, value));
        ASSERT_EQ(value, record.value);

        BPlusTreeInternal internal {*tree};
        internal.release(std::move(slot.node));
    }

    std::vector<std::string> keys;
};

TEST_F(ExternalRootSplitTests, SplitOnLeftmost)
{
    const auto record = large_payload(keys.front());
    ASSERT_OK(tree->insert(record.key, record.value));
    assert_contains(record);
}

TEST_F(ExternalRootSplitTests, SplitLeftOfMiddle)
{
    const auto record = large_payload(keys[keys.size() / 2 - 10]);
    ASSERT_OK(tree->insert(record.key, record.value));
    assert_contains(record);
}

TEST_F(ExternalRootSplitTests, SplitOnRightmost)
{
    const auto record = large_payload(keys.back());
    ASSERT_OK(tree->insert(record.key, record.value));
    assert_contains(record);
}

TEST_F(ExternalRootSplitTests, SplitRightOfMiddle)
{
    const auto record = large_payload(keys[keys.size() / 2 + 10]);
    ASSERT_OK(tree->insert(record.key, record.value));
    assert_contains(record);
}

class InternalRootSplitTests : public ExternalRootSplitTests
{
public:
    auto SetUp() -> void override
    {
        ExternalRootSplitTests::SetUp();

        // Cause the root to overflow and become an internal node.
        const auto record = large_payload(std::string(1, '\0'));
        ASSERT_OK(tree->insert(record.key, record.value));

        // The key needs to be 2 bytes, there aren't enough possible 1-byte keys to make the root split again.
        // <cell_pointer> + <child_id> + <key_size> + <key>
        const auto min_cell_size = 2 + 8 + 1 + 2;
        ASSERT_EQ(varint_length(2), 1);

        BPlusTreeInternal internal {*tree};
        bool done {};
        Node root;

        for (auto i = key_range_start;; i += 2) {
            ASSERT_OK(internal.acquire(root, Id::root()));
            base_usable_space = usable_space(root);
            internal.release(std::move(root));
            if (base_usable_space < min_cell_size) {
                break;
            }
            ASSERT_LE(i, key_range_max);
            keys_2.emplace_back(base_key(i));
            ASSERT_OK(tree->insert(keys_2.back(), ""));
        }
    }

    auto TearDown() -> void override
    {
        ASSERT_TRUE(is_finished());
        for (const auto &key : keys_2) {
            assert_contains({key, ""});
        }
        ExternalRootSplitTests::TearDown();
    }

    static auto base_key(std::size_t index) -> std::string
    {
        std::string key(2, '\0');
        key[0] = static_cast<char>(index >> 8);
        key[1] = static_cast<char>(index);
        return key;
    }

    [[nodiscard]] auto is_finished() -> bool
    {
        Node root;
        BPlusTreeInternal internal {*tree};
        EXPECT_OK(internal.acquire(root, Id::root()));
        const auto space = usable_space(root);
        internal.release(std::move(root));
        return space != base_usable_space;
    }

    std::vector<std::string> keys_2;
    std::size_t base_usable_space {};
    std::size_t key_range_start {4'000};
    std::size_t key_range_max {6'000};
};

TEST_F(InternalRootSplitTests, Split_1)
{
    for (std::size_t i {}; !is_finished(); ++i) {
        const auto record = large_payload(keys_2[i]);
        ASSERT_OK(tree->insert(record.key, record.value));
    }
}

TEST_F(InternalRootSplitTests, Split_2)
{
    for (auto itr = --end(keys_2); !is_finished(); --itr) {
        const auto record = large_payload(*itr);
        ASSERT_OK(tree->insert(record.key, record.value));
    }
}

template<class Test>
static auto test_allocate_node(Test &test, bool is_external) -> Node
{
    Node node;
    BPlusTreeInternal internal {*test.tree};
    EXPECT_OK(internal.allocate(node, is_external));
    return node;
}

template<class Test>
static auto test_acquire_node(Test &test, Id pid) -> Node
{
    Node node;
    BPlusTreeInternal internal {*test.tree};
    EXPECT_OK(internal.acquire(node, pid));
    return node;
}

template<class Test>
static auto test_release_node(Test &test, Node node) -> void
{
    BPlusTreeInternal internal {*test.tree};
    internal.release(std::move(node));
}

template<class Test>
static auto test_is_root_external(Test &test) -> bool
{
    auto root = test_acquire_node(test, Id::root());
    const auto answer = root.header.is_external;
    test_release_node(test, std::move(root));
    return answer;
}

struct BPlusTreeTestParameters {
    std::size_t page_size {};
    std::size_t extra {};
};

class BPlusTreeTests
    : public TestWithPager,
      public testing::TestWithParam<BPlusTreeTestParameters>
{
public:
    BPlusTreeTests()
        : param {GetParam()},
          collect_scratch(param.page_size, '\x00')
    {
    }

    auto SetUp() -> void override
    {
        tree = std::make_unique<BPlusTree>(*pager);

        // Root page setup.
        Node root;
        BPlusTreeInternal internal {*tree};
        ASSERT_OK(internal.allocate_root(root));
        internal.release(std::move(root));
        ASSERT_TRUE(pager->flush({}).is_ok());
    }

    auto TearDown() -> void override
    {
        validate();
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

    auto acquire_node(Id pid)
    {
        return test_acquire_node(*this, pid);
    }

    auto release_node(Node node) const
    {
        test_release_node(*this, std::move(node));
    }

    auto is_root_external()
    {
        return test_is_root_external(*this);
    }

    auto validate() const -> void
    {
        tree->TEST_check_nodes();
        tree->TEST_check_order();
        tree->TEST_check_links();
    }

    BPlusTreeTestParameters param;
    NodeMetaManager meta {GetParam().page_size};
    std::string collect_scratch;
    std::unique_ptr<BPlusTree> tree;
};

TEST_P(BPlusTreeTests, ConstructsAndDestructs)
{
    validate();
}

TEST_P(BPlusTreeTests, InsertsRecords)
{
    bool exists;
    ASSERT_OK(tree->insert("a", make_value('1'), &exists));
    ASSERT_FALSE(exists);
    ASSERT_OK(tree->insert("b", make_value('2'), &exists));
    ASSERT_FALSE(exists);
    ASSERT_OK(tree->insert("c", make_value('3'), &exists));
    ASSERT_FALSE(exists);
    validate();
}

TEST_P(BPlusTreeTests, ErasesRecords)
{
    (void)tree->insert("a", make_value('1'));
    (void)tree->insert("b", make_value('2'));
    (void)tree->insert("c", make_value('3'));
    ASSERT_OK(tree->erase("a"));
    ASSERT_OK(tree->erase("b"));
    ASSERT_OK(tree->erase("c"));
    validate();
}

TEST_P(BPlusTreeTests, FindsRecords)
{
    const auto keys = "abc";
    const auto vals = "123";
    (void)tree->insert(std::string(1, keys[0]), make_value(vals[0]));
    (void)tree->insert(std::string(1, keys[1]), make_value(vals[1]));
    (void)tree->insert(std::string(1, keys[2]), make_value(vals[2]));

    for (std::size_t i {}; i < 3; ++i) {
        SearchResult slot;
        ASSERT_OK(tree->search(std::string(1, keys[i]), slot));
        ASSERT_EQ(slot.index, i);
        const auto cell = read_cell(slot.node, slot.index);
        ASSERT_EQ(cell.key[0], keys[i]);
        ASSERT_EQ(cell.key[cell.key_size], vals[i]);
    }
}

TEST_P(BPlusTreeTests, CannotFindNonexistentRecords)
{
    SearchResult slot;
    ASSERT_OK(tree->search("a", slot));
    ASSERT_EQ(slot.node.header.cell_count, 0);
    ASSERT_FALSE(slot.exact);
}

TEST_P(BPlusTreeTests, CannotEraseNonexistentRecords)
{
    ASSERT_TRUE(tree->erase("a").is_not_found());
}

TEST_P(BPlusTreeTests, WritesOverflowKeys)
{
    ASSERT_OK(tree->insert(make_value('1', true), "a"));
    ASSERT_OK(tree->insert(make_value('2', true), "b"));
    ASSERT_OK(tree->insert(make_value('3', true), "c"));
    validate();
}

TEST_P(BPlusTreeTests, WritesOverflowValues)
{
    ASSERT_OK(tree->insert("a", make_value('1', true)));
    ASSERT_OK(tree->insert("b", make_value('2', true)));
    ASSERT_OK(tree->insert("c", make_value('3', true)));
    validate();
}

TEST_P(BPlusTreeTests, ErasesOverflowKeys)
{
    (void)tree->insert(make_value('1', true), "a");
    (void)tree->insert(make_value('2', true), "b");
    (void)tree->insert(make_value('3', true), "c");
    ASSERT_OK(tree->erase(make_value('1', true)));
    ASSERT_OK(tree->erase(make_value('2', true)));
    ASSERT_OK(tree->erase(make_value('3', true)));
}

TEST_P(BPlusTreeTests, ErasesOverflowValues)
{
    (void)tree->insert("a", make_value('1', true));
    (void)tree->insert("b", make_value('2', true));
    (void)tree->insert("c", make_value('3', true));
    ASSERT_OK(tree->erase("a"));
    ASSERT_OK(tree->erase("b"));
    ASSERT_OK(tree->erase("c"));
}

TEST_P(BPlusTreeTests, ReadsValuesFromOverflowChains)
{
    const auto keys = "abc";
    const auto vals = "123";
    std::string values[3];
    values[0] = random.Generate(param.page_size).to_string();
    values[1] = random.Generate(param.page_size).to_string();
    values[2] = random.Generate(param.page_size).to_string();
    (void)tree->insert(std::string(1, keys[0]), values[0]);
    (void)tree->insert(std::string(1, keys[1]), values[1]);
    (void)tree->insert(std::string(1, keys[2]), values[2]);

    for (std::size_t i {}; i < 3; ++i) {
        SearchResult slot;
        ASSERT_OK(tree->search(std::string(1, keys[i]), slot));
        const auto cell = read_cell(slot.node, slot.index);
        const auto pid = read_overflow_id(cell);
        Slice value;
        ASSERT_OK(tree->collect_value(collect_scratch, cell, value));
        pager->release(std::move(slot.node.page));
        ASSERT_EQ(value, values[i]);
    }
}

TEST_P(BPlusTreeTests, ResolvesFirstOverflowOnRightmostPosition)
{
    for (std::size_t i {}; is_root_external(); ++i) {
        ASSERT_OK(tree->insert(tools::integral_key(i), make_value('v')));
        validate();
    }
}

TEST_P(BPlusTreeTests, ResolvesFirstOverflowOnLeftmostPosition)
{
    for (std::size_t i {}; is_root_external(); ++i) {
        ASSERT_LE(i, 100);
        ASSERT_OK(tree->insert(tools::integral_key(100 - i), make_value('v')));
    }
    validate();
}

TEST_P(BPlusTreeTests, ResolvesFirstOverflowOnMiddlePosition)
{
    for (std::size_t i {}; is_root_external(); ++i) {
        ASSERT_LE(i, 100);
        ASSERT_OK(tree->insert(make_long_key(i & 1 ? 100 - i : i), make_value('v')));
    }
    validate();
}

TEST_P(BPlusTreeTests, ResolvesMultipleOverflowsOnLeftmostPosition)
{
    for (std::size_t i {}; i < 100; ++i) {
        ASSERT_OK(tree->insert(make_long_key(99 - i), make_value('v')));
        if (i % 10 == 9) {
            validate();
        }
    }
}

TEST_P(BPlusTreeTests, ResolvesMultipleOverflowsOnRightmostPosition)
{
    for (std::size_t i {}; i < 100; ++i) {
        ASSERT_OK(tree->insert(make_long_key(i), make_value('v')));
        if (i % 10 == 9) {
            validate();
        }
    }
}

TEST_P(BPlusTreeTests, ResolvesMultipleOverflowsOnMiddlePosition)
{
    for (std::size_t i {}, j {99}; i < j; ++i, --j) {
        ASSERT_OK(tree->insert(make_long_key(i), make_value('v')));
        ASSERT_OK(tree->insert(make_long_key(j), make_value('v')));
        if (i % 10 == 9) {
            validate();
        }
    }
}

TEST_P(BPlusTreeTests, ResolvesFirstUnderflowOnRightmostPosition)
{
    long i {};
    for (; is_root_external(); ++i) {
        (void)tree->insert(make_long_key(i), make_value('v'));
    }

    while (i--) {
        ASSERT_OK(tree->erase(make_long_key(i)));
        validate();
    }
}

TEST_P(BPlusTreeTests, ResolvesFirstUnderflowOnLeftmostPosition)
{
    std::size_t i {};
    for (; is_root_external(); ++i) {
        (void)tree->insert(make_long_key(i), make_value('v'));
    }
    for (std::size_t j {}; j < i; ++j) {
        ASSERT_OK(tree->erase(make_long_key(j)));
        validate();
    }
}

TEST_P(BPlusTreeTests, ResolvesFirstUnderflowOnMiddlePosition)
{
    std::size_t i {};
    for (; is_root_external(); ++i) {
        (void)tree->insert(make_long_key(i), make_value('v'));
    }
    for (std::size_t j {1}; j < i / 2 - 1; ++j) {
        ASSERT_OK(tree->erase(make_long_key(i / 2 - j + 1)));
        ASSERT_OK(tree->erase(make_long_key(i / 2 + j)));
        validate();
    }
}

static auto add_initial_records(BPlusTreeTests &test, bool has_overflow = false)
{
    for (std::size_t i {}; i < initial_record_count; ++i) {
        (void)test.tree->insert(test.make_long_key(i), test.make_value('v', has_overflow));
    }
}

TEST_P(BPlusTreeTests, ResolvesMultipleUnderflowsOnRightmostPosition)
{
    add_initial_records(*this);
    for (std::size_t i {}; i < initial_record_count; ++i) {
        ASSERT_OK(tree->erase(make_long_key(initial_record_count - i - 1)));
    }
    validate();
}

TEST_P(BPlusTreeTests, ResolvesMultipleUnderflowsOnLeftmostPosition)
{
    add_initial_records(*this);
    for (std::size_t i {}; i < initial_record_count; ++i) {
        ASSERT_OK(tree->erase(make_long_key(i)));
    }
    validate();
}

TEST_P(BPlusTreeTests, ResolvesMultipleUnderflowsOnMiddlePosition)
{
    add_initial_records(*this);
    for (std::size_t i {}, j {initial_record_count - 1}; i < j; ++i, --j) {
        ASSERT_OK(tree->erase(make_long_key(i)));
        ASSERT_OK(tree->erase(make_long_key(j)));
    }
    validate();
}

TEST_P(BPlusTreeTests, ResolvesOverflowsFromOverwrite)
{
    add_initial_records(*this);
    // Replace the small values with very large ones.
    add_initial_records(*this, true);
    validate();
}

TEST_P(BPlusTreeTests, SplitWithShortAndLongKeys)
{
    for (unsigned i {}; i < initial_record_count; ++i) {
        char key[3] {};
        put_u16(key, initial_record_count - i - 1);
        ASSERT_OK(tree->insert({key, 2}, "v"));
    }
    for (unsigned i {}; i < initial_record_count; ++i) {
        const auto key = random.Generate(GetParam().page_size);
        ASSERT_OK(tree->insert(key, "v"));
        validate();
    }
}

TEST_P(BPlusTreeTests, AllowsNonInsertOperationsOnEmptyKeys)
{
    SearchResult result;
    ASSERT_OK(tree->insert("key", "value"));
    ASSERT_OK(tree->search("", result));
    ASSERT_FALSE(result.exact);
    ASSERT_TRUE(result.node.page.id().is_root());
    ASSERT_EQ(result.index, 0);
    ASSERT_TRUE(tree->erase("").is_not_found());
}

#if not NDEBUG
TEST_P(BPlusTreeTests, InsertEmptyKeyDeathTest)
{
    ASSERT_DEATH((void)tree->insert("", "value"), EXPECTATION_MATCHER);
}
#endif // NDEBUG

INSTANTIATE_TEST_SUITE_P(
    BPlusTreeTests,
    BPlusTreeTests,
    ::testing::Values(
        BPlusTreeTestParameters {MINIMUM_PAGE_SIZE},
        BPlusTreeTestParameters {MINIMUM_PAGE_SIZE * 2},
        BPlusTreeTestParameters {MAXIMUM_PAGE_SIZE / 2},
        BPlusTreeTestParameters {MAXIMUM_PAGE_SIZE}));

class BPlusTreeSpecialCases : public BPlusTreeTests
{
public:
    auto SetUp() -> void override
    {
        BPlusTreeTests::SetUp();
        BPlusTreeInternal internal {*tree};
        payloads = internal.payloads();
    }

    PayloadManager *payloads {};
};

TEST_P(BPlusTreeSpecialCases, NonRootCanFitAtLeast4Cells)
{
    // Note that the maximal cell size is the same in both node types.
    auto node = test_allocate_node(*this, true);

    const auto key = random.Generate(meta(true).max_local);
    ASSERT_OK(payloads->emplace(scratch.data(), node, key, "x", 0));
    ASSERT_OK(payloads->emplace(scratch.data(), node, key, "x", 1));
    ASSERT_OK(payloads->emplace(scratch.data(), node, key, "x", 2));
    ASSERT_OK(payloads->emplace(scratch.data(), node, key, "x", 3));
    ASSERT_FALSE(node.overflow.has_value()) << "node cannot fit 4 maximally-sized cells";
    ASSERT_GE(usable_space(node), 7 * 4) << "node cannot account for larger value size varints";
    ASSERT_LT(usable_space(node), read_cell(node, 0).size + 2) << "node can fit another maximally-sized cell";

    release_node(std::move(node));
}

TEST_P(BPlusTreeSpecialCases, FixRootSplitsIntoParentWhenFileHeaderGetsInTheWay)
{

}

INSTANTIATE_TEST_SUITE_P(
    BPlusTreeSpecialCases,
    BPlusTreeSpecialCases,
    ::testing::Values(
        BPlusTreeTestParameters {MINIMUM_PAGE_SIZE},
        BPlusTreeTestParameters {MINIMUM_PAGE_SIZE * 2},
        BPlusTreeTestParameters {MAXIMUM_PAGE_SIZE / 2},
        BPlusTreeTestParameters {MAXIMUM_PAGE_SIZE}));


class BPlusTreeSanityChecks : public BPlusTreeTests
{
public:
    auto random_chunk(bool overflow, bool nonzero = true)
    {
        return random.Generate(random.Next<std::size_t>(nonzero, param.page_size * overflow + 12));
    }

    auto random_write() -> Record
    {
        const auto key = random_chunk(overflow_keys);
        const auto val = random_chunk(overflow_values, false);
        EXPECT_OK(tree->insert(key, val));
        return {key.to_string(), val.to_string()};
    }

    bool overflow_keys = GetParam().extra & 0b10;
    bool overflow_values = GetParam().extra & 0b01;
};

TEST_P(BPlusTreeSanityChecks, Insert)
{
    for (std::size_t i {}; i < initial_record_count * 10; ++i) {
        random_write();
    }
    validate();
}

TEST_P(BPlusTreeSanityChecks, Search)
{
    std::unordered_map<std::string, std::string> records;
    for (std::size_t i {}; i < initial_record_count * 10; ++i) {
        const auto [k, v] = random_write();
        records[k] = v;
    }
    validate();

    for (const auto &[key, value] : records) {
        SearchResult slot;
        ASSERT_OK(tree->search(key, slot));
        ASSERT_TRUE(slot.exact);
        const auto cell = read_cell(slot.node, slot.index);
        Slice slice;
        ASSERT_OK(tree->collect_key(collect_scratch, cell, slice));
        ASSERT_EQ(slice, key);
        ASSERT_OK(tree->collect_value(collect_scratch, cell, slice));
        ASSERT_EQ(slice, value);
        release_node(std::move(slot.node));
    }
}

TEST_P(BPlusTreeSanityChecks, Erase)
{
    std::unordered_map<std::string, std::string> records;
    for (std::size_t iteration {}; iteration < 3; ++iteration) {
        for (std::size_t i {}; i < initial_record_count * 10; ++i) {
            const auto [k, v] = random_write();
            records[k] = v;
        }

        std::size_t i {};
        for (const auto &[key, value] : records) {
            ASSERT_OK(tree->erase(key));
        }
        validate();
        records.clear();
    }
}

// "extra" parameter bits:
//     0b01: Use overflowing values
//     0b10: Use overflowing keys
INSTANTIATE_TEST_SUITE_P(
    BPlusTreeSanityChecks,
    BPlusTreeSanityChecks,
    ::testing::Values(
        BPlusTreeTestParameters {MINIMUM_PAGE_SIZE, 0b00},
        BPlusTreeTestParameters {MINIMUM_PAGE_SIZE, 0b01},
        BPlusTreeTestParameters {MINIMUM_PAGE_SIZE, 0b10},
        BPlusTreeTestParameters {MINIMUM_PAGE_SIZE, 0b11},
        BPlusTreeTestParameters {MAXIMUM_PAGE_SIZE, 0b00},
        BPlusTreeTestParameters {MAXIMUM_PAGE_SIZE, 0b01},
        BPlusTreeTestParameters {MAXIMUM_PAGE_SIZE, 0b10},
        BPlusTreeTestParameters {MAXIMUM_PAGE_SIZE, 0b11}));

class CursorTests : public BPlusTreeTests
{
protected:
    auto SetUp() -> void override
    {
        BPlusTreeTests::SetUp();
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
    std::size_t i {};
    while (cursor->is_valid()) {
        ASSERT_TRUE(cursor->is_valid());
        ASSERT_EQ(cursor->key(), make_long_key(i++));
        ASSERT_EQ(cursor->value(), make_value('v'));
        cursor->next();
    }
    ASSERT_EQ(i, initial_record_count);
}

TEST_P(CursorTests, SeeksForwardFromBoundary)
{
    std::unique_ptr<Cursor> cursor {CursorInternal::make_cursor(*tree)};
    cursor->seek(make_long_key(initial_record_count / 4));
    while (cursor->is_valid()) {
        cursor->next();
    }
}

TEST_P(CursorTests, SeeksForwardToBoundary)
{
    std::unique_ptr<Cursor> cursor {CursorInternal::make_cursor(*tree)};
    std::unique_ptr<Cursor> bounds {CursorInternal::make_cursor(*tree)};
    cursor->seek_first();
    bounds->seek(make_long_key(initial_record_count * 3 / 4));
    while (cursor->key() != bounds->key()) {
        ASSERT_TRUE(cursor->is_valid());
        cursor->next();
    }
}

TEST_P(CursorTests, SeeksForwardBetweenBoundaries)
{
    std::unique_ptr<Cursor> cursor {CursorInternal::make_cursor(*tree)};
    cursor->seek(make_long_key(initial_record_count / 4));
    std::unique_ptr<Cursor> bounds {CursorInternal::make_cursor(*tree)};
    bounds->seek(make_long_key(initial_record_count * 3 / 4));
    while (cursor->key() != bounds->key()) {
        ASSERT_TRUE(cursor->is_valid());
        cursor->next();
    }
}

TEST_P(CursorTests, SeeksBackward)
{
    std::unique_ptr<Cursor> cursor {CursorInternal::make_cursor(*tree)};
    cursor->seek_last();
    std::size_t i {};
    while (cursor->is_valid()) {
        ASSERT_EQ(cursor->key().to_string(), make_long_key(initial_record_count - 1 - i++));
        ASSERT_EQ(cursor->value().to_string(), make_value('v'));
        cursor->previous();
    }
    ASSERT_EQ(i, initial_record_count);
}

TEST_P(CursorTests, SeeksBackwardFromBoundary)
{
    std::unique_ptr<Cursor> cursor {CursorInternal::make_cursor(*tree)};
    const auto bounds = initial_record_count * 3 / 4;
    cursor->seek(make_long_key(bounds));
    for (std::size_t i {}; i <= bounds; ++i) {
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
    bounds->seek(make_long_key(initial_record_count / 4));
    while (cursor->key() != bounds->key()) {
        ASSERT_TRUE(cursor->is_valid());
        cursor->previous();
    }
}

TEST_P(CursorTests, SeeksBackwardBetweenBoundaries)
{
    std::unique_ptr<Cursor> cursor {CursorInternal::make_cursor(*tree)};
    std::unique_ptr<Cursor> bounds {CursorInternal::make_cursor(*tree)};
    cursor->seek(make_long_key(initial_record_count * 3 / 4));
    bounds->seek(make_long_key(initial_record_count / 4));
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
    for (std::size_t iteration {}; iteration < 100; ++iteration) {
        const auto i = random.Next(initial_record_count - 1);
        const auto key = make_long_key(i);
        cursor->seek(key);

        ASSERT_TRUE(cursor->is_valid());
        ASSERT_EQ(cursor->key(), key);

        for (std::size_t n {}; n < random.Next<std::size_t>(10); ++n) {
            cursor->next();

            if (const auto j = i + n + 1; j < initial_record_count) {
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
    for (std::size_t iteration {}; iteration < 100; ++iteration) {
        const auto i = random.Next<std::size_t>(initial_record_count - 1);
        const auto key = make_long_key(i);
        cursor->seek(key);

        ASSERT_TRUE(cursor->is_valid());
        ASSERT_EQ(cursor->key(), key);

        for (std::size_t n {}; n < random.Next<std::size_t>(10); ++n) {
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
    ASSERT_DEATH((void)cursor->key(), EXPECTATION_MATCHER);
    ASSERT_DEATH((void)cursor->value(), EXPECTATION_MATCHER);
    ASSERT_DEATH((void)cursor->next(), EXPECTATION_MATCHER);
    ASSERT_DEATH((void)cursor->previous(), EXPECTATION_MATCHER);
}
#endif // NDEBUG

INSTANTIATE_TEST_SUITE_P(
    CursorTests,
    CursorTests,
    ::testing::Values(
        BPlusTreeTestParameters {MINIMUM_PAGE_SIZE},
        BPlusTreeTestParameters {MINIMUM_PAGE_SIZE * 2},
        BPlusTreeTestParameters {MAXIMUM_PAGE_SIZE / 2},
        BPlusTreeTestParameters {MAXIMUM_PAGE_SIZE}));

class PointerMapTests : public BPlusTreeTests
{
public:
    [[nodiscard]] auto map_size() -> std::size_t
    {
        return (pager->page_size() - sizeof(Lsn)) / (sizeof(char) + sizeof(Id));
    }
};

TEST_P(PointerMapTests, FirstPointerMapIsPage2)
{
    PointerMap pointers {*pager};
    ASSERT_EQ(pointers.lookup(Id {0}), Id {0});
    ASSERT_EQ(pointers.lookup(Id {1}), Id {0});
    ASSERT_EQ(pointers.lookup(Id {2}), Id {2});
    ASSERT_EQ(pointers.lookup(Id {3}), Id {2});
    ASSERT_EQ(pointers.lookup(Id {4}), Id {2});
    ASSERT_EQ(pointers.lookup(Id {5}), Id {2});
}

 TEST_P(PointerMapTests, ReadsAndWritesEntries)
{
    std::string buffer(pager->page_size(), '\0');
    Page map_page;
    map_page.TEST_populate(Id {2}, buffer, true);
    PointerMap map {*pager};

    ASSERT_OK(map.write_entry(Id {3}, PointerMap::Entry {Id {33}, PointerMap::Node}));
    ASSERT_OK(map.write_entry(Id {4}, PointerMap::Entry {Id {44}, PointerMap::FreelistLink}));
    ASSERT_OK(map.write_entry(Id {5}, PointerMap::Entry {Id {55}, PointerMap::OverflowLink}));

    PointerMap::Entry entry_1, entry_2, entry_3;
    ASSERT_OK(map.read_entry(Id {3}, entry_1));
    ASSERT_OK(map.read_entry(Id {4}, entry_2));
    ASSERT_OK(map.read_entry(Id {5}, entry_3));

    ASSERT_EQ(entry_1.back_ptr.value, 33);
    ASSERT_EQ(entry_2.back_ptr.value, 44);
    ASSERT_EQ(entry_3.back_ptr.value, 55);
    ASSERT_EQ(entry_1.type, PointerMap::Node);
    ASSERT_EQ(entry_2.type, PointerMap::FreelistLink);
    ASSERT_EQ(entry_3.type, PointerMap::OverflowLink);
}

TEST_P(PointerMapTests, PointerMapCanFitAllPointers)
{
    PointerMap pointers {*pager};

    // PointerMap::find_map() expects the given pointer map page to be allocated already.
    for (std::size_t i {}; i < map_size() * 2; ++i) {
        Page page;
        ASSERT_OK(pager->allocate(page));
        pager->release(std::move(page));
    }

    for (std::size_t i {}; i < map_size() + 10; ++i) {
        if (i != map_size()) {
            const Id id {i + 3};
            const PointerMap::Entry entry {id, PointerMap::Node};
            ASSERT_OK(pointers.write_entry(id, entry));
        }
    }
    for (std::size_t i {}; i < map_size() + 10; ++i) {
        if (i != map_size()) {
            const Id id {i + 3};
            PointerMap::Entry entry;
            ASSERT_OK(pointers.read_entry(id, entry));
            ASSERT_EQ(entry.back_ptr.value, id.value);
            ASSERT_EQ(entry.type, PointerMap::Node);
        }
    }
}

TEST_P(PointerMapTests, MapPagesAreRecognized)
{
    Id id {2};
    PointerMap pointers {*pager};
    ASSERT_EQ(pointers.lookup(id), id);

    // Back pointers for the next "map.map_size()" pages are stored on page 2. The next pointermap page is
    // the page following the last page whose back pointer is on page 2. This pattern continues forever.
    for (std::size_t i {}; i < 1'000'000; ++i) {
        id.value += map_size() + 1;
        ASSERT_EQ(pointers.lookup(id), id);
    }
}

TEST_P(PointerMapTests, FindsCorrectMapPages)
{
    std::size_t counter {};
    Id map_id {2};
    PointerMap pointers {*pager};

    for (Id pid {3}; pid.value <= 100 * map_size(); ++pid.value) {
        if (counter++ == map_size()) {
            // Found a map page. Calls to find() with a page ID between this page and the next map page
            // should map to this page ID.
            map_id.value += map_size() + 1;
            counter = 0;
        } else {
            ASSERT_EQ(pointers.lookup(pid), map_id);
        }
    }
}

INSTANTIATE_TEST_SUITE_P(
    PointerMapTests,
    PointerMapTests,
    ::testing::Values(
        BPlusTreeTestParameters {MINIMUM_PAGE_SIZE},
        BPlusTreeTestParameters {MINIMUM_PAGE_SIZE * 2},
        BPlusTreeTestParameters {MAXIMUM_PAGE_SIZE / 2},
        BPlusTreeTestParameters {MAXIMUM_PAGE_SIZE}));

class VacuumTests
    : public TestWithPager,
      public testing::Test
{
public:
    static constexpr std::size_t FRAME_COUNT {8};

    VacuumTests()
        : meta {PAGE_SIZE}
    {
    }

    auto SetUp() -> void override
    {
        tree = std::make_unique<BPlusTree>(*pager);
        Node root;
        BPlusTreeInternal internal {*tree};
        ASSERT_OK(internal.allocate_root(root));
        internal.release(std::move(root));
        ASSERT_TRUE(pager->flush({}).is_ok());
        pointers = internal.pointers();
        overflow = internal.overflow();
        payloads = internal.payloads();
        freelist = internal.freelist();
    }

    auto acquire_node(Id pid, bool is_writable)
    {
        Page page;
        EXPECT_OK(pager->acquire(pid, page));
        Node node;
        node.page = std::move(page);
        node.scratch = scratch.data();
        node.meta = &meta(node.header.is_external);
        if (is_writable) {
            pager->upgrade(node.page);
        }
        node.initialize();
        return node;
    }

    auto allocate_node(bool is_external)
    {
        Page page;
        EXPECT_OK(pager->allocate(page));
        if (pointers->lookup(page.id()) == page.id()) {
            pager->release(std::move(page));
            EXPECT_OK(pager->allocate(page));
        }
        NodeHeader header;
        header.is_external = is_external;
        header.cell_start = static_cast<PageSize>(page.size());
        header.write(page);
        const auto pid = page.id();
        pager->release(std::move(page));
        return acquire_node(pid, true);
    }

    auto release_node(Node node) const
    {
        pager->release(std::move(node).take());
    }

    auto clean_up_test(std::size_t max_key_size, std::size_t max_value_size)
    {
        std::vector<std::string> keys;
        BPlusTreeInternal internal {*tree};
        while (test_is_root_external(*this)) {
            keys.emplace_back(random.Generate(max_key_size).to_string());
            (void)tree->insert(keys.back(), random.Generate(max_value_size));
            ASSERT_FALSE(internal.is_pointer_map(Id {pager->page_count()}));
        }
        for (const auto &key : keys) {
            ASSERT_OK(tree->erase(key));
        }

        bool vacuumed;
        Id target {pager->page_count()};
        do {
            ASSERT_OK(tree->vacuum_one(target, vacuumed));
            --target.value;
        } while (vacuumed);
        ASSERT_TRUE(target.is_null());
    }

    auto sanity_check(std::size_t lower_bounds, std::size_t record_count, std::size_t max_key_size, std::size_t max_value_size) const
    {
        std::unordered_map<std::string, std::string> map;

        for (std::size_t iteration {}; iteration < 5; ++iteration) {
            while (map.size() < lower_bounds + record_count) {
                const auto key_size = random.Next<std::size_t>(1, max_key_size);
                const auto key = random.Generate(key_size);
                const auto value_size = random.Next<std::size_t>(max_value_size);
                const auto value = random.Generate(value_size);
                ASSERT_OK(tree->insert(key, value));
                map[key.to_string()] = value.to_string();
            }

            auto itr = begin(map);
            while (map.size() > lower_bounds) {
                ASSERT_OK(tree->erase(itr->first));
                itr = map.erase(itr);
            }

            Id target {pager->page_count()};
            for (;;) {
                bool vacuumed {};
                ASSERT_OK(tree->vacuum_one(target, vacuumed));
                if (!vacuumed) {
                    break;
                }
                tree->TEST_check_links();
                tree->TEST_check_nodes();
                tree->TEST_check_order();
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

    NodeMetaManager meta;
    std::unique_ptr<BPlusTree> tree;
    PointerMap *pointers {};
    OverflowList *overflow {};
    PayloadManager *payloads {};
    Freelist *freelist {};
};

//      P   1   2   3
// [1] [2] [3] [4] [5]
//
TEST_F(VacuumTests, FreelistRegistersBackPointers)
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
    ASSERT_OK(pointers->read_entry(Id {5}, entry));
    ASSERT_EQ(entry.type, PointerMap::FreelistLink);
    ASSERT_EQ(entry.back_ptr, Id {4});

    ASSERT_OK(pointers->read_entry(Id {4}, entry));
    ASSERT_EQ(entry.type, PointerMap::FreelistLink);
    ASSERT_EQ(entry.back_ptr, Id {3});

    ASSERT_OK(pointers->read_entry(Id {3}, entry));
    ASSERT_EQ(entry.type, PointerMap::FreelistLink);
    ASSERT_EQ(entry.back_ptr, Id::null());
}

TEST_F(VacuumTests, OverflowChainRegistersBackPointers)
{
    // Creates an overflow chain of length 2, rooted at the second cell on the root page.
    std::string overflow_data(PAGE_SIZE * 2, 'x');
    ASSERT_OK(tree->insert("a", "value"));
    ASSERT_OK(tree->insert("b", overflow_data));

    PointerMap::Entry head_entry, tail_entry;
    ASSERT_OK(pointers->read_entry(Id {3}, head_entry));
    ASSERT_OK(pointers->read_entry(Id {4}, tail_entry));

    ASSERT_TRUE(head_entry.back_ptr.is_root());
    ASSERT_EQ(tail_entry.back_ptr, Id {3});
    ASSERT_EQ(head_entry.type, PointerMap::OverflowHead);
    ASSERT_EQ(tail_entry.type, PointerMap::OverflowLink);
}

TEST_F(VacuumTests, OverflowChainIsNullTerminated)
{
    {
        // allocate_node() accounts for the first pointer map page.
        auto node_3 = allocate_node(true);
        Page page_4;
        ASSERT_OK(pager->allocate(page_4));
        ASSERT_EQ(page_4.id().value, 4);
        write_next_id(node_3.page, Id {123});
        write_next_id(page_4, Id {123});
        ASSERT_OK(freelist->push(std::move(page_4)));
        ASSERT_OK(freelist->push(std::move(node_3.page)));
    }

    ASSERT_OK(tree->insert("a", "value"));
    ASSERT_OK(tree->insert("b", std::string(PAGE_SIZE * 2, 'x')));

    Page page_3, page_4;
    ASSERT_OK(pager->acquire(Id {3}, page_3));
    ASSERT_OK(pager->acquire(Id {4}, page_4));
    ASSERT_EQ(read_next_id(page_3), Id {4});
    ASSERT_EQ(read_next_id(page_4), Id::null());
    pager->release(std::move(page_3));
    pager->release(std::move(page_4));
}

TEST_F(VacuumTests, VacuumsFreelistInOrder)
{
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
    bool vacuumed {};
    ASSERT_OK(tree->vacuum_one(Id {5}, vacuumed));
    ASSERT_TRUE(vacuumed);

    PointerMap::Entry entry;
    ASSERT_OK(pointers->read_entry(Id {4}, entry));
    ASSERT_EQ(entry.type, PointerMap::FreelistLink);
    ASSERT_EQ(entry.back_ptr, Id::null());

    // Page Types:     N   P   1
    // Page Contents: [1] [2] [3] [X] [X]
    // Page IDs:       1   2   3   4   5
    ASSERT_OK(tree->vacuum_one(Id {4}, vacuumed));
    ASSERT_TRUE(vacuumed);
    ASSERT_OK(pointers->read_entry(Id {3}, entry));
    ASSERT_EQ(entry.type, PointerMap::FreelistLink);
    ASSERT_EQ(entry.back_ptr, Id::null());

    // Page Types:     N   P
    // Page Contents: [1] [2] [X] [X] [X]
    // Page IDs:       1   2   3   4   5
    ASSERT_OK(tree->vacuum_one(Id {3}, vacuumed));
    ASSERT_TRUE(vacuumed);
    ASSERT_TRUE(freelist->is_empty());

    // Page Types:     N
    // Page Contents: [1] [X] [X] [X] [X]
    // Page IDs:       1   2   3   4   5
    ASSERT_OK(tree->vacuum_one(Id {2}, vacuumed));
    ASSERT_TRUE(vacuumed);

    // Page Types:     N
    // Page Contents: [1]
    // Page IDs:       1
    ASSERT_OK(pager->truncate(1));
    ASSERT_EQ(pager->page_count(), 1);
}

TEST_F(VacuumTests, VacuumsFreelistInReverseOrder)
{
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
    bool vacuumed {};
    ASSERT_OK(tree->vacuum_one(Id {5}, vacuumed));
    ASSERT_TRUE(vacuumed);
    PointerMap::Entry entry;
    ASSERT_OK(pointers->read_entry(Id {4}, entry));
    ASSERT_EQ(entry.back_ptr, Id::null());
    ASSERT_EQ(entry.type, PointerMap::FreelistLink);
    {
        Page page;
        ASSERT_OK(pager->acquire(Id {4}, page));
        ASSERT_EQ(read_next_id(page), Id {3});
        pager->release(std::move(page));
    }

    // Page Types:     N   P   1
    // Page Contents: [a] [b] [e] [ ] [ ]
    // Page IDs:       1   2   3   4   5
    ASSERT_OK(tree->vacuum_one(Id {4}, vacuumed));
    ASSERT_TRUE(vacuumed);
    ASSERT_OK(pointers->read_entry(Id {3}, entry));
    ASSERT_EQ(entry.type, PointerMap::FreelistLink);
    ASSERT_EQ(entry.back_ptr, Id::null());

    // Page Types:     N   P
    // Page Contents: [a] [b] [ ] [ ] [ ]
    // Page IDs:       1   2   3   4   5
    ASSERT_OK(tree->vacuum_one(Id {3}, vacuumed));
    ASSERT_TRUE(vacuumed);
    ASSERT_TRUE(freelist->is_empty());

    // Page Types:     N
    // Page Contents: [a] [ ] [ ] [ ] [ ]
    // Page IDs:       1   2   3   4   5
    ASSERT_OK(tree->vacuum_one(Id {2}, vacuumed));
    ASSERT_TRUE(vacuumed);

    // Page Types:     N
    // Page Contents: [a]
    // Page IDs:       1
    ASSERT_OK(pager->truncate(1));
    ASSERT_EQ(pager->page_count(), 1);
}

TEST_F(VacuumTests, CleansUpOverflowValues)
{
    clean_up_test(16, PAGE_SIZE * 2);
}

// When external nodes are merged, the separator is removed. If the separator key was overflowing, we
// must not forget to move its overflow pages to the freelist.
TEST_F(VacuumTests, CleansUpOverflowKeys)
{
    clean_up_test(PAGE_SIZE * 2, 16);
}

TEST_F(VacuumTests, CleansUpOverflowPayloads)
{
    clean_up_test(PAGE_SIZE * 2, PAGE_SIZE * 2);
}

TEST_F(VacuumTests, VacuumFreelistSanityCheck)
{
    std::default_random_engine rng {42};

    for (std::size_t iteration {}; iteration < 1'000; ++iteration) {
        std::vector<Node> nodes;
        for (std::size_t i {}; i < FRAME_COUNT - 1; ++i) {
            nodes.emplace_back(allocate_node(true));
        }

        std::shuffle(begin(nodes), end(nodes), rng);

        for (auto &node : nodes) {
            ASSERT_OK(freelist->push(std::move(node.page)));
        }

        // This will vacuum the whole freelist, as well as the pointer map page on page 2.
        Id target {pager->page_count()};
        bool vacuumed {};
        for (std::size_t i {}; i < FRAME_COUNT; ++i) {
            ASSERT_OK(tree->vacuum_one(target, vacuumed));
            ASSERT_TRUE(vacuumed);
            --target.value;
        }
        ASSERT_OK(tree->vacuum_one(target, vacuumed));
        ASSERT_FALSE(vacuumed);
        ASSERT_OK(pager->truncate(1));
        ASSERT_EQ(pager->page_count(), 1);
    }
}

static auto vacuum_and_validate(VacuumTests &test, const std::string &value)
{
    bool vacuumed;
    ASSERT_EQ(test.pager->page_count(), 6);
    ASSERT_OK(test.tree->vacuum_one(Id {6}, vacuumed));
    ASSERT_TRUE(vacuumed);
    ASSERT_OK(test.tree->vacuum_one(Id {5}, vacuumed));
    ASSERT_TRUE(vacuumed);
    ASSERT_OK(test.pager->truncate(4));
    ASSERT_EQ(test.pager->page_count(), 4);

    auto *cursor = CursorInternal::make_cursor(*test.tree);
    cursor->seek_first();
    ASSERT_TRUE(cursor->is_valid());
    ASSERT_EQ(cursor->key(), "a");
    ASSERT_EQ(cursor->value(), "value");
    cursor->next();

    ASSERT_TRUE(cursor->is_valid());
    ASSERT_EQ(cursor->key(), "b");
    ASSERT_EQ(cursor->value(), value);
    cursor->next();

    ASSERT_FALSE(cursor->is_valid());
    delete cursor;
}

TEST_F(VacuumTests, VacuumsOverflowChain_A)
{
    // Save these pages until the overflow chain is created, otherwise they will be used for it.
    auto node_3 = allocate_node(true);
    auto node_4 = allocate_node(true);
    ASSERT_EQ(node_4.page.id().value, 4);

    // Creates an overflow chain of length 2, rooted at the second cell on the root page.
    std::string overflow_data(PAGE_SIZE * 2, 'x');
    ASSERT_OK(tree->insert("a", "value"));
    ASSERT_OK(tree->insert("b", overflow_data));

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
    ASSERT_OK(pointers->read_entry(Id {3}, head_entry));
    ASSERT_OK(pointers->read_entry(Id {4}, tail_entry));

    ASSERT_TRUE(head_entry.back_ptr.is_root());
    ASSERT_EQ(tail_entry.back_ptr, Id {3});
    ASSERT_EQ(head_entry.type, PointerMap::OverflowHead);
    ASSERT_EQ(tail_entry.type, PointerMap::OverflowLink);
}

TEST_F(VacuumTests, VacuumsOverflowChain_B)
{
    // This time, we'll force the head of the overflow chain to be the last page in the file.
    auto node_3 = allocate_node(true);
    auto node_4 = allocate_node(true);
    auto node_5 = allocate_node(true);
    auto node_6 = allocate_node(true);
    ASSERT_EQ(node_6.page.id().value, 6);
    ASSERT_OK(freelist->push(std::move(node_5.page)));
    ASSERT_OK(freelist->push(std::move(node_6.page)));

    std::string overflow_data(PAGE_SIZE * 2, 'x');
    ASSERT_OK(tree->insert("a", "value"));
    ASSERT_OK(tree->insert("b", overflow_data));

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
    ASSERT_OK(pointers->read_entry(Id {4}, head_entry));
    ASSERT_OK(pointers->read_entry(Id {3}, tail_entry));

    ASSERT_TRUE(head_entry.back_ptr.is_root());
    ASSERT_EQ(tail_entry.back_ptr, Id {4});
    ASSERT_EQ(head_entry.type, PointerMap::OverflowHead);
    ASSERT_EQ(tail_entry.type, PointerMap::OverflowLink);
}

TEST_F(VacuumTests, VacuumOverflowChainSanityCheck)
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
    for (std::size_t i {}; i < 3; ++i) {
        const auto value = random.Generate(PAGE_SIZE * std::min<std::size_t>(i + 1, 2));
        ASSERT_OK(tree->insert(tools::integral_key<1>(i), value));
        values.emplace_back(value.to_string());
    }

    while (!reserved.empty()) {
        ASSERT_OK(freelist->push(std::move(reserved.back().page)));
        reserved.pop_back();
    }

    bool vacuumed;
    ASSERT_EQ(pager->page_count(), 12);
    ASSERT_OK(tree->vacuum_one(Id {12}, vacuumed));
    ASSERT_OK(tree->vacuum_one(Id {11}, vacuumed));
    ASSERT_OK(tree->vacuum_one(Id {10}, vacuumed));
    ASSERT_OK(tree->vacuum_one(Id {9}, vacuumed));
    ASSERT_OK(tree->vacuum_one(Id {8}, vacuumed));
    ASSERT_OK(pager->truncate(7));
    ASSERT_EQ(pager->page_count(), 7);

    auto *cursor = CursorInternal::make_cursor(*tree);
    cursor->seek_first();
    for (std::size_t i {}; i < values.size(); ++i) {
        ASSERT_TRUE(cursor->is_valid());
        ASSERT_EQ(cursor->key(), tools::integral_key<1>(i));
        ASSERT_EQ(cursor->value(), values[i]);
        cursor->next();
    }
    ASSERT_FALSE(cursor->is_valid());
    delete cursor;
}

TEST_F(VacuumTests, VacuumsNodes)
{
    auto node_3 = allocate_node(true);
    auto node_4 = allocate_node(true);
    ASSERT_EQ(node_4.page.id().value, 4);

    std::vector<std::string> values;
    for (std::size_t i {}; i < 5; ++i) {
        const auto key = tools::integral_key(i);
        const auto value = random.Generate(meta(true).max_local - key.size());
        ASSERT_OK(tree->insert(key, value));
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
    ASSERT_EQ(pager->page_count(), 6);
    bool vacuumed {};
    ASSERT_OK(tree->vacuum_one(Id {6}, vacuumed));
    ASSERT_TRUE(vacuumed);
    ASSERT_OK(tree->vacuum_one(Id {5}, vacuumed));
    ASSERT_TRUE(vacuumed);
    ASSERT_OK(pager->truncate(4));

    auto *cursor = CursorInternal::make_cursor(*tree);
    cursor->seek_first();
    for (std::size_t i {}; i < values.size(); ++i) {
        ASSERT_TRUE(cursor->is_valid());
        ASSERT_EQ(cursor->key(), tools::integral_key(i));
        ASSERT_EQ(cursor->value(), values[i]);
        cursor->next();
    }
    ASSERT_FALSE(cursor->is_valid());
    delete cursor;
}

TEST_F(VacuumTests, SanityCheck_Freelist)
{
    sanity_check(0, 50, 16, 16);
}

TEST_F(VacuumTests, SanityCheck_Freelist_OverflowHead)
{
    sanity_check(0, 50, 16, PAGE_SIZE / 2);
}

TEST_F(VacuumTests, SanityCheck_Freelist_OverflowLink)
{
    sanity_check(0, 50, 16, PAGE_SIZE * 2);
}

TEST_F(VacuumTests, SanityCheck_Nodes_1)
{
    sanity_check(50, 50, 16, 16);
}

TEST_F(VacuumTests, SanityCheck_Nodes_2)
{
    sanity_check(200, 50, 16, 16);
}

TEST_F(VacuumTests, SanityCheck_NodesWithOverflowValues)
{
    sanity_check(50, 50, 16, PAGE_SIZE * 2);
}

TEST_F(VacuumTests, SanityCheck_NodesWithOverflowKeys)
{
    sanity_check(50, 50, PAGE_SIZE * 2, 16);
}

TEST_F(VacuumTests, SanityCheck_NodesWithOverflowPayloads)
{
    sanity_check(50, 50, PAGE_SIZE * 2, PAGE_SIZE * 2);
}

} // namespace calicodb