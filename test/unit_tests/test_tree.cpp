#include "pager/pager.h"
#include "tree/cursor_impl.h"
#include "tree/memory.h"
#include "tree/node.h"
#include "tree/tree.h"
#include "unit_tests.h"
#include "wal/helpers.h"
#include <gtest/gtest.h>

namespace Calico {

class NodeMetaManager {
    NodeMeta m_external_meta;
    NodeMeta m_internal_meta;

public:
    explicit NodeMetaManager(Size page_size)
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
      public testing::Test {
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
      public testing::Test {
public:
    ~ComponentTests() override = default;

    auto SetUp() -> void override
    {
        collect_scratch.resize(PAGE_SIZE);

        pointers = std::make_unique<PointerMap>(*pager);
        freelist = std::make_unique<FreeList>(*pager, *pointers);
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
    std::unique_ptr<FreeList> freelist;
    std::unique_ptr<OverflowList> overflow;
    std::unique_ptr<PayloadManager> payloads;
};

TEST_F(ComponentTests, EmplacesCell)
{
    auto root = acquire_node(Id::root(), true);
    auto *start = root.page.data() + 0x100;
    const auto *end = emplace_cell(start, 1, 1, "a", "1");
    const auto cell_size = static_cast<Size>(end - start);
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
    const auto cell_size = static_cast<Size>(end - start);
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
    const auto cell_size = static_cast<Size>(end - start);
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
    for (Size i {}; i < 3; ++i) {
        const auto key = random.Generate(PAGE_SIZE * 10);
        const auto value = random.Generate(PAGE_SIZE * 10);
        ASSERT_OK(payloads->emplace(collect_scratch.data(), root, key, value, i));
        data.emplace_back(key.to_string());
        data.emplace_back(value.to_string());
    }
    for (Size i {}; i < 3; ++i) {
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

static auto run_promotion_test(ComponentTests &test, Size key_size, Size value_size)
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
    ASSERT_EQ(old_head.type, PointerMap::OVERFLOW_HEAD);
    ASSERT_EQ(old_head.back_ptr, Id::root());

    // Copy of the overflow key.
    PointerMap::Entry new_head;
    ASSERT_OK(pointers->read_entry(Id {4}, new_head));
    ASSERT_EQ(new_head.type, PointerMap::OVERFLOW_HEAD);
    ASSERT_EQ(new_head.back_ptr, Id::root());
}

TEST_F(ComponentTests, PromotionIgnoresOverflowValue)
{
    run_promotion_test(*this, 10, PAGE_SIZE);
    PointerMap::Entry old_head;
    ASSERT_OK(pointers->read_entry(Id {3}, old_head));
    ASSERT_EQ(old_head.type, PointerMap::OVERFLOW_HEAD);
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
    ASSERT_EQ(old_head.type, PointerMap::OVERFLOW_HEAD);
    ASSERT_EQ(old_head.back_ptr, Id::root());

    // 1 overflow page needed for the key, and 1 for the value.
    PointerMap::Entry new_head;
    ASSERT_OK(pointers->read_entry(Id {5}, new_head));
    ASSERT_EQ(new_head.type, PointerMap::OVERFLOW_HEAD);
    ASSERT_EQ(new_head.back_ptr, Id::root());
}

TEST_F(ComponentTests, NodeIterator)
{
    for (Size i {}; i < 4; ++i) {
        auto root = acquire_node(Id::root(), true);
        const auto key = Tools::integral_key<2>((i + 1) * 2);
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
    for (Size i: {1, 6, 3, 2, 8, 4, 5, 7}) {
        ASSERT_EQ(i % 2 == 0, itr.seek(Tools::integral_key<2>(i)));
        ASSERT_TRUE(itr.is_valid());
        ASSERT_OK(itr.status());
    }
    ASSERT_FALSE(itr.seek(Tools::integral_key<2>(10)));
    ASSERT_FALSE(itr.is_valid());
    ASSERT_OK(itr.status());
    release_node(std::move(root));
}

TEST_F(ComponentTests, NodeIteratorHandlesOverflowKeys)
{
    std::vector<std::string> keys;
    for (Size i {}; i < 3; ++i) {
        auto root = acquire_node(Id::root(), true);
        auto key = random.Generate(PAGE_SIZE).to_string();
        const auto value = random.Generate(PAGE_SIZE).to_string();
        key[0] = static_cast<Byte>(i);
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
    Size i {};
    for (const auto &key: keys) {
        ASSERT_TRUE(itr.seek(key));
        ASSERT_TRUE(itr.is_valid());
        ASSERT_EQ(itr.index(), i++);
        ASSERT_OK(itr.status());
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
    {}

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

class ExternalRootSplitTests : public NodeTests {
public:
    ExternalRootSplitTests() = default;

    auto SetUp() -> void override
    {
        // <cell_pointer> + <value_size> + <key_size> + <key> + <value> + <overflow_id>
        static constexpr Size min_cell_size {2 + 1 + 1 + 1 + 0 + 0};

        NodeTests::SetUp();
        BPlusTreeInternal internal {*tree};
        bool exists {};
        bool done {};
        Node root;

        ASSERT_OK(internal.acquire(root, Id::root()));
        auto space = usable_space(root);
        internal.release(std::move(root));

        for (Size i {}; space >= min_cell_size; ++i) {
            ASSERT_LT(i, 0x100);
            keys.emplace_back(1, static_cast<Byte>(i));
            space -= min_cell_size;
        }

        // Create the worst case for splitting.
        for (const auto &key: keys) {
            ASSERT_OK(tree->insert(key, "", exists));
            ASSERT_FALSE(exists);
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

        for (const auto &key: keys) {
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
    bool _;
};

TEST_F(ExternalRootSplitTests, SplitOnLeftmost)
{
    const auto record = large_payload(keys.front());
    ASSERT_OK(tree->insert(record.key, record.value, _));
    assert_contains(record);
}

TEST_F(ExternalRootSplitTests, SplitLeftOfMiddle)
{
    const auto record = large_payload(keys[keys.size() / 2 - 10]);
    ASSERT_OK(tree->insert(record.key, record.value, _));
    assert_contains(record);
}

TEST_F(ExternalRootSplitTests, SplitOnRightmost)
{
    const auto record = large_payload(keys.back());
    ASSERT_OK(tree->insert(record.key, record.value, _));
    assert_contains(record);
}

TEST_F(ExternalRootSplitTests, SplitRightOfMiddle)
{
    const auto record = large_payload(keys[keys.size() / 2 + 10]);
    ASSERT_OK(tree->insert(record.key, record.value, _));
    assert_contains(record);
}

class InternalRootSplitTests: public ExternalRootSplitTests {
public:
    auto SetUp() -> void override
    {
        ExternalRootSplitTests::SetUp();

        // Cause the root to overflow and become an internal node.
        const auto record = large_payload(std::string(1, '\0'));
        ASSERT_OK(tree->insert(record.key, record.value, _));

        // The key needs to be 2 bytes, there aren't enough possible 1-byte keys to make the root split again.
        // <cell_pointer> + <child_id> + <key_size> + <key>
        const auto min_cell_size = 2 + 8 + 1 + 2;
        ASSERT_EQ(varint_length(2), 1);

        BPlusTreeInternal internal {*tree};
        bool exists {};
        bool done {};
        Node root;

        for (auto i = key_range_start; ; i += 2) {
            ASSERT_OK(internal.acquire(root, Id::root()));
            base_usable_space = usable_space(root);
            internal.release(std::move(root));
            if (base_usable_space < min_cell_size) {
                break;
            }
            ASSERT_LE(i, key_range_max);
            keys_2.emplace_back(base_key(i));

            ASSERT_OK(tree->insert(keys_2.back(), "", exists));
            ASSERT_FALSE(exists);
        }
    }

    auto TearDown() -> void override
    {
        ASSERT_TRUE(is_finished());
        for (const auto &key: keys_2) {
            assert_contains({key, ""});
        }
        ExternalRootSplitTests::TearDown();
    }

    static auto base_key(Size index) -> std::string
    {
       std::string key(2, '\0');
       key[0] = static_cast<Byte>(index >> 8);
       key[1] = static_cast<Byte>(index);
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
    Size base_usable_space {};
    Size key_range_start {4'000};
    Size key_range_max {6'000};
};

TEST_F(InternalRootSplitTests, Split_1)
{
    for (Size i {}; !is_finished(); ++i) {
        const auto record = large_payload(keys_2[i]);
        ASSERT_OK(tree->insert(record.key, record.value, _));
    }
}

TEST_F(InternalRootSplitTests, Split_2)
{
    for (auto itr = --end(keys_2); !is_finished(); --itr) {
        const auto record = large_payload(*itr);
        ASSERT_OK(tree->insert(record.key, record.value, _));
    }
}

struct BPlusTreeTestParameters {
    Size page_size {};
    Size extra {};
};

class BPlusTreeTests
    : public TestWithPager,
      public testing::TestWithParam<BPlusTreeTestParameters> {
public:
    BPlusTreeTests()
        : param {GetParam()},
          collect_scratch(param.page_size, '\x00')
    {}

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

    [[nodiscard]] auto make_value(char c, bool overflow = false) const
    {
        Size size {param.page_size};
        if (overflow) {
            size /= 3;
        } else {
            size /= 20;
        }
        return std::string(size, c);
    }

    auto acquire_node(Id pid)
    {
        Node node;
        EXPECT_OK(pager->acquire(pid, node.page));
        node.scratch = scratch.data();
        node.header.read(node.page);
        node.meta = &meta(node.header.is_external);
        return node;
    }

    auto release_node(Node node) const
    {
        pager->release(std::move(node).take());
    }

    auto is_root_external()
    {
        auto root = acquire_node(Id::root());
        const auto answer = root.header.is_external;
        release_node(std::move(root));
        return answer;
    }

    auto validate() const -> void
    {
        tree->TEST_check_nodes();
        tree->TEST_check_links();
        tree->TEST_check_order();
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
    ASSERT_OK(tree->insert("a", make_value('1'), exists));
    ASSERT_FALSE(exists);
    ASSERT_OK(tree->insert("b", make_value('2'), exists));
    ASSERT_FALSE(exists);
    ASSERT_OK(tree->insert("c", make_value('3'), exists));
    ASSERT_FALSE(exists);
    validate();
}

TEST_P(BPlusTreeTests, ErasesRecords)
{
    bool _;
    (void)tree->insert("a", make_value('1'), _);
    (void)tree->insert("b", make_value('2'), _);
    (void)tree->insert("c", make_value('3'), _);
    ASSERT_OK(tree->erase("a"));
    ASSERT_OK(tree->erase("b"));
    ASSERT_OK(tree->erase("c"));
    validate();
}

TEST_P(BPlusTreeTests, FindsRecords)
{
    const auto keys = "abc";
    const auto vals = "123";
    bool _;
    (void)tree->insert(std::string(1, keys[0]), make_value(vals[0]), _);
    (void)tree->insert(std::string(1, keys[1]), make_value(vals[1]), _);
    (void)tree->insert(std::string(1, keys[2]), make_value(vals[2]), _);

    for (Size i {}; i < 3; ++i) {
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

TEST_P(BPlusTreeTests, WritesOverflowChains)
{
    bool _;
    ASSERT_OK(tree->insert("a", make_value('1', true), _));
    ASSERT_OK(tree->insert("b", make_value('2', true), _));
    ASSERT_OK(tree->insert("c", make_value('3', true), _));
    validate();
}

TEST_P(BPlusTreeTests, ErasesOverflowChains)
{
    bool _;
    (void)tree->insert("a", make_value('1', true), _);
    (void)tree->insert("b", make_value('2', true), _);
    (void)tree->insert("c", make_value('3', true), _);
    ASSERT_OK(tree->erase("a"));
    ASSERT_OK(tree->erase("b"));
    ASSERT_OK(tree->erase("c"));
}

TEST_P(BPlusTreeTests, ReadsValuesFromOverflowChains)
{
    bool _;
    const auto keys = "abc";
    const auto vals = "123";
    std::string values[3];
    values[0] = random.Generate(param.page_size).to_string();
    values[1] = random.Generate(param.page_size).to_string();
    values[2] = random.Generate(param.page_size).to_string();
    (void)tree->insert(std::string(1, keys[0]), values[0], _);
    (void)tree->insert(std::string(1, keys[1]), values[1], _);
    (void)tree->insert(std::string(1, keys[2]), values[2], _);

    for (Size i {}; i < 3; ++i) {
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
    bool _;
    for (Size i {}; is_root_external(); ++i) {
        ASSERT_OK(tree->insert(Tools::integral_key(i), make_value('v'), _));
        validate();
    }
}

TEST_P(BPlusTreeTests, ResolvesFirstOverflowOnLeftmostPosition)
{
    bool _;
    for (Size i {}; is_root_external(); ++i) {
        ASSERT_LE(i, 100);
        ASSERT_OK(tree->insert(Tools::integral_key(100 - i), make_value('v'), _));
    }
    validate();
}

TEST_P(BPlusTreeTests, ResolvesFirstOverflowOnMiddlePosition)
{
    bool _;
    for (Size i {}; is_root_external(); ++i) {
        ASSERT_LE(i, 100);
        ASSERT_OK(tree->insert(Tools::integral_key(i & 1 ? 100 - i : i), make_value('v'), _));
    }
    validate();
}

TEST_P(BPlusTreeTests, ResolvesMultipleOverflowsOnLeftmostPosition)
{
    bool _;
    for (Size i {}; i < 1'000; ++i) {
        ASSERT_OK(tree->insert(Tools::integral_key(999 - i), make_value('v'), _));
        if (i % 100 == 99) {
            validate();
        }
    }
}

TEST_P(BPlusTreeTests, ResolvesMultipleOverflowsOnRightmostPosition)
{
    bool _;
    for (Size i {}; i < 1'000; ++i) {
        ASSERT_OK(tree->insert(Tools::integral_key(i), make_value('v'), _));
        if (i % 100 == 99) {
            validate();
        }
    }
}

TEST_P(BPlusTreeTests, ResolvesMultipleOverflowsOnMiddlePosition)
{
    bool _;
    for (Size i {}, j {999}; i < j; ++i, --j) {
        ASSERT_OK(tree->insert(Tools::integral_key(i), make_value('v'), _));
        ASSERT_OK(tree->insert(Tools::integral_key(j), make_value('v'), _));

        if (i % 100 == 99) {
            validate();
        }
    }
}

TEST_P(BPlusTreeTests, ResolvesFirstUnderflowOnRightmostPosition)
{
    bool _;
    long i {};
    for (; is_root_external(); ++i) {
        (void)tree->insert(Tools::integral_key(i), make_value('v'), _);
    }

    while (i--) {
        ASSERT_OK(tree->erase(Tools::integral_key(i)));
        validate();
    }
}

TEST_P(BPlusTreeTests, ResolvesFirstUnderflowOnLeftmostPosition)
{
    Size i {};
    bool _;
    for (; is_root_external(); ++i) {
        (void)tree->insert(Tools::integral_key(i), make_value('v'), _);
    }
    for (Size j {}; j < i; ++j) {
        ASSERT_OK(tree->erase(Tools::integral_key(j)));
        validate();
    }
}

TEST_P(BPlusTreeTests, ResolvesFirstUnderflowOnMiddlePosition)
{
    Size i {};
    bool _;
    for (; is_root_external(); ++i) {
        (void)tree->insert(Tools::integral_key(i), make_value('v'), _);
    }
    for (Size j {1}; j < i / 2 - 1; ++j) {
        ASSERT_OK(tree->erase(Tools::integral_key(i / 2 - j + 1)));
        ASSERT_OK(tree->erase(Tools::integral_key(i / 2 + j)));
        validate();
    }
}

static auto insert_1000(BPlusTreeTests &test, bool has_overflow = false)
{
    bool _;
    for (Size i {}; i < 1'000; ++i) {
        (void)test.tree->insert(Tools::integral_key(i), test.make_value('v', has_overflow), _);
    }
}

TEST_P(BPlusTreeTests, ResolvesMultipleUnderflowsOnRightmostPosition)
{
    insert_1000(*this);
    for (Size i {}; i < 1'000; ++i) {
        ASSERT_OK(tree->erase(Tools::integral_key(999 - i)));
        if (i % 100 == 99) {
            validate();
        }
    }
}

TEST_P(BPlusTreeTests, ResolvesMultipleUnderflowsOnLeftmostPosition)
{
    insert_1000(*this);
    for (Size i {}; i < 1'000; ++i) {
        ASSERT_OK(tree->erase(Tools::integral_key(i)));
        if (i % 100 == 99) {
            validate();
        }
    }
}

TEST_P(BPlusTreeTests, ResolvesMultipleUnderflowsOnMiddlePosition)
{
    insert_1000(*this);
    for (Size i {}, j {999}; i < j; ++i, --j) {
        ASSERT_OK(tree->erase(Tools::integral_key(i)));
        ASSERT_OK(tree->erase(Tools::integral_key(j)));
        if (i % 100 == 99) {
            validate();
        }
    }
}

TEST_P(BPlusTreeTests, ResolvesOverflowsFromOverwrite)
{
    bool _;
    for (Size i {}; i < 1'000; ++i) {
        ASSERT_OK(tree->insert(Tools::integral_key(i), "v", _));
    }
    // Replace the small values with very large ones.
    for (Size i {}; i < 1'000; ++i) {
        ASSERT_OK(tree->insert(Tools::integral_key(i), make_value('v', true), _));
    }
    validate();
}

TEST_P(BPlusTreeTests, SplitWithLongKeys)
{
    bool _;
    for (unsigned i {}; i < 1'000; ++i) {
        const auto key = random.Generate(GetParam().page_size * 2);
        ASSERT_OK(tree->insert(key, "v", _));
        if (i % 100 == 99) {
            validate();
        }
    }
}

TEST_P(BPlusTreeTests, SplitWithShortAndLongKeys)
{
    bool _;
    for (unsigned i {}; i < 1'000; ++i) {
        char key[3] {};
        put_u16(key, 999 - i);
        ASSERT_OK(tree->insert({key, 2}, "v", _));
    }
    for (unsigned i {}; i < 1'000; ++i) {
        const auto key = random.Generate(GetParam().page_size);
        ASSERT_OK(tree->insert(key, "v", _));

        if (i % 100 == 99) {
            validate();
        }
    }
}

INSTANTIATE_TEST_SUITE_P(
    BPlusTreeTests,
    BPlusTreeTests,
    ::testing::Values(
        BPlusTreeTestParameters {MINIMUM_PAGE_SIZE},
        BPlusTreeTestParameters {MINIMUM_PAGE_SIZE * 2},
        BPlusTreeTestParameters {MAXIMUM_PAGE_SIZE / 2},
        BPlusTreeTestParameters {MAXIMUM_PAGE_SIZE}));

class BPlusTreeSanityChecks : public BPlusTreeTests {
public:
    auto random_chunk(bool overflow, bool nonzero = true)
    {
        return random.Generate(random.Next<Size>(nonzero, param.page_size * overflow + 12));
    }

    auto random_write() -> Record
    {
        const auto key = random_chunk(overflow_keys);
        const auto val = random_chunk(overflow_values, false);
        bool _;
        EXPECT_OK(tree->insert(key, val, _));
        return {key.to_string(), val.to_string()};
    }

    bool overflow_keys = GetParam().extra & 0b10;
    bool overflow_values = GetParam().extra & 0b01;
};

TEST_P(BPlusTreeSanityChecks, Insert)
{
    for (Size i {}; i < 1'000; ++i) {
        random_write();
        if (i % 100 == 99) {
            validate();
        }
    }
}

TEST_P(BPlusTreeSanityChecks, Search)
{
    std::unordered_map<std::string, std::string> records;
    for (Size i {}; i < 1'000; ++i) {
        const auto [k, v] = random_write();
        records[k] = v;
    }
    validate();

    for (const auto &[key, value]: records) {
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
    for (Size iteration {}; iteration < 3; ++iteration) {
        for (Size i {}; i < 1'000; ++i) {
            const auto [k, v] = random_write();
            records[k] = v;
        }

        Size i {};
        for (const auto &[key, value]: records) {
            ASSERT_OK(tree->erase(key));
            if (i % 100 == 99) {
                validate();
            }
        }
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

class CursorTests : public BPlusTreeTests {
protected:
    static constexpr Size RECORD_COUNT {1'000};

    auto SetUp() -> void override
    {
        BPlusTreeTests::SetUp();
        insert_1000(*this);
    }
};

TEST_P(CursorTests, KeyAndValueUseSeparateStorage)
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
    for (Size i {}; i < RECORD_COUNT; ++i) {
        ASSERT_TRUE(cursor->is_valid());
        ASSERT_EQ(cursor->key(), Tools::integral_key(i));
        ASSERT_EQ(cursor->value(), make_value('v'));
        cursor->next();
    }
    ASSERT_FALSE(cursor->is_valid());
}

TEST_P(CursorTests, SeeksForwardFromBoundary)
{
    std::unique_ptr<Cursor> cursor {CursorInternal::make_cursor(*tree)};
    cursor->seek(Tools::integral_key(RECORD_COUNT / 4));
    for (Size i {}; i < RECORD_COUNT * 3 / 4; ++i) {
        ASSERT_TRUE(cursor->is_valid());
        cursor->next();
    }
    ASSERT_FALSE(cursor->is_valid());
}

TEST_P(CursorTests, SeeksForwardToBoundary)
{
    std::unique_ptr<Cursor> cursor {CursorInternal::make_cursor(*tree)};
    std::unique_ptr<Cursor> bounds {CursorInternal::make_cursor(*tree)};
    cursor->seek_first();
    bounds->seek(Tools::integral_key(RECORD_COUNT * 3 / 4));
    for (Size i {}; i < RECORD_COUNT * 3 / 4; ++i) {
        ASSERT_TRUE(cursor->is_valid());
        ASSERT_NE(cursor->key(), bounds->key());
        cursor->next();
    }
    ASSERT_EQ(cursor->key(), bounds->key());
}

TEST_P(CursorTests, SeeksForwardBetweenBoundaries)
{
    std::unique_ptr<Cursor> cursor {CursorInternal::make_cursor(*tree)};
    cursor->seek(Tools::integral_key(250));
    std::unique_ptr<Cursor> bounds {CursorInternal::make_cursor(*tree)};
    bounds->seek(Tools::integral_key(750));
    for (Size i {}; i < 500; ++i) {
        ASSERT_TRUE(cursor->is_valid());
        ASSERT_NE(cursor->key(), bounds->key());
        cursor->next();
    }
    ASSERT_EQ(cursor->key(), bounds->key());
}

TEST_P(CursorTests, SeeksBackward)
{
    std::unique_ptr<Cursor> cursor {CursorInternal::make_cursor(*tree)};
    cursor->seek_last();
    for (Size i {}; i < RECORD_COUNT; ++i) {
        ASSERT_TRUE(cursor->is_valid());
        ASSERT_EQ(cursor->key().to_string(), Tools::integral_key(RECORD_COUNT - i - 1));
        ASSERT_EQ(cursor->value().to_string(), make_value('v'));
        cursor->previous();
    }
    ASSERT_FALSE(cursor->is_valid());
}

TEST_P(CursorTests, SeeksBackwardFromBoundary)
{
    std::unique_ptr<Cursor> cursor {CursorInternal::make_cursor(*tree)};
    const auto bounds = RECORD_COUNT * 3 / 4;
    cursor->seek(Tools::integral_key(bounds));
    for (Size i {}; i <= bounds; ++i) {
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
    bounds->seek(Tools::integral_key(RECORD_COUNT / 4));
    for (Size i {}; i < RECORD_COUNT * 3 / 4 - 1; ++i) {
        ASSERT_TRUE(cursor->is_valid());
        ASSERT_NE(cursor->key(), bounds->key());
        cursor->previous();
    }
    ASSERT_EQ(cursor->key(), bounds->key());
}

TEST_P(CursorTests, SeeksBackwardBetweenBoundaries)
{
    std::unique_ptr<Cursor> cursor {CursorInternal::make_cursor(*tree)};
    std::unique_ptr<Cursor> bounds {CursorInternal::make_cursor(*tree)};
    cursor->seek(Tools::integral_key(RECORD_COUNT * 3 / 4));
    bounds->seek(Tools::integral_key(RECORD_COUNT / 4));
    for (Size i {}; i < RECORD_COUNT / 2; ++i) {
        ASSERT_TRUE(cursor->is_valid());
        ASSERT_NE(cursor->key(), bounds->key());
        cursor->previous();
    }
    ASSERT_EQ(cursor->key(), bounds->key());
}

TEST_P(CursorTests, SanityCheck_Forward)
{
    std::unique_ptr<Cursor> cursor {CursorInternal::make_cursor(*tree)};
    for (Size iteration {}; iteration < 100; ++iteration) {
        const auto i = random.Next<Size>(RECORD_COUNT);
        const auto key = Tools::integral_key(i);
        cursor->seek(key);

        ASSERT_TRUE(cursor->is_valid());
        ASSERT_EQ(cursor->key(), key);

        for (Size n {}; n < random.Next<Size>(10); ++n) {
            cursor->next();

            if (const auto j = i + n + 1; j < RECORD_COUNT) {
                ASSERT_TRUE(cursor->is_valid());
                ASSERT_EQ(cursor->key(), Tools::integral_key(j));
            } else {
                ASSERT_FALSE(cursor->is_valid());
            }
        }
    }
}

TEST_P(CursorTests, SanityCheck_Backward)
{
    std::unique_ptr<Cursor> cursor {CursorInternal::make_cursor(*tree)};
    for (Size iteration {}; iteration < 100; ++iteration) {
        const auto i = random.Next<Size>(RECORD_COUNT);
        const auto key = Tools::integral_key(i);
        cursor->seek(key);

        ASSERT_TRUE(cursor->is_valid());
        ASSERT_EQ(cursor->key(), key);

        for (Size n {}; n < random.Next<Size>(10); ++n) {
            cursor->previous();

            if (i > n) {
                ASSERT_TRUE(cursor->is_valid());
                ASSERT_EQ(cursor->key(), Tools::integral_key(i - n - 1));
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

class PointerMapTests : public BPlusTreeTests {
public:
    [[nodiscard]] auto map_size() -> Size
    {
        return (pager->page_size() - sizeof(Lsn)) / (sizeof(Byte) + sizeof(Id));
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
//
//TEST_P(PointerMapTests, ReadsAndWritesEntries)
//{
//    std::string buffer(pager->page_size(), '\0');
//    Page map_page {Id {2}, buffer, true};
//    PointerMap map {*pager};
//
//    ASSERT_OK(map.write_entry(Id {3}, PointerMap::Entry {Id {33}, PointerMap::NODE}));
//    ASSERT_OK(map.write_entry(Id {4}, PointerMap::Entry {Id {44}, PointerMap::FREELIST_LINK}));
//    ASSERT_OK(map.write_entry(Id {5}, PointerMap::Entry {Id {55}, PointerMap::OVERFLOW_LINK}));
//
//    PointerMap::Entry entry_1, entry_2, entry_3;
//    ASSERT_OK(map.read_entry(Id {3}, entry_1));
//    ASSERT_OK(map.read_entry(Id {4}, entry_2));
//    ASSERT_OK(map.read_entry(Id {5}, entry_3));
//
//    ASSERT_EQ(entry_1.back_ptr.value, 33);
//    ASSERT_EQ(entry_2.back_ptr.value, 44);
//    ASSERT_EQ(entry_3.back_ptr.value, 55);
//    ASSERT_EQ(entry_1.type, PointerMap::NODE);
//    ASSERT_EQ(entry_2.type, PointerMap::FREELIST_LINK);
//    ASSERT_EQ(entry_3.type, PointerMap::OVERFLOW_LINK);
//}

TEST_P(PointerMapTests, PointerMapCanFitAllPointers)
{
    PointerMap pointers {*pager};

    // PointerMap::find_map() expects the given pointer map page to be allocated already.
    for (Size i {}; i < map_size() * 2; ++i) {
        Page page;
        ASSERT_OK(pager->allocate(page));
        pager->release(std::move(page));
    }

    for (Size i {}; i < map_size() + 10; ++i) {
        if (i != map_size()) {
            const Id id {i + 3};
            const PointerMap::Entry entry {id, PointerMap::NODE};
            ASSERT_OK(pointers.write_entry(id, entry));
        }
    }
    for (Size i {}; i < map_size() + 10; ++i) {
        if (i != map_size()) {
            const Id id {i + 3};
            PointerMap::Entry entry;
            ASSERT_OK(pointers.read_entry(id, entry));
            ASSERT_EQ(entry.back_ptr.value, id.value);
            ASSERT_EQ(entry.type, PointerMap::NODE);
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
    for (Size i {}; i < 1'000'000; ++i) {
        id.value += map_size() + 1;
        ASSERT_EQ(pointers.lookup(id), id);
    }
}

TEST_P(PointerMapTests, FindsCorrectMapPages)
{
    Size counter {};
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
      public testing::Test {
public:
    static constexpr Size FRAME_COUNT {8};

    VacuumTests()
        : meta {PAGE_SIZE}
    {}

    auto SetUp() -> void override
    {
        tree = std::make_unique<BPlusTree>(*pager);
        Node root;
        BPlusTreeInternal internal {*tree};
        ASSERT_OK(internal.allocate_root(root));
        internal.release(std::move(root));
        ASSERT_TRUE(pager->flush({}).is_ok());
        c = tree->TEST_components();
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
        if (c.pointers->lookup(page.id()) == page.id()) {
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

    auto sanity_check(Size lower_bounds, Size record_count, Size max_value_size) const
    {
        std::unordered_map<std::string, std::string> map;
        bool _;

        for (Size iteration {}; iteration < 5; ++iteration) {
            while (map.size() < lower_bounds + record_count) {
                const auto key = random.Generate(16);
                const auto value_size = random.Next<Size>(max_value_size);
                const auto value = random.Generate(value_size);
                ASSERT_OK(tree->insert(key, value, _));
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
                target.value--;
            }

            ASSERT_OK(pager->truncate(target.value));

            auto *cursor = CursorInternal::make_cursor(*tree);
            for (const auto &[key, value]: map) {
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
    BPlusTree::Components c;
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

    ASSERT_OK(c.freelist->push(std::move(node_5.page)));
    ASSERT_OK(c.freelist->push(std::move(node_4.page)));
    ASSERT_OK(c.freelist->push(std::move(node_3.page)));

    PointerMap::Entry entry;
    ASSERT_OK(c.pointers->read_entry(Id {5}, entry));
    ASSERT_EQ(entry.type, PointerMap::FREELIST_LINK);
    ASSERT_EQ(entry.back_ptr, Id {4});

    ASSERT_OK(c.pointers->read_entry(Id {4}, entry));
    ASSERT_EQ(entry.type, PointerMap::FREELIST_LINK);
    ASSERT_EQ(entry.back_ptr, Id {3});

    ASSERT_OK(c.pointers->read_entry(Id {3}, entry));
    ASSERT_EQ(entry.type, PointerMap::FREELIST_LINK);
    ASSERT_EQ(entry.back_ptr, Id::null());
}

TEST_F(VacuumTests, OverflowChainRegistersBackPointers)
{
    // Creates an overflow chain of length 2, rooted at the second cell on the root page.
    std::string overflow_data(PAGE_SIZE * 2, 'x');
    bool _;
    ASSERT_OK(tree->insert("a", "value", _));
    ASSERT_OK(tree->insert("b", overflow_data, _));

    PointerMap::Entry head_entry, tail_entry;
    ASSERT_OK(c.pointers->read_entry(Id {3}, head_entry));
    ASSERT_OK(c.pointers->read_entry(Id {4}, tail_entry));

    ASSERT_TRUE(head_entry.back_ptr.is_root());
    ASSERT_EQ(tail_entry.back_ptr, Id {3});
    ASSERT_EQ(head_entry.type, PointerMap::OVERFLOW_HEAD);
    ASSERT_EQ(tail_entry.type, PointerMap::OVERFLOW_LINK);
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
        ASSERT_OK(c.freelist->push(std::move(page_4)));
        ASSERT_OK(c.freelist->push(std::move(node_3.page)));
    }

    bool _;
    ASSERT_OK(tree->insert("a", "value", _));
    ASSERT_OK(tree->insert("b", std::string(PAGE_SIZE * 2, 'x'), _));

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
    ASSERT_OK(c.freelist->push(std::move(node_3.page)));
    ASSERT_OK(c.freelist->push(std::move(node_4.page)));
    ASSERT_OK(c.freelist->push(std::move(node_5.page)));

    // Page Types:     N   P   2   1
    // Page Contents: [1] [2] [3] [4] [X]
    // Page IDs:       1   2   3   4   5
    bool vacuumed {};
    ASSERT_OK(tree->vacuum_one(Id {5}, vacuumed));
    ASSERT_TRUE(vacuumed);

    PointerMap::Entry entry;
    ASSERT_OK(c.pointers->read_entry(Id {4}, entry));
    ASSERT_EQ(entry.type, PointerMap::FREELIST_LINK);
    ASSERT_EQ(entry.back_ptr, Id::null());

    // Page Types:     N   P   1
    // Page Contents: [1] [2] [3] [X] [X]
    // Page IDs:       1   2   3   4   5
    ASSERT_OK(tree->vacuum_one(Id {4}, vacuumed));
    ASSERT_TRUE(vacuumed);
    ASSERT_OK(c.pointers->read_entry(Id {3}, entry));
    ASSERT_EQ(entry.type, PointerMap::FREELIST_LINK);
    ASSERT_EQ(entry.back_ptr, Id::null());

    // Page Types:     N   P
    // Page Contents: [1] [2] [X] [X] [X]
    // Page IDs:       1   2   3   4   5
    ASSERT_OK(tree->vacuum_one(Id {3}, vacuumed));
    ASSERT_TRUE(vacuumed);
    ASSERT_TRUE(c.freelist->is_empty());

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
    ASSERT_OK(c.freelist->push(std::move(node_5.page)));
    ASSERT_OK(c.freelist->push(std::move(node_4.page)));
    ASSERT_OK(c.freelist->push(std::move(node_3.page)));

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
    ASSERT_OK(c.pointers->read_entry(Id {4}, entry));
    ASSERT_EQ(entry.back_ptr, Id::null());
    ASSERT_EQ(entry.type, PointerMap::FREELIST_LINK);
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
    ASSERT_OK(c.pointers->read_entry(Id {3}, entry));
    ASSERT_EQ(entry.type, PointerMap::FREELIST_LINK);
    ASSERT_EQ(entry.back_ptr, Id::null());

    // Page Types:     N   P
    // Page Contents: [a] [b] [ ] [ ] [ ]
    // Page IDs:       1   2   3   4   5
    ASSERT_OK(tree->vacuum_one(Id {3}, vacuumed));
    ASSERT_TRUE(vacuumed);
    ASSERT_TRUE(c.freelist->is_empty());

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

TEST_F(VacuumTests, VacuumFreelistSanityCheck)
{
    std::default_random_engine rng {42};

    for (Size iteration {}; iteration < 1'000; ++iteration) {
        std::vector<Node> nodes;
        for (Size i {}; i < FRAME_COUNT - 1; ++i) {
            nodes.emplace_back(allocate_node(true));
        }

        std::shuffle(begin(nodes), end(nodes), rng);

        for (auto &node: nodes) {
            ASSERT_OK(c.freelist->push(std::move(node.page)));
        }

        // This will vacuum the whole freelist, as well as the pointer map page on page 2.
        Id target {pager->page_count()};
        bool vacuumed {};
        for (Size i {}; i < FRAME_COUNT; ++i) {
            ASSERT_OK(tree->vacuum_one(target, vacuumed));
            ASSERT_TRUE(vacuumed);
            target.value--;
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
    bool _;
    ASSERT_OK(tree->insert("a", "value", _));
    ASSERT_OK(tree->insert("b", overflow_data, _));

    ASSERT_OK(c.freelist->push(std::move(node_3.page)));
    ASSERT_OK(c.freelist->push(std::move(node_4.page)));

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
    ASSERT_OK(c.pointers->read_entry(Id {3}, head_entry));
    ASSERT_OK(c.pointers->read_entry(Id {4}, tail_entry));

    ASSERT_TRUE(head_entry.back_ptr.is_root());
    ASSERT_EQ(tail_entry.back_ptr, Id {3});
    ASSERT_EQ(head_entry.type, PointerMap::OVERFLOW_HEAD);
    ASSERT_EQ(tail_entry.type, PointerMap::OVERFLOW_LINK);
}

TEST_F(VacuumTests, VacuumsOverflowChain_B)
{
    // This time, we'll force the head of the overflow chain to be the last page in the file.
    auto node_3 = allocate_node(true);
    auto node_4 = allocate_node(true);
    auto node_5 = allocate_node(true);
    auto node_6 = allocate_node(true);
    ASSERT_EQ(node_6.page.id().value, 6);
    ASSERT_OK(c.freelist->push(std::move(node_5.page)));
    ASSERT_OK(c.freelist->push(std::move(node_6.page)));

    std::string overflow_data(PAGE_SIZE * 2, 'x');
    bool _;
    ASSERT_OK(tree->insert("a", "value", _));
    ASSERT_OK(tree->insert("b", overflow_data, _));

    // Page Types:     n   p   2   1   B   A
    // Page Contents: [a] [b] [c] [d] [e] [f]
    // Page IDs:       1   2   3   4   5   6
    ASSERT_OK(c.freelist->push(std::move(node_3.page)));
    ASSERT_OK(c.freelist->push(std::move(node_4.page)));

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
    ASSERT_OK(c.pointers->read_entry(Id {4}, head_entry));
    ASSERT_OK(c.pointers->read_entry(Id {3}, tail_entry));

    ASSERT_TRUE(head_entry.back_ptr.is_root());
    ASSERT_EQ(tail_entry.back_ptr, Id {4});
    ASSERT_EQ(head_entry.type, PointerMap::OVERFLOW_HEAD);
    ASSERT_EQ(tail_entry.type, PointerMap::OVERFLOW_LINK);
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

    bool _;
    // Create overflow chains, but don't overflow the root node. Should create 3 chains, 1 of length 1, and 2 of length 2.
    std::vector<std::string> values;
    for (Size i {}; i < 3; ++i) {
        const auto value = random.Generate(PAGE_SIZE * std::min<Size>(i + 1, 2));
        ASSERT_OK(tree->insert(Tools::integral_key<1>(i), value, _));
        values.emplace_back(value.to_string());
    }

    while (!reserved.empty()) {
        ASSERT_OK(c.freelist->push(std::move(reserved.back().page)));
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
    for (Size i {}; i < values.size(); ++i) {
        ASSERT_TRUE(cursor->is_valid());
        ASSERT_EQ(cursor->key(), Tools::integral_key<1>(i));
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
    for (Size i {}; i < 5; ++i) {
        const auto key = Tools::integral_key(i);
        const auto value = random.Generate(meta(true).max_local - key.size());
        bool _;
        ASSERT_OK(tree->insert(key, value, _));
        values.emplace_back(value.to_string());
    }

    // Page Types:     n   p   2   1   n   n
    // Page Contents: [a] [b] [c] [d] [e] [f]
    // Page IDs:       1   2   3   4   5   6
    ASSERT_OK(c.freelist->push(std::move(node_3.page)));
    ASSERT_OK(c.freelist->push(std::move(node_4.page)));

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
    for (Size i {}; i < values.size(); ++i) {
        ASSERT_TRUE(cursor->is_valid());
        ASSERT_EQ(cursor->key(), Tools::integral_key(i));
        ASSERT_EQ(cursor->value(), values[i]);
        cursor->next();
    }
    ASSERT_FALSE(cursor->is_valid());
    delete cursor;
}

TEST_F(VacuumTests, SanityCheck_Freelist)
{
    sanity_check(0, 50, 10);
}

TEST_F(VacuumTests, SanityCheck_Freelist_OverflowHead)
{
    sanity_check(0, 50, PAGE_SIZE / 2);
}

TEST_F(VacuumTests, SanityCheck_Freelist_OverflowLink)
{
    sanity_check(0, 50, PAGE_SIZE * 2);
}

TEST_F(VacuumTests, SanityCheck_Nodes_1)
{
    sanity_check(50, 50, 10);
}

TEST_F(VacuumTests, SanityCheck_Nodes_2)
{
    sanity_check(200, 50, 10);
}

TEST_F(VacuumTests, SanityCheck_Nodes_Overflow_1)
{
    sanity_check(50, 50, PAGE_SIZE * 2);
}

TEST_F(VacuumTests, SanityCheck_Nodes_Overflow_2)
{
    sanity_check(200, 50, PAGE_SIZE * 2);
}

} // namespace Calico