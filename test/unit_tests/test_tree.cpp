#include "pager/pager.h"
#include "tree/cursor_internal.h"
#include "tree/node.h"
#include "tree/memory.h"
#include "tree/tree.h"
#include "unit_tests.h"
#include "wal/helpers.h"
#include <gtest/gtest.h>

namespace Calico {

class HeaderTests: public testing::Test {
protected:
    HeaderTests()
        : backing(0x200, '\x00'),
          page {Page {Id::root(), backing, true}}
    {}

    std::string backing;
    Page page;
};

TEST_F(HeaderTests, FileHeader)
{
    FileHeader source;
    source.magic_code = 1;
    source.page_count = 3;
    source.record_count = 4;
    source.free_list_id.value = 5;
    source.recovery_lsn.value = 6;
    source.page_size = backing.size();
    source.header_crc = source.compute_crc();

    source.write(page);
    // Write a node header to make sure it doesn't overwrite the file header memory.
    NodeHeader unused;
    unused.write(page);
    FileHeader target {page};

    ASSERT_EQ(source.magic_code, target.magic_code);
    ASSERT_EQ(source.header_crc, target.header_crc);
    ASSERT_EQ(source.page_count, target.page_count);
    ASSERT_EQ(source.record_count, target.record_count);
    ASSERT_EQ(source.free_list_id, target.free_list_id);
    ASSERT_EQ(source.recovery_lsn, target.recovery_lsn);
    ASSERT_EQ(source.page_size, target.page_size);
    ASSERT_EQ(source.compute_crc(), target.header_crc);
}

TEST_F(HeaderTests, NodeHeader)
{
    NodeHeader source;
    source.page_lsn.value = 1;
    source.parent_id.value = 2;
    source.next_id.value = 3;
    source.prev_id.value = 4;
    source.cell_count = 5;
    source.cell_start = 6;
    source.frag_count = 7;
    source.free_start = 8;
    source.free_total = 9;
    source.is_external = false;

    source.write(page);
    FileHeader unused;
    unused.write(page);
    NodeHeader target {page};

    ASSERT_EQ(source.page_lsn, target.page_lsn);
    ASSERT_EQ(source.parent_id, target.parent_id);
    ASSERT_EQ(source.next_id, target.next_id);
    ASSERT_EQ(source.prev_id, target.prev_id);
    ASSERT_EQ(source.cell_count, target.cell_count);
    ASSERT_EQ(source.cell_start, target.cell_start);
    ASSERT_EQ(source.frag_count, target.frag_count);
    ASSERT_EQ(source.free_start, target.free_start);
    ASSERT_EQ(source.free_total, target.free_total);
    ASSERT_EQ(source.is_external, target.is_external);
}

class NodeMetaManager {
    NodeMeta m_external_meta;
    NodeMeta m_internal_meta;

public:
    explicit NodeMetaManager(Size page_size)
    {
        // min_local and max_local fields are only needed in external nodes.
        m_external_meta.min_local = compute_min_local(page_size);
        m_external_meta.max_local = compute_max_local(page_size);

        m_external_meta.cell_size = external_cell_size;
        m_external_meta.read_key = read_external_key;
        m_external_meta.parse_cell = parse_external_cell;

        m_internal_meta.cell_size = internal_cell_size;
        m_internal_meta.read_key = read_internal_key;
        m_internal_meta.parse_cell = parse_internal_cell;
    }

    [[nodiscard]]
    auto operator()(bool is_external) const -> const NodeMeta &
    {
        return is_external ? m_external_meta : m_internal_meta;
    }
};

TEST(NodeSlotTests, SlotsAreConsistent)
{
    std::string backing(0x200, '\x00');
    std::string scratch(0x200, '\x00');
    Page page {Id::root(), backing, true};
    Node node {std::move(page), scratch.data()};
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

static constexpr Size SMALL_PAGE_SIZE {0x200};
static constexpr Size MEDIUM_PAGE_SIZE {0x1000};
static constexpr Size LARGE_PAGE_SIZE {0x8000};
static constexpr Id ROOT_PID {1};
static constexpr Id NON_ROOT_PID {2};

struct ExternalNodeTestParameters {
    Id pid;
    Size page_size {};
};

/*
 * Make sure we can create new cells and write them to external nodes.
 */
class ExternalNodeTests: public testing::TestWithParam<ExternalNodeTestParameters> {
protected:
    ExternalNodeTests()
        : param {GetParam()},
          backing(param.page_size, '\x00'),
          scratch1(param.page_size, '\x00'),
          scratch2(param.page_size, '\x00'),
          meta {param.page_size},
          node {Page {param.pid, backing, true}, scratch1.data()}
    {
        node.meta = &meta(true);
    }

    ~ExternalNodeTests() override
    {
        node.TEST_validate();
        auto page = std::move(node).take();
        (void)page.deltas();
    }

    auto create_cell(const Slice &key, const Slice &value, Id overflow_id = Id {123})
    {
        auto value_size = value.size();
        const auto cell_size = determine_cell_size(key.size(), value_size, *node.meta);
        const auto needs_overflow_id = value.size() != value_size;
        emplace_cell(scratch2.data(), value.size(), key, value.range(0, value_size), Id {needs_overflow_id * overflow_id.value});
        auto cell = node.meta->parse_cell(*node.meta, scratch2.data());
        EXPECT_EQ(cell.size, cell_size);
        return std::make_tuple(cell, value_size);
    }

    auto node_emplace_cell(Size index, const Slice &key, const Slice &value, Id overflow_id = Id {123})
    {
        auto value_size = value.size();
        const auto cell_size = determine_cell_size(key.size(), value_size, *node.meta);
        const auto needs_overflow_id = value.size() != value_size;
        auto *ptr = scratch2.data();

        if (const auto offset = allocate_block(node, index, cell_size)) {
            ptr = node.page.data() + offset;
        }
        emplace_cell(ptr, value.size(), key, value.range(0, value_size), Id {needs_overflow_id * overflow_id.value});
    }

    auto simulate_write(const Slice &key, const Slice &value, Id overflow_id = Id {123})
    {
        const auto [cell, _] = create_cell(key, value, overflow_id);
        Node::Iterator itr {node};
        if (itr.seek(key)) {
            erase_cell(node, itr.index());
        }
        write_cell(node, itr.index(), cell);
    }

    ExternalNodeTestParameters param;
    std::string backing;
    std::string scratch1;
    std::string scratch2;
    NodeMetaManager meta;
    Node node;
};

TEST_P(ExternalNodeTests, ConstructsAndDestructs)
{}

TEST_P(ExternalNodeTests, CreatesCell)
{
    const Slice key {"hello"};
    const Slice value {"world"};

    const auto [cell, value_size] = create_cell(key, value);

    ASSERT_EQ(value_size, value.size());
    ASSERT_EQ(cell.key_size, key.size());
    ASSERT_EQ(cell.total_ps, key.size() + value.size());
    ASSERT_EQ(cell.local_ps, cell.total_ps);
    ASSERT_EQ(cell.size, 6 + cell.local_ps);
}

TEST_P(ExternalNodeTests, CreatesCellWithLargeValue)
{
    const auto min_local = meta(true).min_local;

    const Slice key {"hello"};
    const std::string value_buffer(param.page_size, 'x');
    const Slice value {value_buffer};

    const auto [cell, value_size] = create_cell(key, value);

    ASSERT_LT(value_size, value.size());
    ASSERT_EQ(key.size() + value_size, min_local);
    ASSERT_EQ(cell.key_size, key.size());
    ASSERT_EQ(cell.total_ps, key.size() + value_buffer.size());
    ASSERT_EQ(cell.local_ps, min_local);
    ASSERT_EQ(cell.size, 6 + cell.local_ps + sizeof(Id));
    ASSERT_EQ(get_u64(cell.key + cell.local_ps), 123);
}

TEST_P(ExternalNodeTests, CreatesCellWithLargeKey)
{
    const auto max_local = meta(true).max_local;

    const std::string key_buffer(max_local, 'x');
    const Slice key {key_buffer};
    const Slice value {"world"};

    const auto [cell, value_size] = create_cell(key, value);

    ASSERT_EQ(value_size, 0);
    ASSERT_EQ(cell.key_size, key.size());
    ASSERT_EQ(cell.total_ps, key.size() + 5);
    ASSERT_EQ(cell.local_ps, key.size());
    ASSERT_EQ(cell.size, 6 + cell.local_ps + sizeof(Id));
    ASSERT_EQ(get_u64(cell.key + cell.local_ps), 123);
}

TEST_P(ExternalNodeTests, CreatesCellWithLargePayload)
{
    const auto min_local = meta(true).min_local;
    const auto max_local = meta(true).max_local;
    const auto diff = 10;

    const std::string key_buffer(min_local - diff, 'x');
    const std::string value_buffer(max_local - diff, 'x');
    const Slice key {key_buffer};
    const Slice value {value_buffer};

    const auto [cell, value_size] = create_cell(key, value);

    ASSERT_EQ(key.size() + value_size, min_local);
    ASSERT_EQ(cell.key_size, key.size());
    ASSERT_EQ(cell.total_ps, key.size() + value.size());
    ASSERT_EQ(cell.local_ps, min_local);
    ASSERT_EQ(cell.size, 6 + cell.local_ps + sizeof(Id));
    ASSERT_EQ(get_u64(cell.key + cell.local_ps), 123);
}

TEST_P(ExternalNodeTests, EmplacesCells)
{
    node_emplace_cell(0, "a", "1");
    node_emplace_cell(1, "b", "2");
    node_emplace_cell(2, "c", "3");
    ASSERT_EQ(node.header.cell_count, 3);

    auto cell = read_cell(node, 0);
    ASSERT_EQ((Slice {cell.key, cell.local_ps}), "a1");

    cell = read_cell(node, 1);
    ASSERT_EQ((Slice {cell.key, cell.local_ps}), "b2");

    cell = read_cell(node, 2);
    ASSERT_EQ((Slice {cell.key, cell.local_ps}), "c3");
}

TEST_P(ExternalNodeTests, ErasesCells)
{
    node_emplace_cell(0, "a", "1");
    node_emplace_cell(1, "b", "2");
    node_emplace_cell(2, "c", "3");
    node_emplace_cell(3, "d", "4");
    erase_cell(node, 3);
    erase_cell(node, 1);
    erase_cell(node, 0);
    erase_cell(node, 0);
    ASSERT_EQ(node.header.cell_count, 0);
}

TEST_P(ExternalNodeTests, DefragmentationPreservesUsableSpace)
{
    node_emplace_cell(0, "a", "1");
    node_emplace_cell(1, "c", "3");
    const auto target_space = usable_space(node);
    node_emplace_cell(2, "b", "2");
    node_emplace_cell(3, "d", "4");
    ASSERT_LT(usable_space(node), target_space);
    erase_cell(node, 3);
    erase_cell(node, 2);

    ASSERT_EQ(usable_space(node), target_space);
    manual_defragment(node);
    ASSERT_EQ(usable_space(node), target_space);
    ASSERT_EQ(node.header.cell_count, 2);
}

TEST_P(ExternalNodeTests, Iteration)
{
    // a, b, c, d, e, f, g
    for (Size i {}; i < 7; ++i) {
        std::string k(1, 'a' + i);
        node_emplace_cell(i, k, "");
    }
    Node::Iterator itr {node};

    ASSERT_TRUE(itr.is_valid());
    ASSERT_EQ(itr.key(), "a");
    ASSERT_EQ(itr.index(), 0);

    ASSERT_TRUE(itr.seek("c"));
    ASSERT_TRUE(itr.is_valid());
    ASSERT_EQ(itr.key(), "c");
    ASSERT_EQ(itr.index(), 2);

    ASSERT_TRUE(itr.seek("f"));
    ASSERT_TRUE(itr.is_valid());
    ASSERT_EQ(itr.key(), "f");
    ASSERT_EQ(itr.index(), 5);

    itr.next();
    ASSERT_TRUE(itr.is_valid());
    ASSERT_EQ(itr.key(), "g");
    ASSERT_EQ(itr.index(), 6);

    itr.next();
    ASSERT_FALSE(itr.is_valid());
    ASSERT_EQ(itr.index(), 7);
}

TEST_P(ExternalNodeTests, WritesCellsInOrder)
{
    simulate_write("b", "2");
    simulate_write("c", "3");
    simulate_write("a", "1");
    ASSERT_EQ(read_key(node, 0), "a");
    ASSERT_EQ(read_key(node, 1), "b");
    ASSERT_EQ(read_key(node, 2), "c");
}

TEST_P(ExternalNodeTests, HandlesOverflowIds)
{
    simulate_write("a", std::string(node.page.size(), '1'), Id {111});
    simulate_write("b", std::string(node.page.size(), '2'), Id {222});
    simulate_write("c", std::string(node.page.size(), '3'), Id {333});

    const auto cell1 = read_cell(node, 0);
    const auto cell2 = read_cell(node, 1);
    const auto cell3 = read_cell(node, 2);
    ASSERT_EQ(read_key(cell1), "a");
    ASSERT_EQ(read_key(cell2), "b");
    ASSERT_EQ(read_key(cell3), "c");
    ASSERT_EQ((Slice {cell1.key + 1, cell1.local_ps - 1}), std::string(cell1.local_ps - 1, '1'));
    ASSERT_EQ((Slice {cell2.key + 1, cell2.local_ps - 1}), std::string(cell2.local_ps - 1, '2'));
    ASSERT_EQ((Slice {cell3.key + 1, cell3.local_ps - 1}), std::string(cell3.local_ps - 1, '3'));
    ASSERT_EQ(get_u64(cell1.key + cell1.local_ps), 111);
    ASSERT_EQ(get_u64(cell2.key + cell2.local_ps), 222);
    ASSERT_EQ(get_u64(cell3.key + cell3.local_ps), 333);
}

TEST_P(ExternalNodeTests, DefragmentsToMakeRoomForCellBody)
{
    simulate_write("\x01", "1");
    simulate_write("\x02", "2");

    Size i {};
    while (!node.overflow.has_value()) {
        simulate_write(Tools::integral_key<4>(i++), "");
    }
    std::exchange(node.overflow, std::nullopt);

    erase_cell(node, 0);
    erase_cell(node, 1);
    node.TEST_validate();

    ASSERT_NE(usable_space(node), node.gap_size);
    // This cell will be too big to fit either in the gap space or any available free block.
    simulate_write("abcdef", "123456");

    ASSERT_FALSE(node.overflow.has_value());
    ASSERT_EQ(usable_space(node), node.gap_size);
}

TEST_P(ExternalNodeTests, SanityCheck)
{
    Tools::RandomGenerator random { 1'024 * 1'024 * 4};
    for (int iteration {}; iteration < 10; ++iteration) {
        while (!node.overflow.has_value()) {
            const auto key = random.Generate(12);
            const auto val = random.Generate(param.page_size / 10);
            simulate_write(key, val);
            node.TEST_validate();
        }
        std::exchange(node.overflow, std::nullopt);

        while (node.header.cell_count) {
            erase_cell(node, random.Next<Size>(node.header.cell_count - 1));
            node.TEST_validate();
        }
    }
}

INSTANTIATE_TEST_SUITE_P(
    ExternalNodeTests,
    ExternalNodeTests,
    ::testing::Values(
        ExternalNodeTestParameters {ROOT_PID, SMALL_PAGE_SIZE},
        ExternalNodeTestParameters {ROOT_PID, MEDIUM_PAGE_SIZE},
        ExternalNodeTestParameters {ROOT_PID, LARGE_PAGE_SIZE},
        ExternalNodeTestParameters {NON_ROOT_PID, SMALL_PAGE_SIZE},
        ExternalNodeTestParameters {NON_ROOT_PID, MEDIUM_PAGE_SIZE},
        ExternalNodeTestParameters {NON_ROOT_PID, LARGE_PAGE_SIZE}));

TEST(MaximumKeySizeTest, SizeTableIsCorrect)
{
    ASSERT_EQ(101, compute_max_local(MINIMUM_PAGE_SIZE));
    ASSERT_EQ(229, compute_max_local(MINIMUM_PAGE_SIZE << 1));
    ASSERT_EQ(485, compute_max_local(MINIMUM_PAGE_SIZE << 2));
    ASSERT_EQ(997, compute_max_local(MINIMUM_PAGE_SIZE << 3));
    ASSERT_EQ(2021, compute_max_local(MINIMUM_PAGE_SIZE << 4));
    ASSERT_EQ(4069, compute_max_local(MINIMUM_PAGE_SIZE << 5));
    ASSERT_EQ(8165, compute_max_local(MINIMUM_PAGE_SIZE << 6));
    ASSERT_EQ(MAXIMUM_PAGE_SIZE, MINIMUM_PAGE_SIZE << 6);
}

struct CellConversionTestParameters {
    bool is_src_external {};
    bool is_dst_external {};
    Size page_size {};
};

class CellConversionTests: public testing::TestWithParam<CellConversionTestParameters> {
protected:
    CellConversionTests()
        : param {GetParam()},
          backing(param.page_size, '\x00'),
          scratch1(param.page_size, '\x00'),
          scratch2(param.page_size, '\x00'),
          meta {param.page_size},
          dst_node {Page {Id {2}, backing, true}, scratch2.data()}
    {
        dst_node.header.is_external = param.is_dst_external;
        dst_node.meta = &meta(param.is_dst_external);
    }

    ~CellConversionTests() override
    {
        dst_node.TEST_validate();
        auto page = std::move(dst_node).take();
        (void)page.deltas();
    }

    auto SetUp() -> void override
    {
        const Slice key {"hello"};
        const Slice value {"world"};

        if (param.is_src_external) {
            auto value_size = value.size();
            const auto cell_size = determine_cell_size(key.size(), value_size, meta(param.is_src_external));
            const auto is_overflowing = Size(value.size() != value_size);
            emplace_cell(scratch1.data() + 4, value.size(), key, value.range(0, value_size), Id {123 * is_overflowing});
            cell = meta(true).parse_cell(meta(true), scratch1.data() + 4);
            EXPECT_EQ(cell.size, cell_size);

        } else {
            const auto cell_size = key.size() + sizeof(Id) + 2;
            put_u64(scratch1.data(), 123);
            put_u16(scratch1.data() + 8, key.size());
            std::memcpy(scratch1.data() + 10, key.data(), key.size());
            cell = meta(false).parse_cell(meta(false), scratch1.data());
            EXPECT_EQ(cell.size, cell_size);
        }

    }

    CellConversionTestParameters param;
    std::string backing;
    std::string scratch1;
    std::string scratch2;
    NodeMetaManager meta;
    Node dst_node;
    Cell cell;
};

TEST_P(CellConversionTests, WritesAndReadsBack)
{
    if (param.is_src_external != param.is_dst_external) {
        CALICO_EXPECT_FALSE(param.is_dst_external);
        promote_cell(cell);
    }

    write_cell(dst_node, 0, cell);
    ASSERT_EQ(dst_node.header.cell_count, 1);
    const auto out = read_cell(dst_node, 0);

    if (param.is_dst_external) {
        ASSERT_EQ((Slice {out.key, cell.local_ps}), "helloworld");
    } else {
        ASSERT_EQ((Slice {out.key, cell.local_ps}), "hello");
    }
}

INSTANTIATE_TEST_SUITE_P(
    CellConversionTests,
    CellConversionTests,
    ::testing::Values(
        // Possible transfers between nodes of the same type.
        CellConversionTestParameters {true, true, SMALL_PAGE_SIZE},
        CellConversionTestParameters {true, true, MEDIUM_PAGE_SIZE},
        CellConversionTestParameters {true, true, LARGE_PAGE_SIZE},
        CellConversionTestParameters {false, false, SMALL_PAGE_SIZE},
        CellConversionTestParameters {false, false, MEDIUM_PAGE_SIZE},
        CellConversionTestParameters {false, false, LARGE_PAGE_SIZE},

        // Possible transfers between nodes of different types (only external to internal is needed).
        CellConversionTestParameters {true, false, SMALL_PAGE_SIZE},
        CellConversionTestParameters {true, false, MEDIUM_PAGE_SIZE},
        CellConversionTestParameters {true, false, LARGE_PAGE_SIZE}));

struct BPlusTreeTestParameters {
    Size page_size {};
};

class BPlusTreeTests : public ParameterizedInMemoryTest<BPlusTreeTestParameters> {
public:
    BPlusTreeTests()
        : param {GetParam()},
          scratch(param.page_size, '\x00'),
          log_scratch(wal_scratch_size(param.page_size), '\x00')
    {}

    auto SetUp() -> void override
    {
        auto r = Pager::open({
            PREFIX,
            storage.get(),
            &log_scratch,
            &wal,
            nullptr,
            &status,
            &commit_lsn,
            &in_xact,
            8,
            param.page_size,
        });
        EXPECT_TRUE(r.has_value()) << r.error().what().data();
        pager = std::move(*r);
        tree = std::make_unique<BPlusTree>(*pager);

        // Root page setup.
        auto root = tree->setup();
        pager->release(std::move(*root).take());
        EXPECT_TRUE(pager->flush({}).is_ok());
    }

    auto TearDown() -> void override
    {
        validate();
    }

    [[nodiscard]]
    auto make_value(char c, bool overflow = false) const
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
        return Node {*pager->acquire(pid), scratch.data()};
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
    std::string log_scratch;
    Status status;
    bool in_xact {true};
    Lsn commit_lsn;
    DisabledWriteAheadLog wal;
    std::string scratch;
    std::unique_ptr<Pager> pager;
    std::unique_ptr<BPlusTree> tree;
    Tools::RandomGenerator random {1'024 * 1'024 * 8};
};

TEST_P(BPlusTreeTests, ConstructsAndDestructs)
{
    validate();
}

TEST_P(BPlusTreeTests, InsertsRecords)
{
    ASSERT_TRUE(tree->insert("a", make_value('1')).value());
    ASSERT_TRUE(tree->insert("b", make_value('2')).value());
    ASSERT_TRUE(tree->insert("c", make_value('3')).value());
    validate();
}

TEST_P(BPlusTreeTests, ErasesRecords)
{
    (void)tree->insert("a", make_value('1'));
    (void)tree->insert("b", make_value('2'));
    (void)tree->insert("c", make_value('3'));
    ASSERT_TRUE(tree->erase("a").has_value());
    ASSERT_TRUE(tree->erase("b").has_value());
    ASSERT_TRUE(tree->erase("c").has_value());
    validate();
}

TEST_P(BPlusTreeTests, FindsRecords)
{
    const auto keys = "abc";
    const auto vals = "123";
    (void)tree->insert(std::string(1, keys[0]), make_value(vals[0]));
    (void)tree->insert(std::string(1, keys[1]), make_value(vals[1]));
    (void)tree->insert(std::string(1, keys[2]), make_value(vals[2]));

    for (Size i {}; i < 3; ++i) {
        auto r = tree->search(std::string(1, keys[i])).value();
        ASSERT_EQ(r.index, i);
        const auto cell = read_cell(r.node, r.index);
        ASSERT_EQ(cell.key[0], keys[i]);
        ASSERT_EQ(cell.key[cell.key_size], vals[i]);
    }
}

TEST_P(BPlusTreeTests, CannotFindNonexistentRecords)
{
    auto slot = tree->search("a");
    ASSERT_TRUE(slot.has_value());
    ASSERT_EQ(slot->node.header.cell_count, 0);
    ASSERT_FALSE(slot->exact);
}

TEST_P(BPlusTreeTests, CannotEraseNonexistentRecords)
{
    ASSERT_TRUE(tree->erase("a").error().is_not_found());
}

TEST_P(BPlusTreeTests, WritesOverflowChains)
{
    ASSERT_TRUE(tree->insert("a", make_value('1', true)).value());
    ASSERT_TRUE(tree->insert("b", make_value('2', true)).value());
    ASSERT_TRUE(tree->insert("c", make_value('3', true)).value());
    validate();
}

TEST_P(BPlusTreeTests, ErasesOverflowChains)
{
    (void)tree->insert("a", make_value('1', true)).value();
    (void)tree->insert("b", make_value('2', true)).value();
    (void)tree->insert("c", make_value('3', true)).value();
    ASSERT_TRUE(tree->erase("a").has_value());
    ASSERT_TRUE(tree->erase("b").has_value());
    ASSERT_TRUE(tree->erase("c").has_value());
}

TEST_P(BPlusTreeTests, ReadsOverflowChains)
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

    for (Size i {}; i < 3; ++i) {
        auto r = tree->search(std::string(1, keys[i])).value();
        const auto cell = read_cell(r.node, r.index);
        const auto pid = read_overflow_id(cell);
        const auto local_vs = cell.local_ps - cell.key_size;
        const auto value = tree->collect(std::move(r.node), r.index);
        ASSERT_EQ(value, values[i]);
    }
}

TEST_P(BPlusTreeTests, ResolvesFirstOverflowOnRightmostPosition)
{
    for (Size i {}; is_root_external(); ++i) {
        ASSERT_TRUE(*tree->insert(Tools::integral_key(i), make_value('v')));
        validate();
    }
}

TEST_P(BPlusTreeTests, ResolvesFirstOverflowOnLeftmostPosition)
{
    for (Size i {}; is_root_external(); ++i) {
        ASSERT_LE(i, 100);
        ASSERT_TRUE(*tree->insert(Tools::integral_key(100 - i), make_value('v')));
    }
    validate();
}

TEST_P(BPlusTreeTests, ResolvesFirstOverflowOnMiddlePosition)
{
    for (Size i {}; is_root_external(); ++i) {
        ASSERT_LE(i, 100);
        ASSERT_TRUE(*tree->insert(Tools::integral_key(i & 1 ? 100 - i : i), make_value('v')));
    }
    validate();
}

TEST_P(BPlusTreeTests, ResolvesMultipleOverflowsOnLeftmostPosition)
{
    for (Size i {}; i < 1'000; ++i) {
        ASSERT_TRUE(*tree->insert(Tools::integral_key(999 - i), make_value('v')));
        if (i % 100 == 99) {
            validate();
        }
    }
}

TEST_P(BPlusTreeTests, ResolvesMultipleOverflowsOnRightmostPosition)
{
    for (Size i {}; i < 1'000; ++i) {
        ASSERT_TRUE(*tree->insert(Tools::integral_key(i), make_value('v')));
        if (i % 100 == 99) {
            validate();
        }
    }
}

TEST_P(BPlusTreeTests, ResolvesMultipleOverflowsOnMiddlePosition)
{
    for (Size i {}, j {999}; i < j; ++i, --j) {
        ASSERT_TRUE(*tree->insert(Tools::integral_key(i), make_value('v')));
        ASSERT_TRUE(*tree->insert(Tools::integral_key(j), make_value('v')));
        if (i % 100 == 99) {
            validate();
        }
    }
}

TEST_P(BPlusTreeTests, ResolvesFirstUnderflowOnRightmostPosition)
{
    long i {};
    for (; is_root_external(); ++i) {
        (void)tree->insert(Tools::integral_key(i), make_value('v'));
    }

    while (i--) {
        ASSERT_TRUE(tree->erase(Tools::integral_key(i)).has_value());
        validate();
    }
}

TEST_P(BPlusTreeTests, ResolvesFirstUnderflowOnLeftmostPosition)
{
    Size i {};
    for (; is_root_external(); ++i) {
        (void)tree->insert(Tools::integral_key(i), make_value('v'));
    }
    for (Size j {}; j < i; ++j) {
        ASSERT_TRUE(tree->erase(Tools::integral_key(j)).has_value());
        validate();
    }
}

TEST_P(BPlusTreeTests, ResolvesFirstUnderflowOnMiddlePosition)
{
    Size i {};
    for (; is_root_external(); ++i) {
        (void)tree->insert(Tools::integral_key(i), make_value('v'));
    }
    for (Size j {1}; j < i/2 - 1; ++j) {
        ASSERT_TRUE(tree->erase(Tools::integral_key(i/2 - j + 1)).has_value());
        ASSERT_TRUE(tree->erase(Tools::integral_key(i/2 + j)).has_value());
        validate();
    }
}

static auto insert_1000(BPlusTreeTests &test, bool has_overflow = false)
{
    for (Size i {}; i < 1'000; ++i) {
        (void)*test.tree->insert(Tools::integral_key(i), test.make_value('v', has_overflow));
    }
}

TEST_P(BPlusTreeTests, ResolvesMultipleUnderflowsOnRightmostPosition)
{
    insert_1000(*this);
    for (Size i {}; i < 1'000; ++i) {
        ASSERT_TRUE(tree->erase(Tools::integral_key(999 - i)).has_value());
        if (i % 100 == 99) {
            validate();
        }
    }
}

TEST_P(BPlusTreeTests, ResolvesMultipleUnderflowsOnLeftmostPosition)
{
    insert_1000(*this);
    for (Size i {}; i < 1'000; ++i) {
        ASSERT_TRUE(tree->erase(Tools::integral_key(i)).has_value());
        if (i % 100 == 99) {
            validate();
        }
    }
}

TEST_P(BPlusTreeTests, ResolvesMultipleUnderflowsOnMiddlePosition)
{
    insert_1000(*this);
    for (Size i {}, j {999}; i < j; ++i, --j) {
        ASSERT_TRUE(tree->erase(Tools::integral_key(i)).has_value());
        ASSERT_TRUE(tree->erase(Tools::integral_key(j)).has_value());
        if (i % 100 == 99) {
            validate();
        }
    }
}

TEST_P(BPlusTreeTests, ResolvesOverflowsFromOverwrite)
{
    for (Size i {}; i < 1'000; ++i) {
        ASSERT_TRUE(tree->insert(Tools::integral_key(i), "v").has_value());
    }
    // Replace the small values with very large ones.
    for (Size i {}; i < 1'000; ++i) {
        ASSERT_TRUE(tree->insert(Tools::integral_key(i), make_value('v', true)).has_value());
    }
    validate();
}

TEST_P(BPlusTreeTests, InternalRotationAfterSplitOnRight)
{
    // Populate the internal nodes with small keys.
    for (unsigned i {}; i < 10'000; ++i) {
        char key[3] {};
        put_u16(key, i);
        ASSERT_TRUE(tree->insert({key, 2}, "v").has_value());
    }
    // Overflow with a bunch of large keys.
    for (unsigned i {}; i < 10'000; ++i) {
        ASSERT_TRUE(tree->insert(Tools::integral_key<100>(i), "v").has_value());
    }
    validate();
}

TEST_P(BPlusTreeTests, InternalRotationAfterSplitOnLeft)
{
    for (unsigned i {}; i < 10'000; ++i) {
        char key[3] {};
        put_u16(key, 9'999 - i);
        ASSERT_TRUE(tree->insert({key, 2}, "v").has_value());
    }
    for (unsigned i {}; i < 10'000; ++i) {
        ASSERT_TRUE(tree->insert(Tools::integral_key<100>(9'999 - i), "v").has_value());
    }
    validate();
}

TEST_P(BPlusTreeTests, InternalRotationAfterSplitOnMiddle)
{
//    for (unsigned i {}; i < 10'000; ++i) {
//        char key[3] {};
//        put_u16(key, 9'999 - i);
//        ASSERT_TRUE(tree->insert({key, 2}, "v").has_value());
//    }
    for (Size i {}, j {9'999}; i < j; ++i, --j) {
        ASSERT_TRUE(tree->insert(Tools::integral_key<100>(i), "v").has_value());
        ASSERT_TRUE(tree->insert(Tools::integral_key<100>(j), "v").has_value());
        if (i % 1'000 == 99) {
            validate();
        }
    }
}

static auto random_key(BPlusTreeTests &test)
{
    const auto key_size = test.random.Next<Size>(1, 10);
    return test.random.Generate(key_size);
}

static auto random_value(BPlusTreeTests &test)
{
    const auto val_size = test.random.Next<Size>(test.param.page_size / 2);
    return test.random.Generate(val_size);
}

static auto random_write(BPlusTreeTests &test)
{
    const auto key = random_key(test);
    const auto val = random_value(test);
    (void)test.tree->insert(key, val);
}

static auto find_random_key(BPlusTreeTests &test)
{
    auto slot = test.tree->search(random_key(test));
    EXPECT_TRUE(slot.has_value());

    auto [node, index, exact] = std::move(*slot);
    EXPECT_LE(index, node.header.cell_count);
    index -= index == node.header.cell_count;

    auto key = read_key(node, index).to_string();
    test.pager->release(std::move(node).take());
    return key;
}

TEST_P(BPlusTreeTests, SanityCheck_Insert)
{
    for (Size i {}; i < 1'000; ++i) {
        random_write(*this);
        if (i % 100 == 99) {
            validate();
        }
    }
}

TEST_P(BPlusTreeTests, SanityCheck_Search)
{
    std::vector<Size> integers(1'000);
    std::iota(begin(integers), end(integers), 0);
    for (auto i: integers) {
        const auto key = Tools::integral_key<6>(i);
        ASSERT_TRUE(tree->insert(key, key).value());
    }
    std::default_random_engine rng {42};
    std::shuffle(begin(integers), end(integers), rng);

    for (auto i: integers) {
        const auto key = Tools::integral_key<6>(i);
        auto slot = tree->search(key);
        ASSERT_TRUE(slot.has_value());
        ASSERT_TRUE(slot->exact);
        const auto cell = read_cell(slot->node, slot->index);
        const Slice payload {cell.key, cell.local_ps};
        ASSERT_EQ(payload, key + key);
        release_node(std::move(slot->node));
    }
}

TEST_P(BPlusTreeTests, SanityCheck_Erase)
{
    Size counter {};
    for (Size i {}; i < 1'000; ++i) {
        if (counter < 500) {
            while (counter < 1'000) {
                random_write(*this);
                counter++;
            }
        }

        const auto key = find_random_key(*this);
        ASSERT_TRUE(tree->erase(key).has_value());
        counter--;

//        if (i % 100 == 99) {
//        }
        validate();
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

class CursorTests: public BPlusTreeTests {
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
    for (Size i {}; i < RECORD_COUNT*3/4 - 1; ++i) {
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

INSTANTIATE_TEST_SUITE_P(
    CursorTests,
    CursorTests,
    ::testing::Values(
        BPlusTreeTestParameters {MINIMUM_PAGE_SIZE},
        BPlusTreeTestParameters {MINIMUM_PAGE_SIZE * 2},
        BPlusTreeTestParameters {MAXIMUM_PAGE_SIZE / 2},
        BPlusTreeTestParameters {MAXIMUM_PAGE_SIZE}));

class PointerMapTests: public testing::TestWithParam<Size> {
public:
    [[nodiscard]]
    static auto map_size() -> Size
    {
        return (GetParam()-sizeof(Lsn)) / (sizeof(Byte)+sizeof(Id));
    }

    PointerMap map {GetParam()};
};

TEST_P(PointerMapTests, FirstPointerMapIsPage2)
{
    ASSERT_EQ(map.lookup_map(Id {0}), Id {0});
    ASSERT_EQ(map.lookup_map(Id {1}), Id {0});
    ASSERT_EQ(map.lookup_map(Id {2}), Id {2});
    ASSERT_EQ(map.lookup_map(Id {3}), Id {2});
    ASSERT_EQ(map.lookup_map(Id {4}), Id {2});
    ASSERT_EQ(map.lookup_map(Id {5}), Id {2});
}

TEST_P(PointerMapTests, ReadsAndWritesEntries)
{
    std::string buffer(GetParam(), '\0');
    Page map_page {Id {2}, buffer, true};

    ASSERT_EQ(map.lookup_map(Id {0}), Id {0});
    ASSERT_EQ(map.lookup_map(Id {1}), Id {0});
    ASSERT_EQ(map.lookup_map(Id {2}), Id {2});
    ASSERT_EQ(map.lookup_map(Id {3}), Id {2});
    ASSERT_EQ(map.lookup_map(Id {4}), Id {2});
    ASSERT_EQ(map.lookup_map(Id {5}), Id {2});
}

TEST_P(PointerMapTests, PointerMapCanFitAllPointers)
{
    std::string buffer(GetParam() + 8, '\0');
    auto backing = Span {buffer}.truncate(GetParam());
    Page map_page {Id {2}, backing, true};

    for (Size i {}; i < map_size(); ++i) {
        const Id id {i + 3};
        auto map_id = map.lookup_map(id);
        ASSERT_EQ(map_id.value, 2);
        const PointerMap::Entry entry {id, PointerMap::NODE};
        map.write_entry(nullptr, map_page, id, entry);
    }
    for (Size i {}; i < map_size(); ++i) {
        const Id id {i + 3};
        auto map_id = map.lookup_map(id);
        ASSERT_EQ(map_id.value, 2);
        const auto entry = map.read_entry(map_page, id);
        ASSERT_EQ(entry.back_ptr.value, id.value);
        ASSERT_EQ(entry.type, PointerMap::NODE);
    }
    const auto result = Slice{buffer}.advance(GetParam());
    const Slice blank {"\0\0\0\0\0\0\0\0", 8};
    ASSERT_EQ(blank, result);
}

TEST_P(PointerMapTests, MapPagesAreRecognized)
{
    Id id {2};
    ASSERT_EQ(map.lookup_map(id), id);

    // Back pointers for the next "map.map_size()" pages are stored on page 2. The next pointermap page is
    // the page following the last page whose back pointer is on page 2. This pattern continues forever.
    for (Size i {}; i < 1'000'000; ++i) {
        id.value += map_size() + 1;
        ASSERT_EQ(map.lookup_map(id), id);
    }
}

TEST_P(PointerMapTests, FindsCorrectMapPages)
{
    Size counter {};
    Id map_id {2};

    for (Id pid {3}; pid.value <= 100 * map_size(); ++pid.value) {
        if (counter++ == map_size()) {
            // Found a map page. Calls to find() with a page ID between this page and the next map page
            // should map to this page ID.
            map_id.value += map_size() + 1;
            counter = 0;
        } else {
            ASSERT_EQ(map.lookup_map(pid), map_id);
        }
    }
}

INSTANTIATE_TEST_SUITE_P(
    PointerMapTests,
    PointerMapTests,
    ::testing::Values(
        MINIMUM_PAGE_SIZE,
        MINIMUM_PAGE_SIZE * 2,
        MAXIMUM_PAGE_SIZE / 2,
        MAXIMUM_PAGE_SIZE));

} // namespace Calico