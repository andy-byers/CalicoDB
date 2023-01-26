#include "temp/bplus_tree.h"
#include "temp/node.h"
#include "pager/basic_pager.h"
#include "wal/helpers.h"
#include "unit_tests.h"
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
    FileHeader_ source;
    source.magic_code = 1;
    source.header_crc = 2;
    source.page_count = 3;
    source.record_count = 4;
    source.free_list_id.value = 5;
    source.recovery_lsn.value = 6;
    source.page_size = backing.size();

    source.write(page);
    // Write a node header to make sure it doesn't overwrite the file header memory.
    NodeHeader_ unused;
    unused.write(page);
    FileHeader_ target {page};

    ASSERT_EQ(source.magic_code, target.magic_code);
    ASSERT_EQ(source.header_crc, target.header_crc);
    ASSERT_EQ(source.page_count, target.page_count);
    ASSERT_EQ(source.record_count, target.record_count);
    ASSERT_EQ(source.free_list_id, target.free_list_id);
    ASSERT_EQ(source.recovery_lsn, target.recovery_lsn);
    ASSERT_EQ(source.page_size, target.page_size);
}

TEST_F(HeaderTests, NodeHeader)
{
    NodeHeader_ source;
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
    FileHeader_ unused;
    unused.write(page);
    NodeHeader_ target {page};

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
    Node_ node {std::move(page), scratch.data()};

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
        auto page = std::move(node).take();
        (void)page.take();
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
        Node_::Iterator itr {node};
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
    Node_ node;
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
    node_emplace_cell(0, "a", "bc");
    node_emplace_cell(1, "1", "23");
    node_emplace_cell(2, "x", "yz");
    ASSERT_EQ(node.header.cell_count, 3);

    auto cell = read_cell(node, 0);
    ASSERT_EQ((Slice {cell.key, cell.local_ps}), "abc");

    cell = read_cell(node, 1);
    ASSERT_EQ((Slice {cell.key, cell.local_ps}), "123");

    cell = read_cell(node, 2);
    ASSERT_EQ((Slice {cell.key, cell.local_ps}), "xyz");
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

TEST_P(ExternalNodeTests, DefragmentPreservesMemory)
{
    node_emplace_cell(0, "a", "1");
    const auto target_space = usable_space(node);
    node_emplace_cell(1, "b", "2");
    node_emplace_cell(2, "c", "3");
    node_emplace_cell(3, "d", "4");
    erase_cell(node, 1);
    erase_cell(node, 1);
    erase_cell(node, 1);

    manual_defragment(node);
    ASSERT_EQ(usable_space(node), target_space);
    ASSERT_EQ(node.header.cell_count, 1);
}

TEST_P(ExternalNodeTests, Iteration)
{
    // a, b, c, d, e, f, g
    for (Size i {}; i < 7; ++i) {
        std::string k(1, 'a' + i);
        node_emplace_cell(i, k, "");
    }
    Node_::Iterator itr {node};

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
    ASSERT_EQ(node.read_key(node.get_slot(0)), "a");
    ASSERT_EQ(node.read_key(node.get_slot(1)), "b");
    ASSERT_EQ(node.read_key(node.get_slot(2)), "c");
}

TEST_P(ExternalNodeTests, HandlesOverflowIds)
{
    simulate_write("a", std::string(node.page.size(), '1'), Id {111});
    simulate_write("b", std::string(node.page.size(), '2'), Id {222});
    simulate_write("c", std::string(node.page.size(), '3'), Id {333});

    const auto cell1 = node.parse_cell(node.get_slot(0));
    const auto cell2 = node.parse_cell(node.get_slot(1));
    const auto cell3 = node.parse_cell(node.get_slot(2));
    ASSERT_EQ(node.read_key(cell1), "a");
    ASSERT_EQ(node.read_key(cell2), "b");
    ASSERT_EQ(node.read_key(cell3), "c");
    ASSERT_EQ((Slice {cell1.key + 1, cell1.local_ps - 1}), std::string(cell1.local_ps - 1, '1'));
    ASSERT_EQ((Slice {cell2.key + 1, cell2.local_ps - 1}), std::string(cell2.local_ps - 1, '2'));
    ASSERT_EQ((Slice {cell3.key + 1, cell3.local_ps - 1}), std::string(cell3.local_ps - 1, '3'));
    ASSERT_EQ(get_u64(cell1.key + cell1.local_ps), 111);
    ASSERT_EQ(get_u64(cell2.key + cell2.local_ps), 222);
    ASSERT_EQ(get_u64(cell3.key + cell3.local_ps), 333);
}

static constexpr Size SMALL_PAGE_SIZE {0x200};
static constexpr Size MEDIUM_PAGE_SIZE {0x1000};
static constexpr Size LARGE_PAGE_SIZE {0x8000};
static constexpr Id ROOT_PID {1};
static constexpr Id NON_ROOT_PID {2};

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
        auto page = std::move(dst_node).take();
        (void)page.take();
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
    Node_ dst_node;
    Cell_ cell;
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

class BPlusTreeTests_: public TestOnHeapWithParam<BPlusTreeTestParameters> {
public:
    BPlusTreeTests_()
        : param {GetParam()},
          scratch(param.page_size, '\x00'),
          log_scratch {wal_scratch_size(param.page_size), 32}
    {}

    auto SetUp() -> void override
    {
        system.has_xact = true;

        auto r = BasicPager::open({
            PREFIX,
            store.get(),
            &log_scratch,
            &wal,
            &system,
            8,
            param.page_size,
        });
        EXPECT_TRUE(r.has_value()) << r.error().what().data();
        pager = std::move(*r);

        auto root_page = pager->allocate_();
        pager->upgrade_(*root_page);
        Node_ root_node {std::move(*root_page), scratch.data()};
        root_node.header.is_external = true;
        pager->release_(std::move(root_node).take());
        pager->flush({});

        tree = std::make_unique<BPlusTree_>(*pager);
    }

    auto validate()
    {
        tree->TEST_check_links();
        tree->TEST_check_order();
    }

    BPlusTreeTestParameters param;
    LogScratchManager log_scratch;
    System system {PREFIX, {}};
    DisabledWriteAheadLog wal;
    std::string scratch;
    std::unique_ptr<Pager> pager;
    std::unique_ptr<BPlusTree_> tree;
};

TEST_P(BPlusTreeTests_, ConstructsAndDestructs)
{
    validate();
}

TEST_P(BPlusTreeTests_, InsertsRecords)
{
    ASSERT_TRUE(tree->insert("a", "x").value());
    ASSERT_TRUE(tree->insert("b", "y").value());
    ASSERT_TRUE(tree->insert("c", "z").value());
    validate();
}

TEST_P(BPlusTreeTests_, FindsRecordPositions)
{
    ASSERT_TRUE(tree->insert("a", "x").value());
    ASSERT_TRUE(tree->insert("b", "y").value());
    ASSERT_TRUE(tree->insert("c", "z").value());

    {
        auto r = tree->find("a").value();
        ASSERT_EQ(r.index, 0);
        const auto s = r.node.get_slot(r.index);
        const auto c = r.node.parse_cell(s);
        ASSERT_EQ(c.key[0], 'a');
        ASSERT_EQ(c.key[c.key_size], 'x');
    }
    {
        auto r = tree->find("b").value();
        ASSERT_EQ(r.index, 1);
        const auto s = r.node.get_slot(r.index);
        const auto c = r.node.parse_cell(s);
        ASSERT_EQ(c.key[0], 'b');
        ASSERT_EQ(c.key[c.key_size], 'y');
    }
    {
        auto r = tree->find("c").value();
        ASSERT_EQ(r.index, 2);
        const auto s = r.node.get_slot(r.index);
        const auto c = r.node.parse_cell(s);
        ASSERT_EQ(c.key[0], 'c');
        ASSERT_EQ(c.key[c.key_size], 'z');
    }
}

TEST_P(BPlusTreeTests_, ResolvesOverflows)
{
    for (Size i {}; i < 400; ++i) {
        ASSERT_TRUE(tree->insert(make_key<4>(i), "abc123xyzABC123XYZ").value());
        std::cerr << tree->TEST_to_string() << "\n\n";
        validate();
    }
}

INSTANTIATE_TEST_SUITE_P(
    BPlusTreeTests_,
    BPlusTreeTests_,
    ::testing::Values(
        BPlusTreeTestParameters {MINIMUM_PAGE_SIZE},
        BPlusTreeTestParameters {MINIMUM_PAGE_SIZE * 2},
        BPlusTreeTestParameters {MAXIMUM_PAGE_SIZE / 2},
        BPlusTreeTestParameters {MAXIMUM_PAGE_SIZE}));

} // namespace Calico