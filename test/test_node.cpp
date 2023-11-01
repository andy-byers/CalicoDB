// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#include "calicodb/env.h"
#include "node.h"
#include "test.h"
#include <gtest/gtest.h>
#include <limits>

namespace calicodb::test
{

class NodeTests : public testing::Test
{
public:
    Env *env;

    static constexpr auto kCellScratchSize = TEST_PAGE_SIZE / 2;
    char m_external_cell[kCellScratchSize] = {};
    char m_internal_cell[kCellScratchSize] = {};
    char m_bucket_cell[kCellScratchSize] = {};

    static constexpr auto kMaxPayloadSize = kMaxAllocation;
    char m_max_external_cell[kCellScratchSize] = {};
    char m_max_internal_cell[kCellScratchSize] = {};
    char m_max_bucket_cell[kCellScratchSize] = {};

    explicit NodeTests()
        : m_backing(TEST_PAGE_SIZE, '\0'),
          m_scratch(TEST_PAGE_SIZE, '\0'),
          m_options(TEST_PAGE_SIZE, m_scratch.data()),
          m_ref(PageRef::alloc(TEST_PAGE_SIZE))
    {
        env = &default_env();
        m_ref->page_id = Id(3);
        std::memset(m_ref->data, 0, TEST_PAGE_SIZE);
        m_node = Node::from_new_page(m_options, *m_ref, true);

        encode_leaf_record_cell_hdr(m_external_cell, 2, 0);
        encode_branch_record_cell_hdr(m_internal_cell, 2, Id(42));
        prepare_bucket_cell_hdr(m_bucket_cell, 2);

        encode_leaf_record_cell_hdr(m_max_external_cell, kMaxPayloadSize, 0);
        encode_branch_record_cell_hdr(m_max_internal_cell, kMaxPayloadSize, Id(42));
        prepare_bucket_cell_hdr(m_max_bucket_cell, kMaxPayloadSize);
    }

    ~NodeTests() override
    {
        PageRef::free(m_ref);
    }

    std::string m_backing;
    std::string m_scratch;
    const Node::Options m_options;
    PageRef *const m_ref;
    Node m_node;

    [[nodiscard]] auto make_cell(uint32_t k) -> Cell
    {
        auto *ptr = m_node.is_leaf() ? m_external_cell
                                     : m_internal_cell;
        Cell cell;
        EXPECT_EQ(0, m_node.parser(ptr, ptr + kCellScratchSize,
                                   m_node.min_local, m_node.max_local, cell));
        EXPECT_LE(k, std::numeric_limits<uint16_t>::max());
        cell.key[0] = static_cast<char>(k >> 8);
        cell.key[1] = static_cast<char>(k);
        return cell;
    }

    [[nodiscard]] auto make_bucket_cell(uint32_t k) -> Cell
    {
        Cell cell;
        EXPECT_TRUE(m_node.is_leaf()) << "branch nodes cannot contain bucket cells";
        EXPECT_EQ(0, m_node.parser(m_bucket_cell, m_bucket_cell + kCellScratchSize,
                                   m_node.min_local, m_node.max_local, cell));
        EXPECT_LE(k, std::numeric_limits<uint16_t>::max());
        cell.key[0] = static_cast<char>(k >> 8);
        cell.key[1] = static_cast<char>(k);
        return cell;
    }

    [[nodiscard]] auto make_max_cell(uint32_t k) -> Cell
    {
        auto *ptr = m_node.is_leaf() ? m_max_external_cell
                                     : m_max_internal_cell;
        Cell cell;
        EXPECT_EQ(0, m_node.parser(ptr, ptr + kCellScratchSize,
                                   m_node.min_local, m_node.max_local, cell));
        put_u32(cell.key + cell.local_size, 123); // Overflow ID
        EXPECT_LE(k, std::numeric_limits<uint16_t>::max());
        cell.key[0] = static_cast<char>(k >> 8);
        cell.key[1] = static_cast<char>(k);
        return cell;
    }

    [[nodiscard]] auto make_max_bucket_cell(uint32_t k) -> Cell
    {
        Cell cell;
        EXPECT_TRUE(m_node.is_leaf()) << "branch nodes cannot contain bucket cells";
        EXPECT_EQ(0, m_node.parser(m_max_bucket_cell, m_max_bucket_cell + kCellScratchSize,
                                   m_node.min_local, m_node.max_local, cell));
        put_u32(cell.key + cell.local_size, 123); // Overflow ID
        EXPECT_LE(k, std::numeric_limits<uint16_t>::max());
        cell.key[0] = static_cast<char>(k >> 8);
        cell.key[1] = static_cast<char>(k);
        return cell;
    }

    auto nth_cell_equals(uint32_t idx, const Cell &cell)
    {
        Cell other;
        EXPECT_EQ(0, m_node.read(idx, other));
        return other.key_size == cell.key_size &&
               other.local_size == cell.local_size &&
               other.total_size == cell.total_size &&
               other.footprint == cell.footprint &&
               other.is_bucket == cell.is_bucket;
    }

    using TestNodeType = uint32_t;
    static constexpr TestNodeType kExternalNode = 0;
    static constexpr TestNodeType kExternalRoot = 1;
    static constexpr TestNodeType kInternalNode = 2;
    static constexpr TestNodeType kInternalRoot = 3;

    [[nodiscard]] auto change_node_type(TestNodeType type) -> bool
    {
        bool is_leaf;
        switch (type) {
            case kInternalRoot:
                m_ref->page_id.value = 1;
                is_leaf = false;
                break;
            case kExternalRoot:
                m_ref->page_id.value = 1;
                is_leaf = true;
                break;
            case kInternalNode:
                m_ref->page_id.value = 3;
                is_leaf = false;
                break;
            case kExternalNode:
                m_ref->page_id.value = 3;
                is_leaf = true;
                break;
            default:
                return false;
        }

        std::memset(m_ref->data, 0, TEST_PAGE_SIZE);
        m_node = Node::from_new_page(m_options, *m_ref, is_leaf);
        return true;
    }
};

class BlockAllocatorTests : public NodeTests
{
public:
    explicit BlockAllocatorTests()
    {
        NodeHdr::put_type(m_node.hdr(), false);
    }

    ~BlockAllocatorTests() override = default;

    void reserve_for_test(uint32_t n)
    {
        // Make the gap large so BlockAllocator doesn't get confused.
        NodeHdr::put_cell_start(
            m_node.hdr(),
            page_offset(m_node.page_id()) + NodeHdr::size(m_node.is_leaf()));
        ASSERT_LT(n, TEST_PAGE_SIZE - FileHdr::kSize - NodeHdr::size(m_node.is_leaf()))
            << "reserve_for_test(" << n << ") leaves no room for possible headers";
        m_size = n;
        m_base = TEST_PAGE_SIZE - n;
    }

    uint32_t m_size = 0;
    uint32_t m_base = 0;
};

TEST_F(BlockAllocatorTests, MergesAdjacentBlocks)
{
    reserve_for_test(40);

    // ..........#####...............#####.....
    ASSERT_EQ(0, BlockAllocator::release(m_node, m_base + 10, 5));
    ASSERT_EQ(0, BlockAllocator::release(m_node, m_base + 30, 5));
    ASSERT_EQ(BlockAllocator::freelist_size(m_node, TEST_PAGE_SIZE), 10);

    // .....##########...............#####.....
    ASSERT_EQ(0, BlockAllocator::release(m_node, m_base + 5, 5));
    ASSERT_EQ(BlockAllocator::freelist_size(m_node, TEST_PAGE_SIZE), 15);

    // .....##########...............##########
    ASSERT_EQ(0, BlockAllocator::release(m_node, m_base + 35, 5));
    ASSERT_EQ(BlockAllocator::freelist_size(m_node, TEST_PAGE_SIZE), 20);

    // .....###############..........##########
    ASSERT_EQ(0, BlockAllocator::release(m_node, m_base + 15, 5));
    ASSERT_EQ(BlockAllocator::freelist_size(m_node, TEST_PAGE_SIZE), 25);

    // .....###############.....###############
    ASSERT_EQ(0, BlockAllocator::release(m_node, m_base + 25, 5));
    ASSERT_EQ(BlockAllocator::freelist_size(m_node, TEST_PAGE_SIZE), 30);

    // .....###################################
    ASSERT_EQ(0, BlockAllocator::release(m_node, m_base + 20, 5));
    ASSERT_EQ(BlockAllocator::freelist_size(m_node, TEST_PAGE_SIZE), 35);

    // ########################################
    ASSERT_EQ(0, BlockAllocator::release(m_node, m_base, 5));
    ASSERT_EQ(BlockAllocator::freelist_size(m_node, TEST_PAGE_SIZE), m_size);
}

TEST_F(BlockAllocatorTests, ConsumesAdjacentFragments)
{
    reserve_for_test(40);
    NodeHdr::put_frag_count(m_node.hdr(), 6);

    // .........*#####**...........**#####*....
    ASSERT_EQ(0, BlockAllocator::release(m_node, m_base + 10, 5));
    ASSERT_EQ(0, BlockAllocator::release(m_node, m_base + 30, 5));

    // .....##########**...........**#####*....
    ASSERT_EQ(0, BlockAllocator::release(m_node, m_base + 5, 4));
    ASSERT_EQ(BlockAllocator::freelist_size(m_node, TEST_PAGE_SIZE), 15);
    ASSERT_EQ(NodeHdr::get_frag_count(m_node.hdr()), 5);

    // .....#################......**#####*....
    ASSERT_EQ(0, BlockAllocator::release(m_node, m_base + 17, 5));
    ASSERT_EQ(BlockAllocator::freelist_size(m_node, TEST_PAGE_SIZE), 22);
    ASSERT_EQ(NodeHdr::get_frag_count(m_node.hdr()), 3);

    // .....##############################*....
    ASSERT_EQ(0, BlockAllocator::release(m_node, m_base + 22, 6));
    ASSERT_EQ(BlockAllocator::freelist_size(m_node, TEST_PAGE_SIZE), 30);
    ASSERT_EQ(NodeHdr::get_frag_count(m_node.hdr()), 1);

    // .....##############################*....
    ASSERT_EQ(0, BlockAllocator::release(m_node, m_base + 36, 4));
    ASSERT_EQ(BlockAllocator::freelist_size(m_node, TEST_PAGE_SIZE), 35);
    ASSERT_EQ(NodeHdr::get_frag_count(m_node.hdr()), 0);
}

TEST_F(BlockAllocatorTests, ExternalNodesConsume3ByteFragments)
{
    reserve_for_test(11);
    NodeHdr::put_type(m_node.hdr(), true);
    NodeHdr::put_frag_count(m_node.hdr(), 3);

    // ....***####
    ASSERT_EQ(0, BlockAllocator::release(m_node, m_base + 7, 4));

    // ###########
    ASSERT_EQ(0, BlockAllocator::release(m_node, m_base + 0, 4));
    ASSERT_EQ(BlockAllocator::freelist_size(m_node, TEST_PAGE_SIZE), m_size - NodeHdr::get_frag_count(m_node.hdr()));
    ASSERT_EQ(NodeHdr::get_frag_count(m_node.hdr()), 0);
}

TEST_F(BlockAllocatorTests, InternalNodesConsume3ByteFragments)
{
    m_node = Node::from_new_page(m_options, *m_ref, false);

    reserve_for_test(11);
    NodeHdr::put_frag_count(m_node.hdr(), 3);

    // ....***####
    ASSERT_EQ(0, BlockAllocator::release(m_node, m_base + 7, 4));

    // ###########
    ASSERT_EQ(0, BlockAllocator::release(m_node, m_base + 0, 4));
    ASSERT_EQ(BlockAllocator::freelist_size(m_node, TEST_PAGE_SIZE), m_size);
    ASSERT_EQ(NodeHdr::get_frag_count(m_node.hdr()), 0);
}

TEST_F(NodeTests, ExternalNonRootFits2Cells)
{
    ASSERT_TRUE(change_node_type(kExternalNode));
    ASSERT_LT(0, m_node.insert(0, make_max_cell(0)));
    ASSERT_LT(0, m_node.insert(1, make_max_cell(1)));
    ASSERT_EQ(0, m_node.insert(2, make_max_cell(2))); // Overflow
    ASSERT_TRUE(m_node.assert_integrity());
}

TEST_F(NodeTests, InternalNonRootFits4Cells)
{
    ASSERT_TRUE(change_node_type(kInternalNode));
    ASSERT_LT(0, m_node.insert(0, make_max_cell(0)));
    ASSERT_LT(0, m_node.insert(1, make_max_cell(1)));
    ASSERT_LT(0, m_node.insert(2, make_max_cell(2)));
    ASSERT_LT(0, m_node.insert(3, make_max_cell(3)));
    ASSERT_EQ(0, m_node.insert(4, make_max_cell(4))); // Overflow
    ASSERT_TRUE(m_node.assert_integrity());
}

TEST_F(NodeTests, ExternalRootFits1Cell)
{
    ASSERT_TRUE(change_node_type(kExternalRoot));
    ASSERT_LT(0, m_node.insert(0, make_max_cell(0)));
    ASSERT_EQ(0, m_node.insert(1, make_max_cell(1))); // Overflow
    ASSERT_TRUE(m_node.assert_integrity());
}

TEST_F(NodeTests, InternalRootFits3Cells)
{
    ASSERT_TRUE(change_node_type(kInternalRoot));
    ASSERT_LT(0, m_node.insert(0, make_max_cell(0)));
    ASSERT_LT(0, m_node.insert(1, make_max_cell(1)));
    ASSERT_LT(0, m_node.insert(2, make_max_cell(2)));
    ASSERT_EQ(0, m_node.insert(3, make_max_cell(3))); // Overflow
    ASSERT_TRUE(m_node.assert_integrity());
}

TEST_F(NodeTests, ExternalNonRootIO)
{
    ASSERT_TRUE(change_node_type(kExternalNode));
    ASSERT_LT(0, m_node.insert(0, make_cell(0)));
    ASSERT_LT(0, m_node.insert(1, make_max_cell(1)));
    ASSERT_TRUE(nth_cell_equals(0, make_cell(0)));
    ASSERT_TRUE(nth_cell_equals(1, make_max_cell(1)));
}

TEST_F(NodeTests, InternalNonRootIO)
{
    ASSERT_TRUE(change_node_type(kInternalNode));
    ASSERT_LT(0, m_node.insert(0, make_cell(0)));
    ASSERT_LT(0, m_node.insert(1, make_max_cell(1)));
    ASSERT_TRUE(nth_cell_equals(0, make_cell(0)));
    ASSERT_TRUE(nth_cell_equals(1, make_max_cell(1)));
}

TEST_F(NodeTests, ExternalRootIO)
{
    ASSERT_TRUE(change_node_type(kExternalRoot));
    ASSERT_LT(0, m_node.insert(0, make_cell(0)));
    ASSERT_LT(0, m_node.insert(1, make_max_cell(1)));
    ASSERT_TRUE(nth_cell_equals(0, make_cell(0)));
    ASSERT_TRUE(nth_cell_equals(1, make_max_cell(1)));
}

TEST_F(NodeTests, InternalRootIO)
{
    ASSERT_TRUE(change_node_type(kInternalRoot));
    ASSERT_LT(0, m_node.insert(0, make_cell(0)));
    ASSERT_LT(0, m_node.insert(1, make_max_cell(1)));
    ASSERT_TRUE(nth_cell_equals(0, make_cell(0)));
    ASSERT_TRUE(nth_cell_equals(1, make_max_cell(1)));
}

TEST_F(NodeTests, ExternalRootBucketIO)
{
    ASSERT_TRUE(change_node_type(kExternalRoot));
    ASSERT_LT(0, m_node.insert(0, make_bucket_cell(0)));
    ASSERT_LT(0, m_node.insert(1, make_max_bucket_cell(1)));
    ASSERT_TRUE(nth_cell_equals(0, make_bucket_cell(0)));
    ASSERT_TRUE(nth_cell_equals(1, make_max_bucket_cell(1)));
}

TEST_F(NodeTests, CellLifecycle)
{
    uint32_t type = 0;
    do {
        auto target_space = m_node.usable_space;
        for (uint32_t i = 0;; ++i) {
            const auto cell_in = make_cell(i);
            const auto rc = m_node.insert(i, cell_in);
            if (rc == 0) {
                break;
            }
            ASSERT_GT(rc, 0);
            target_space -= cell_in.footprint + 2;
            ASSERT_EQ(m_node.usable_space, target_space);
            ASSERT_TRUE(m_node.assert_integrity());
        }

        for (uint32_t j = 0; j < m_node.cell_count(); ++j) {
            const auto cell_in = make_cell(j);
            Cell cell_out = {};
            ASSERT_EQ(0, m_node.read(j, cell_out));
            ASSERT_EQ(cell_in.local_size, cell_out.local_size);
            ASSERT_EQ(cell_in.total_size, cell_out.total_size);
            ASSERT_EQ(Slice(cell_in.key, cell_in.key_size),
                      Slice(cell_out.key, cell_out.key_size));
        }

        while (0 < m_node.cell_count()) {
            Cell cell_out = {};
            ASSERT_EQ(0, m_node.read(0, cell_out));
            ASSERT_EQ(0, m_node.erase(0, cell_out.footprint));
            target_space += cell_out.footprint + 2;
            ASSERT_EQ(m_node.usable_space, target_space);
        }
        ASSERT_TRUE(m_node.assert_integrity());
        ASSERT_EQ(0, m_node.defrag());
        ASSERT_EQ(m_node.usable_space, target_space);

    } while (change_node_type(++type));
}

// When a cell is erased, at most 4 bytes are overwritten at the start (to write the block size and
// next block location as part of intra-node memory management).
TEST_F(NodeTests, OverwriteOnEraseBehavior)
{
    for (auto t : {kExternalNode, kInternalNode}) {
        ASSERT_TRUE(change_node_type(t));
        const auto cell_in = make_cell(0);
        ASSERT_LT(0, m_node.insert(0, cell_in));

        Cell cell_out;
        ASSERT_EQ(0, m_node.read(0, cell_out));
        ASSERT_EQ(cell_in.footprint, cell_out.footprint);

        for (int i = 0; i < 2; ++i) {
            ASSERT_EQ(Slice(cell_in.key, cell_in.key_size),
                      Slice(cell_out.key, cell_out.key_size));
            if (i == 0) {
                // Node::erase() should overwrite at most the first 4 bytes of the cell, which in this case,
                // belong to the child ID. The other fields should remain the same. In fact, the cell itself
                // is still usable as long as we ignore the child ID, which is nonsense now.
                m_node.erase(0, cell_out.footprint);
            }
        }
    }
}

TEST(NodeHeaderTests, ReportsInvalidNodeType)
{
    char type;

    type = NodeHdr::kInvalid;
    ASSERT_EQ(NodeHdr::kInvalid, NodeHdr::get_type(&type));

    type = 100;
    ASSERT_EQ(NodeHdr::kInvalid, NodeHdr::get_type(&type));
}

class CorruptedNodeTests : public NodeTests
{
public:
    void assert_corrupted_node()
    {
        Node corrupted;
        ASSERT_NE(Node::from_existing_page(m_options, *m_node.ref, corrupted), 0);
        ASSERT_NOK(m_node.check_integrity());
    }
    void assert_valid_node()
    {
        Node valid;
        ASSERT_EQ(Node::from_existing_page(m_options, *m_node.ref, valid), 0);
        ASSERT_OK(m_node.check_integrity());
    }
};

TEST_F(CorruptedNodeTests, SanityCheck)
{
    assert_valid_node();
}

TEST_F(CorruptedNodeTests, InvalidType)
{
    m_node.hdr()[NodeHdr::kTypeOffset] = 0;
    assert_corrupted_node();
    m_node.hdr()[NodeHdr::kTypeOffset] = 42;
    assert_corrupted_node();
}

TEST_F(CorruptedNodeTests, InvalidCellCount)
{
    NodeHdr::put_cell_count(m_node.hdr(), std::numeric_limits<uint16_t>::max());
    assert_corrupted_node();
    // Lower bound of the gap is greater than the upper bound.
    NodeHdr::put_cell_count(m_node.hdr(), 512);
    NodeHdr::put_cell_start(m_node.hdr(), NodeHdr::size(m_node.is_leaf()));
    assert_corrupted_node();
}

TEST_F(CorruptedNodeTests, InvalidCellStart)
{
    NodeHdr::put_cell_start(m_node.hdr(), TEST_PAGE_SIZE + 1);
    assert_corrupted_node();
}

TEST_F(CorruptedNodeTests, CorruptedChunks)
{
    TEST_LOG << "CorruptedNodeTests.CorruptedChunks\n";
    static constexpr uint32_t kNumChunks = 8;
    static constexpr uint32_t kChunkSize = TEST_PAGE_SIZE / kNumChunks;
    static_assert(kNumChunks * kChunkSize == TEST_PAGE_SIZE);
    const std::string junk(kChunkSize, '*');
    TEST_LOG << "Chunk size = " << kChunkSize << '\n';
    TEST_LOG << "Junk string = " << junk << '\n';
    for (uint32_t i = 0;; ++i) {
        if (!m_node.insert(i, make_cell(i))) {
            break;
        }
    }
    for (uint32_t i = 0; i < m_node.cell_count(); i += 3) {
        Cell cell;
        ASSERT_EQ(0, m_node.read(i, cell));
        m_node.erase(i, cell.footprint);
    }
    for (uint32_t i = 0; i < kNumChunks; ++i) {
        const auto offset = kChunkSize * i;
        const auto saved = Slice(m_node.ref->data + offset, kChunkSize)
                               .to_string();

        // Corrupt the node.
        std::memcpy(m_node.ref->data + offset, junk.data(), kChunkSize);
        ASSERT_NOK(m_node.check_integrity());

        // Restore the chunk to its original contents.
        std::memcpy(m_node.ref->data + offset, saved.data(), kChunkSize);
        assert_valid_node();
    }
}

class CorruptedNodeFreelistTests : public CorruptedNodeTests
{
public:
    static constexpr size_t kNumBlocks = 3;
    char *m_ptrs[kNumBlocks];
    uint32_t m_reset[kNumBlocks];

    void SetUp() override
    {
        char *cell_ptrs[kNumBlocks * 2];
        uint32_t cell_sizes[kNumBlocks * 2];
        for (uint32_t i = 0; i < kNumBlocks * 2; ++i) {
            auto cell = make_cell(i);
            ASSERT_GT(m_node.insert(i, cell), 0);
            // Fill the cell with pointers into m_node.
            ASSERT_EQ(m_node.read(i, cell), 0);
            cell_sizes[i] = cell.footprint;
            cell_ptrs[i] = cell.ptr;
        }
        for (uint32_t i = 0; i < kNumBlocks; ++i) {
            // Erase every other cell so that the free blocks don't merge.
            ASSERT_EQ(m_node.erase(i, cell_sizes[i * 2]), 0);
            m_ptrs[i] = cell_ptrs[i * 2];
            m_reset[i] = get_u32(m_ptrs[i]); // Free block header
        }
    }
};

TEST_F(CorruptedNodeFreelistTests, StartOutOfBounds)
{
    NodeHdr::put_free_start(m_node.hdr(), TEST_PAGE_SIZE);
    assert_corrupted_node();
}

TEST_F(CorruptedNodeFreelistTests, InvalidFreeBlockHeader)
{
    for (uint32_t i = 0; i < kNumBlocks; ++i) {
        put_u16(m_ptrs[i], TEST_PAGE_SIZE);
        assert_corrupted_node();
        put_u32(m_ptrs[i], m_reset[i]);

        put_u16(m_ptrs[i] + 2, TEST_PAGE_SIZE);
        assert_corrupted_node();
        put_u32(m_ptrs[i], m_reset[i]);

        assert_valid_node();
    }
}

} // namespace calicodb::test