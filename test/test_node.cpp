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
    explicit NodeTests()
        : m_backing(TEST_PAGE_SIZE, '\0'),
          m_scratch(TEST_PAGE_SIZE, '\0'),
          m_ref(PageRef::alloc(TEST_PAGE_SIZE))
    {
        env = &default_env();
        m_ref->page_id = Id(3);
        m_node = Node::from_new_page(*m_ref, TEST_PAGE_SIZE, m_scratch.data(), true);
    }

    ~NodeTests() override
    {
        PageRef::free(m_ref);
    }

    std::string m_backing;
    std::string m_scratch;
    PageRef *const m_ref;
    Node m_node;

    // Use 2 bytes for the keys.
    char m_external_cell[6] = {'\x00', '\x02', '\x00', '\x00'};
    char m_internal_cell[7] = {'\x00', '\x00', '\x00', '\x00', '\x02'};
    [[nodiscard]] auto make_cell(uint32_t k) -> Cell
    {
        EXPECT_LE(k, std::numeric_limits<uint16_t>::max());
        Cell cell = {
            m_internal_cell,
            m_internal_cell + 5,
            2,
            2,
            2,
            sizeof(m_internal_cell),
        };
        if (m_node.is_leaf()) {
            cell.ptr = m_external_cell;
            cell.key = m_external_cell + 4;
            cell.footprint = sizeof(m_external_cell);
        }
        cell.key[0] = static_cast<char>(k >> 8);
        cell.key[1] = static_cast<char>(k);
        return cell;
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
        m_node = Node::from_new_page(*m_ref, TEST_PAGE_SIZE, m_scratch.data(), is_leaf);
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

    auto reserve_for_test(uint32_t n) -> void
    {
        // Make the gap large so BlockAllocator doesn't get confused.
        NodeHdr::put_cell_start(
            m_node.hdr(),
            page_offset(m_node.ref->page_id) + NodeHdr::kSize);
        ASSERT_LT(n, TEST_PAGE_SIZE - FileHdr::kSize - NodeHdr::kSize)
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
    m_node = Node::from_new_page(*m_ref, TEST_PAGE_SIZE, m_scratch.data(), false);

    reserve_for_test(11);
    NodeHdr::put_frag_count(m_node.hdr(), 3);

    // ....***####
    ASSERT_EQ(0, BlockAllocator::release(m_node, m_base + 7, 4));

    // ###########
    ASSERT_EQ(0, BlockAllocator::release(m_node, m_base + 0, 4));
    ASSERT_EQ(BlockAllocator::freelist_size(m_node, TEST_PAGE_SIZE), m_size);
    ASSERT_EQ(NodeHdr::get_frag_count(m_node.hdr()), 0);
}

TEST_F(NodeTests, CellLifecycle)
{
    uint32_t type = 0;
    do {
        auto target_space = m_node.usable_space;
        for (uint32_t i = 0;; ++i) {
            const auto cell_in = make_cell(i);
            const auto rc = m_node.write(i, cell_in);
            if (rc == 0) {
                break;
            }
            ASSERT_GT(rc, 0);
            target_space -= cell_in.footprint + 2;
            ASSERT_EQ(m_node.usable_space, target_space);
            ASSERT_TRUE(m_node.assert_state());
        }

        for (uint32_t j = 0; j < NodeHdr::get_cell_count(m_node.hdr()); ++j) {
            const auto cell_in = make_cell(j);
            Cell cell_out = {};
            ASSERT_EQ(0, m_node.read(j, cell_out));
            ASSERT_EQ(cell_in.local_pl_size, cell_out.local_pl_size);
            ASSERT_EQ(cell_in.total_pl_size, cell_out.total_pl_size);
            ASSERT_EQ(Slice(cell_in.key, cell_in.key_size),
                      Slice(cell_out.key, cell_out.key_size));
        }

        while (0 < NodeHdr::get_cell_count(m_node.hdr())) {
            Cell cell_out = {};
            ASSERT_EQ(0, m_node.read(0, cell_out));
            ASSERT_EQ(0, m_node.erase(0, cell_out.footprint));
            target_space += cell_out.footprint + 2;
            ASSERT_EQ(m_node.usable_space, target_space);
        }
        ASSERT_TRUE(m_node.assert_state());
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
        ASSERT_LT(0, m_node.write(0, cell_in));

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
    auto assert_corrupted_node() -> void
    {
        Node corrupted;
        ASSERT_NE(Node::from_existing_page(*m_node.ref, TEST_PAGE_SIZE, m_scratch.data(), corrupted), 0);
    }
    auto assert_valid_node() -> void
    {
        Node valid;
        ASSERT_EQ(Node::from_existing_page(*m_node.ref, TEST_PAGE_SIZE, m_scratch.data(), valid), 0);
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
    NodeHdr::put_cell_start(m_node.hdr(), NodeHdr::kSize);
    assert_corrupted_node();
}

TEST_F(CorruptedNodeTests, InvalidCellStart)
{
    NodeHdr::put_cell_start(m_node.hdr(), TEST_PAGE_SIZE + 1);
    assert_corrupted_node();
}

class CorruptedNodeFreelistTests : public CorruptedNodeTests
{
public:
    static constexpr size_t kNumBlocks = 3;
    char *m_ptrs[kNumBlocks];
    uint32_t m_reset[kNumBlocks];

    auto SetUp() -> void override
    {
        char *cell_ptrs[kNumBlocks * 2];
        uint32_t cell_sizes[kNumBlocks * 2];
        for (uint32_t i = 0; i < kNumBlocks * 2; ++i) {
            auto cell = make_cell(i);
            ASSERT_GT(m_node.write(i, cell), 0);
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