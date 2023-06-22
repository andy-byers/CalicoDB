// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#include "node.h"
#include <limits>
#include <gtest/gtest.h>

namespace calicodb::test
{

class NodeTests : public testing::Test
{
public:
    explicit NodeTests()
        : m_backing(kPageSize, '\0'),
          m_scratch(kPageSize, '\0')
    {
        m_ref.page = m_backing.data();
        m_ref.page_id = Id(3);
        m_node = Node::from_new_page(m_ref, true);
    }

    ~NodeTests() override = default;

    std::string m_backing;
    std::string m_scratch;
    PageRef m_ref;
    Node m_node;

    // Use 2 bytes for the keys.
    char m_external_cell[4] = {'\x00', '\x02'};
    char m_internal_cell[7] = {'\x00', '\x00', '\x00', '\x00', '\x02'};
    [[nodiscard]] auto make_cell(U32 k) -> Cell
    {
        EXPECT_LE(k, std::numeric_limits<U16>::max());
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
            cell.key = m_external_cell + 2;
            cell.footprint = sizeof(m_external_cell);
        }
        cell.key[0] = static_cast<char>(k >> 8);
        cell.key[1] = static_cast<char>(k);
        return cell;
    }

    using TestNodeType = U32;
    static constexpr TestNodeType kExternalNode = 0;
    static constexpr TestNodeType kExternalRoot = 1;
    static constexpr TestNodeType kInternalNode = 2;
    static constexpr TestNodeType kInternalRoot = 3;

    [[nodiscard]] auto change_node_type(TestNodeType type) -> bool
    {
        bool is_leaf;
        switch (type) {
            case kInternalRoot:
                m_ref.page_id.value = 1;
                is_leaf = false;
                break;
            case kExternalRoot:
                m_ref.page_id.value = 1;
                is_leaf = true;
                break;
            case kInternalNode:
                m_ref.page_id.value = 3;
                is_leaf = false;
                break;
            case kExternalNode:
                m_ref.page_id.value = 3;
                is_leaf = true;
                break;
            default:
                return false;
        }

        std::memset(m_ref.page, 0, kPageSize);
        m_node = Node::from_new_page(m_ref, is_leaf);
        return true;
    }
};

class BlockAllocatorTests : public NodeTests
{
public:
    explicit BlockAllocatorTests() = default;

    ~BlockAllocatorTests() override = default;

    auto reserve_for_test(U32 n) -> void
    {
        ASSERT_LT(n, kPageSize - FileHdr::kSize - NodeHdr::kSize)
            << "reserve_for_test(" << n << ") leaves no room for possible headers";
        m_size = n;
        m_base = kPageSize - n;
    }

    U32 m_size = 0;
    U32 m_base = 0;
};

TEST_F(BlockAllocatorTests, MergesAdjacentBlocks)
{
    reserve_for_test(40);

    // ..........#####...............#####.....
    ASSERT_EQ(0, BlockAllocator::release(m_node, m_base + 10, 5));
    ASSERT_EQ(0, BlockAllocator::release(m_node, m_base + 30, 5));
    ASSERT_EQ(BlockAllocator::freelist_size(m_node), 10);

    // .....##########...............#####.....
    ASSERT_EQ(0, BlockAllocator::release(m_node, m_base + 5, 5));
    ASSERT_EQ(BlockAllocator::freelist_size(m_node), 15);

    // .....##########...............##########
    ASSERT_EQ(0, BlockAllocator::release(m_node, m_base + 35, 5));
    ASSERT_EQ(BlockAllocator::freelist_size(m_node), 20);

    // .....###############..........##########
    ASSERT_EQ(0, BlockAllocator::release(m_node, m_base + 15, 5));
    ASSERT_EQ(BlockAllocator::freelist_size(m_node), 25);

    // .....###############.....###############
    ASSERT_EQ(0, BlockAllocator::release(m_node, m_base + 25, 5));
    ASSERT_EQ(BlockAllocator::freelist_size(m_node), 30);

    // .....###################################
    ASSERT_EQ(0, BlockAllocator::release(m_node, m_base + 20, 5));
    ASSERT_EQ(BlockAllocator::freelist_size(m_node), 35);

    // ########################################
    ASSERT_EQ(0, BlockAllocator::release(m_node, m_base, 5));
    ASSERT_EQ(BlockAllocator::freelist_size(m_node), m_size);
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
    ASSERT_EQ(BlockAllocator::freelist_size(m_node), 15);
    ASSERT_EQ(NodeHdr::get_frag_count(m_node.hdr()), 5);

    // .....#################......**#####*....
    ASSERT_EQ(0, BlockAllocator::release(m_node, m_base + 17, 5));
    ASSERT_EQ(BlockAllocator::freelist_size(m_node), 22);
    ASSERT_EQ(NodeHdr::get_frag_count(m_node.hdr()), 3);

    // .....##############################*....
    ASSERT_EQ(0, BlockAllocator::release(m_node, m_base + 22, 6));
    ASSERT_EQ(BlockAllocator::freelist_size(m_node), 30);
    ASSERT_EQ(NodeHdr::get_frag_count(m_node.hdr()), 1);

    // .....##############################*....
    ASSERT_EQ(0, BlockAllocator::release(m_node, m_base + 36, 4));
    ASSERT_EQ(BlockAllocator::freelist_size(m_node), 35);
    ASSERT_EQ(NodeHdr::get_frag_count(m_node.hdr()), 0);
}

TEST_F(BlockAllocatorTests, ExternalNodesDoNotConsume3ByteFragments)
{
    reserve_for_test(11);
    NodeHdr::put_type(m_node.hdr(), NodeHdr::kExternal);
    NodeHdr::put_frag_count(m_node.hdr(), 3);

    // ....***####
    ASSERT_EQ(0, BlockAllocator::release(m_node, m_base + 7, 4));

    // ####***####
    ASSERT_EQ(0, BlockAllocator::release(m_node, m_base + 0, 4));
    ASSERT_EQ(BlockAllocator::freelist_size(m_node), m_size - NodeHdr::get_frag_count(m_node.hdr()));
    ASSERT_EQ(NodeHdr::get_frag_count(m_node.hdr()), 3);
}

TEST_F(BlockAllocatorTests, InternalNodesConsume3ByteFragments)
{
    m_node = Node::from_new_page(m_ref, false);

    reserve_for_test(11);
    NodeHdr::put_frag_count(m_node.hdr(), 3);

    // ....***####
    ASSERT_EQ(0, BlockAllocator::release(m_node, m_base + 7, 4));

    // ###########
    ASSERT_EQ(0, BlockAllocator::release(m_node, m_base + 0, 4));
    ASSERT_EQ(BlockAllocator::freelist_size(m_node), m_size);
    ASSERT_EQ(NodeHdr::get_frag_count(m_node.hdr()), 0);
}

TEST_F(NodeTests, CellLifecycle)
{
    U32 type = 0;
    do {
        auto target_space = m_node.usable_space;
        for (U32 i = 0;; ++i) {
            const auto cell_in = make_cell(i);
            const auto rc = m_node.write(i, cell_in, m_scratch.data());
            if (rc == 0) {
                break;
            }
            ASSERT_GT(rc, 0);
            target_space -= cell_in.footprint + 2;
            ASSERT_EQ(m_node.usable_space, target_space);
            ASSERT_TRUE(m_node.assert_state());
        }

        for (U32 j = 0; j < NodeHdr::get_cell_count(m_node.hdr()); ++j) {
            const auto cell_in = make_cell(j);
            Cell cell_out = {};
            ASSERT_EQ(0, m_node.read(j, cell_out));
            ASSERT_EQ(cell_in.local_pl_size, cell_out.local_pl_size);
            ASSERT_EQ(cell_in.total_pl_size, cell_out.total_pl_size);
            ASSERT_EQ(Slice(cell_in.key, cell_in.key_size),
                      Slice(cell_out.key, cell_out.key_size));
        }

        while (NodeHdr::get_cell_count(m_node.hdr()) > 0) {
            Cell cell_out = {};
            ASSERT_EQ(0, m_node.read(0, cell_out));
            ASSERT_EQ(0, m_node.erase(0, cell_out.footprint));
            target_space += cell_out.footprint + 2;
            ASSERT_EQ(m_node.usable_space, target_space);
        }
        ASSERT_TRUE(m_node.assert_state());
        ASSERT_EQ(0, m_node.defrag(m_scratch.data()));
        ASSERT_EQ(m_node.usable_space, target_space);

    } while (change_node_type(++type));
}

// When a cell is erased, at most 4 bytes are overwritten at the start (to write the block size and
// next block location as part of intra-node memory management). This means that if the node was an
// internal node, the cell is still usable.
TEST_F(NodeTests, OverwriteOnEraseBehavior)
{
    for (auto t : {kExternalNode, kInternalNode}) {
        ASSERT_TRUE(change_node_type(t));
        const auto cell_in = make_cell(0);
        ASSERT_LT(0, m_node.write(0, cell_in, m_scratch.data()));

        Cell cell_out;
        ASSERT_EQ(0, m_node.read(0, cell_out));
        ASSERT_EQ(cell_in.footprint, cell_out.footprint);

        for (int i = 0; i < 2; ++i) {
            ASSERT_EQ(Slice(cell_in.key, cell_in.key_size) ==
                      Slice(cell_out.key, cell_out.key_size),
                      i == 0 || t == kInternalNode);
            if (i == 0) {
                // Node::erase() should overwrite at most the first 4 bytes of the cell, which in this case,
                // belong to the child ID. The other fields should remain the same. In fact, the cell itself
                // is still usable as long as we ignore the child ID, which is nonsense now.
                m_node.erase(0, cell_out.footprint);
            }
        }
    }
}

} // namespace calicodb::test