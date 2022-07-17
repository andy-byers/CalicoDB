#include "validation.h"
#include <filesystem>
#include <fstream>
#include "calico/cursor.h"
#include "tree/tree.h"
#include "tree/node_pool.h"

namespace cco {

using namespace page;
using namespace utils;

static auto find_minimum(ITree &tree) -> Node
{
    auto &pool = tree.pool();
    auto node = *pool.acquire(PID::root(), false);
    while (!node.is_external()) {
        const auto id = node.child_id(0);
        CCO_EXPECT_TRUE(pool.release(std::move(node)).has_value());
        node = *pool.acquire(id, false);
    }
    return node;
}

static auto has_next(const Node &node)
{
    return !node.right_sibling_id().is_null();
}

static auto get_next(NodePool &pool, Node node)
{
    const auto id = node.right_sibling_id();
    CCO_EXPECT_TRUE(pool.release(std::move(node)).has_value());
    return pool.acquire(id, false);
}

static auto traverse_inorder_helper(NodePool &pool, Node node, const std::function<void(Node&, Index)> &callback) -> void
{
    for (Index index {}; index <= node.cell_count(); ++index) {
        if (!node.is_external()) {
            const auto next_id = node.child_id(index);
            traverse_inorder_helper(pool, *pool.acquire(next_id, false), callback);
        }
        if (index < node.cell_count())
            callback(node, index);
    }
    CCO_EXPECT_TRUE(pool.release(std::move(node)).has_value());
}

static auto traverse_inorder(NodePool &pool, const std::function<void(Node&, Index)> &callback) -> void
{
    traverse_inorder_helper(pool, *pool.acquire(PID::root(), false), callback);
}

auto validate_siblings(ITree &tree) -> void
{
    auto node = find_minimum(tree);
    for (; has_next(node); node = *get_next(tree.pool(), std::move(node))) {
        auto right = *tree.pool().acquire(node.right_sibling_id(), false);
        CCO_EXPECT_LT(node.read_key(0), right.read_key(0));
        CCO_EXPECT_EQ(right.left_sibling_id(), node.id());
    }
    CCO_EXPECT_TRUE(tree.pool().release(std::move(node)).has_value());
}

auto validate_links(ITree &tree) -> void
{
    auto &pool = tree.pool();
    auto check_connection = [&](Node &node, Index index) -> void {
        auto child = *pool.acquire(node.child_id(index), false);
        CCO_EXPECT_EQ(child.parent_id(), node.id());
        CCO_EXPECT_TRUE(tree.pool().release(std::move(child)).has_value());
    };
    traverse_inorder(pool, [&](Node &node, Index index) -> void {
        const auto count = node.cell_count();
        CCO_EXPECT_LT(index, count);
        if (!node.is_external()) {
            check_connection(node, index);
            // Rightmost child.
            if (index == count - 1)
                check_connection(node, index + 1);
        }
    });
}

auto validate_ordering(ITree &tree) -> void
{

    if (tree.cell_count() < 2)
        return;

    static constexpr auto PATH = "/tmp/calico_validation";
    std::ofstream ofs {PATH, std::ios::trunc};
    CCO_EXPECT_TRUE(ofs.is_open());
    Size cell_count {};
    traverse_inorder(tree.pool(), [&cell_count, &ofs](Node &node, Index index) -> void {
        CCO_EXPECT_LT(index, node.cell_count());
        ofs << btos(node.read_key(index)) << '\n';
        cell_count++;
    });
    ofs.close();

    std::string lhs, rhs;
    std::ifstream left {PATH};
    std::ifstream right {PATH};
    CCO_EXPECT_TRUE(left.is_open());
    CCO_EXPECT_TRUE(right.is_open());
    CCO_EXPECT_FALSE(std::getline(right, rhs).eof());
    for (Index i {}; i < cell_count - 1; ++i) {
        CCO_EXPECT_FALSE(std::getline(left, lhs).eof());
        CCO_EXPECT_FALSE(std::getline(right, rhs).eof());
        CCO_EXPECT_LE(lhs, rhs);
    }
    CCO_EXPECT_TRUE(std::getline(right, rhs).eof());
}

} // cco