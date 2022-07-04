#include "validation.h"
#include <filesystem>
#include <fstream>
#include "calico/cursor.h"
#include "tree/tree.h"
#include "tree/node_pool.h"

namespace calico {

static auto find_minimum(ITree &tree) -> Node
{
    auto &pool = tree.pool();
    auto node = pool.acquire(PID::root(), false);
    while (!node.is_external())
        node = pool.acquire(node.child_id(0), false);
    return node;
}

static auto has_next(const Node &node)
{
    return !node.right_sibling_id().is_null();
}

static auto get_next(NodePool &pool, const Node &node)
{
    return pool.acquire(node.right_sibling_id(), false);
}

static auto traverse_inorder_helper(NodePool &pool, Node node, const std::function<void(Node&, Index)> &callback) -> void
{
    const auto id = node.id();
    for (Index index{}; index <= node.cell_count(); ++index) {
        if (!node.is_external()) {
            const auto next_id = node.child_id(index);
            traverse_inorder_helper(pool, pool.acquire(next_id, false), callback);
            node = pool.acquire(id, false);
        }
        if (index < node.cell_count())
            callback(node, index);
    }
}

static auto traverse_inorder(NodePool &pool, const std::function<void(Node&, Index)> &callback) -> void
{
    traverse_inorder_helper(pool, pool.acquire(PID::root(), false), callback);
}

auto validate_siblings(ITree &tree) -> void
{
    for (auto node = find_minimum(tree); has_next(node); node = get_next(tree.pool(), node)) {
        auto right = tree.pool().acquire(node.right_sibling_id(), false);
        CALICO_EXPECT_LT(node.read_key(0), right.read_key(0));
        CALICO_EXPECT_EQ(right.left_sibling_id(), node.id());
    }
}

auto validate_links(ITree &tree) -> void
{
    auto &pool = tree.pool();
    auto check_connection = [&](Node &node, Index index) -> void {
        auto child = pool.acquire(node.child_id(index), false);
        CALICO_EXPECT_EQ(child.parent_id(), node.id());
    };
    traverse_inorder(pool, [&](Node &node, Index index) -> void {
        const auto count = node.cell_count();
        CALICO_EXPECT_LT(index, count);
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
    CALICO_EXPECT_TRUE(ofs.is_open());
    Size cell_count {};
    traverse_inorder(tree.pool(), [&cell_count, &ofs](Node &node, Index index) -> void {
        CALICO_EXPECT_LT(index, node.cell_count());
        ofs << btos(node.read_key(index)) << '\n';
        cell_count++;
    });
    ofs.close();

    std::string lhs, rhs;
    std::ifstream left {PATH};
    std::ifstream right {PATH};
    CALICO_EXPECT_TRUE(left.is_open());
    CALICO_EXPECT_TRUE(right.is_open());
    CALICO_EXPECT_FALSE(std::getline(right, rhs).eof());
    for (Index i {}; i < cell_count - 1; ++i) {
        CALICO_EXPECT_FALSE(std::getline(left, lhs).eof());
        CALICO_EXPECT_FALSE(std::getline(right, rhs).eof());
        CALICO_EXPECT_LE(lhs, rhs);
    }
    CALICO_EXPECT_TRUE(std::getline(right, rhs).eof());
}

} // calico