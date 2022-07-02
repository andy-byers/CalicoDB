#include "validation.h"
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

auto validate_siblings(ITree &tree) -> void
{
    for (auto node = find_minimum(tree); has_next(node); node = get_next(tree.pool(), node)) {
        auto right = tree.pool().acquire(node.right_sibling_id(), false);
        CALICO_EXPECT_LT(node.read_key(0), right.read_key(0));
        CALICO_EXPECT_EQ(right.left_sibling_id(), node.id());
    }
}

auto validate_ordering(ITree &tree) -> void
{
    auto lhs = tree.find_minimum();
    auto rhs = tree.find_minimum();

    if (!lhs.is_valid())
        return;
    CALICO_EXPECT_TRUE(rhs.is_valid());
    CALICO_EXPECT_TRUE(rhs.increment());

    for (; rhs.is_valid(); lhs++, rhs++) {
        CALICO_EXPECT_TRUE(lhs.is_valid());
        CALICO_EXPECT_TRUE(lhs.key() < rhs.key());
    }
//    const auto validate = [](const Node &node) {
//        for (Index i {}; i + 1 < node.cell_count(); ++i) {
//            const auto lhs = node.read_key(i);
//            const auto rhs = node.read_key(i + 1);
//            CALICO_EXPECT_LT(lhs, rhs);
//        }
//    };
//
//    for (auto lhs = find_minimum(tree); has_next(lhs); lhs = get_next(tree.pool(), lhs)) {
//        const auto rhs = get_next(tree.pool(), lhs);
//        CALICO_EXPECT_LT(lhs.read_key(0), rhs.read_key(0));
//        CALICO_EXPECT_EQ(rhs.left_sibling_id(), lhs.id());
//        validate(lhs);
//
//        if (!has_next(rhs))
//            validate(rhs);
//    }
}

auto validate_links(ITree&) -> void
{

}

} // calico