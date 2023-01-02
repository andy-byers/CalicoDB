#include "bplus_tree.h"
#include "calico/cursor.h"
#include "cursor_internal.h"
#include "page/node.h"
#include "pager/pager.h"
#include "utils/layout.h"
#include "utils/system.h"

namespace Calico {

BPlusTree::BPlusTree(Pager &pager, System &system, Size page_size)
    : m_pool {pager, system, page_size},
      m_internal {m_pool, system},
      m_logger {system.create_log("tree")},
      m_system {&system}
{
    m_logger->info("constructing tree");
    m_actions.acquire = [this](auto pid, auto is_writable) {
        return m_pool.acquire(pid, is_writable);
    };
    m_actions.release = [this](auto node) {
        return m_pool.release(std::move(node));
    };
    m_actions.collect = [this](auto &node, auto index) {
        return m_internal.collect_value(node, index);
    };
}

BPlusTree::~BPlusTree()
{
    m_logger->info("destroying tree");
}

auto BPlusTree::open(Pager &pager, System &system, size_t page_size) -> tl::expected<Tree::Ptr, Status>
{
    auto ptr = Tree::Ptr {new(std::nothrow) BPlusTree {pager, system, page_size}};
    if (ptr == nullptr)
        return tl::make_unexpected(system_error("could not allocate tree: out of memory"));
    return ptr;
}

auto BPlusTree::check_key(BytesView key, const char *primary) -> Status
{
    if (key.is_empty()) {
        auto s = invalid_argument("{}: key is empty (use a nonempty key)", primary);
        CALICO_WARN(s);
        return s;
    }
    if (key.size() > m_internal.maximum_key_size()) {
        auto s = invalid_argument("{}: key of length {} B is too long", primary, key.size(), m_internal.maximum_key_size());
        CALICO_WARN(s);
        return s;
    }
    return ok();
}

auto BPlusTree::insert(BytesView key, BytesView value) -> Status
{
    CALICO_TRY_S(check_key(key, "could not insert record"));

    // Find the external node that the record should live in, given its key.
    auto search_result = m_internal.find_external(key);
    if (!search_result.has_value()) return search_result.error();
    auto [id, index, found_eq] = *search_result;
    auto node = m_pool.acquire(id, true);
    if (!node.has_value()) return node.error();

    if (found_eq) {
        const auto r = m_internal.positioned_modify({std::move(*node), index}, value);
        if (!r.has_value()) return r.error();
    } else {
        const auto r = m_internal.positioned_insert({std::move(*node), index}, key, value);
        if (!r.has_value()) return r.error();
    }
    return ok();
}

auto BPlusTree::erase(Cursor cursor) -> Status
{
    if (cursor.is_valid()) {
        auto node = m_pool.acquire(Id {CursorInternal::id(cursor)}, true);
        if (!node.has_value()) return node.error();
        const auto r = m_internal.positioned_remove({std::move(*node), CursorInternal::index(cursor)});
        if (!r.has_value()) return r.error();
        return ok();
    }
    CALICO_EXPECT_FALSE(cursor.status().is_ok());
    return cursor.status();
}

auto BPlusTree::find_aux(BytesView key) -> tl::expected<SearchResult, Status>
{
    if (auto s = check_key(key, "could not find key"); !s.is_ok())
        return tl::make_unexpected(s);

    CALICO_NEW_R(find_result, m_internal.find_external(key));
    auto [id, index, found_exact] = find_result;
    CALICO_NEW_R(node, m_pool.acquire(id, false));

    // TODO: We shouldn't even need to check the cell count here. Instead, we should make sure all the pointers are updated.
    //       There shouldn't be a right sibling ID, i.e. it should be 0, if the cell count is 0.
    if (m_internal.cell_count() && index == node.cell_count() && !node.right_sibling_id().is_null()) {
        CALICO_EXPECT_FALSE(found_exact);
        id = node.right_sibling_id();
        CALICO_TRY_R(m_pool.release(std::move(node)));
        CALICO_PUT_R(node, m_pool.acquire(id, false));
        index = 0;
    }
    return SearchResult {std::move(node), index, found_exact};
}

auto BPlusTree::find_exact(BytesView key) -> Cursor
{
    auto cursor = CursorInternal::make_cursor(&m_actions);
    auto result = find_aux(key);
    if (!result.has_value()) {
        CursorInternal::invalidate(cursor, result.error());
    } else if (result->was_found) {
        auto [node, index, found_exact] = std::move(*result);
        CALICO_EXPECT_TRUE(found_exact);
        CursorInternal::move_to(cursor, std::move(node), index);
    }
    return cursor;
}

auto BPlusTree::find(BytesView key) -> Cursor
{
    auto cursor = CursorInternal::make_cursor(&m_actions);
    auto result = find_aux(key);
    if (!result.has_value()) {
        CursorInternal::invalidate(cursor, result.error());
    } else {
        auto [node, index, found_exact] = std::move(*result);
        CursorInternal::move_to(cursor, std::move(node), index);
    }
    return cursor;
}

auto BPlusTree::find_minimum() -> Cursor
{
    auto cursor = CursorInternal::make_cursor(&m_actions);
    auto temp = m_internal.find_minimum();
    if (!temp.has_value()) {
        CursorInternal::invalidate(cursor, temp.error());
        return cursor;
    }
    auto [id, index, was_found] = *temp;
    if (!was_found) {
        CursorInternal::invalidate(cursor);
        return cursor;
    }
    auto node = m_pool.acquire(id, false);
    if (!node.has_value()) {
        CursorInternal::invalidate(cursor, node.error());
        return cursor;
    }
    CALICO_EXPECT_EQ(index, 0);
    CursorInternal::move_to(cursor, std::move(*node), index);
    return cursor;
}

auto BPlusTree::find_maximum() -> Cursor
{
    auto cursor = CursorInternal::make_cursor(&m_actions);
    auto temp = m_internal.find_maximum();
    if (!temp.has_value()) {
        CursorInternal::invalidate(cursor, temp.error());
        return cursor;
    }
    auto [id, index, was_found] = *temp;
    if (!was_found) {
        CursorInternal::invalidate(cursor);
        return cursor;
    }
    auto node = m_pool.acquire(id, false);
    if (!node.has_value()) {
        CursorInternal::invalidate(cursor, node.error());
        return cursor;
    }
    CALICO_EXPECT_EQ(index, node->cell_count() - 1);
    CursorInternal::move_to(cursor, std::move(*node), index);
    return cursor;
}

auto BPlusTree::root(bool is_writable) -> tl::expected<Node, Status>
{
    if (m_pool.page_count() == 0)
        return m_pool.allocate(PageType::EXTERNAL_NODE);
    return m_internal.find_root(is_writable);
}

auto BPlusTree::save_state(FileHeader &header) const -> void
{
    m_internal.save_state(header);
}

auto BPlusTree::load_state(const FileHeader &header) -> void
{
    m_internal.load_state(header);
}

#undef CHECK_KEY

#if not NDEBUG

using Callback = std::function<void(Node&, Size)>;

static auto traverse_inorder_helper(NodeManager &pool, Node node, const Callback &callback) -> void
{
    for (Size index {}; index <= node.cell_count(); ++index) {
        if (!node.is_external()) {
            auto next = pool.acquire(node.child_id(index), false);
            CALICO_EXPECT_TRUE(next.has_value());
            traverse_inorder_helper(pool, std::move(*next), callback);
        }
        if (index < node.cell_count())
            callback(node, index);
    }
    CALICO_EXPECT_TRUE(pool.release(std::move(node)).has_value());
}

static auto traverse_inorder(NodeManager &pool, const Callback &callback) -> void
{
    auto root = pool.acquire(Id::root(), false);
    CALICO_EXPECT_TRUE(root.has_value());
    traverse_inorder_helper(pool, std::move(*root), callback);
}

static auto validate_siblings(NodeManager &pool) -> void
{
    // Find the leftmost external node.
    auto node = *pool.acquire(Id::root(), false);
    while (!node.is_external()) {
        const auto id = node.child_id(0);
        CALICO_EXPECT_TRUE(pool.release(std::move(node)).has_value());
        auto temp = pool.acquire(id, false);
        CALICO_EXPECT_TRUE(temp.has_value());
        node = std::move(*temp);
    }
    // Traverse across the sibling chain to the right.
    while (!node.right_sibling_id().is_null()) {
        auto right = pool.acquire(node.right_sibling_id(), false);
        CALICO_EXPECT_TRUE(right.has_value());
        CALICO_EXPECT_LT(node.read_key(0), right->read_key(0));
        CALICO_EXPECT_EQ(right->left_sibling_id(), node.id());
        CALICO_EXPECT_TRUE(pool.release(std::move(node)).has_value());
        node = std::move(*right);
    }
    CALICO_EXPECT_TRUE(pool.release(std::move(node)).has_value());
}

auto validate_parent_child(NodeManager &pool) -> void
{
    auto check_connection = [&pool](Node &node, Size index) -> void {
        auto child = *pool.acquire(node.child_id(index), false);
        CALICO_EXPECT_EQ(child.parent_id(), node.id());
        CALICO_EXPECT_TRUE(pool.release(std::move(child)).has_value());
    };
    traverse_inorder(pool, [&check_connection](Node &node, Size index) -> void {
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

#endif // not NDEBUG

auto BPlusTree::TEST_validate_order() -> void
{
#if not NDEBUG
    // NOTE: All keys must fit in main memory (separators included). Doesn't read values.
    if (record_count() < 2)
        return;

    std::vector<std::string> keys;
    keys.reserve(record_count());

    traverse_inorder(m_pool, [&keys](Node &node, Size index) -> void {
        keys.emplace_back(node.read_key(index).to_string());
    });
    CALICO_EXPECT_TRUE(std::is_sorted(cbegin(keys), cend(keys)));
#endif // not NDEBUG
}

auto BPlusTree::TEST_validate_nodes() -> void
{
#if not NDEBUG
    traverse_inorder(m_pool, [](Node &node, Size index) -> void {
        // Only validate once per node.
        if (index == 0) node.TEST_validate();
    });
#endif // not NDEBUG
}

auto BPlusTree::TEST_validate_links() -> void
{
#if not NDEBUG
    validate_siblings(m_pool);
    validate_parent_child(m_pool);
#endif // not NDEBUG
}

} // namespace Calico