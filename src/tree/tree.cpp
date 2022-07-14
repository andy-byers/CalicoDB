#include "tree.h"
#include "calico/cursor.h"
#include "page/file_header.h"
#include "page/node.h"
#include "pool/interface.h"
#include "utils/layout.h"
#include "utils/logging.h"

namespace cco {

using namespace page;
using namespace utils;

Tree::Tree(Parameters param)
    : m_pool {{param.buffer_pool, param.free_start, param.free_count, param.node_count}},
      m_internal {{&m_pool, param.cell_count}},
      m_logger {create_logger(param.log_sink, "Tree")}
{
    m_logger->trace("constructing Tree object");
}

auto Tree::open(Parameters param) -> Result<std::unique_ptr<ITree>>
{
    auto tree = std::unique_ptr<Tree>(new Tree {std::move(param)});
    if (tree->m_pool.node_count() == 0) {
        CCO_TRY_CREATE(root, tree->m_pool.allocate(PageType::EXTERNAL_NODE));
        CCO_TRY(tree->m_pool.release(std::move(root)));
    }
    return tree;
}

auto Tree::insert(BytesView key, BytesView value) -> Result<bool>
{
    static constexpr auto ERROR_PRIMARY = "cannot write record";

    if (key.is_empty()) {
        LogMessage message {*m_logger};
        message.set_primary(ERROR_PRIMARY);
        message.set_detail("key is empty");
        message.set_hint("use a nonempty key");
        return Err {message.invalid_argument()};
    }

    if (key.size() > get_max_local(m_pool.page_size())) {
        LogMessage message {*m_logger};
        message.set_primary(ERROR_PRIMARY);
        message.set_detail("key of length {} B is too long", key.size());
        message.set_hint("maximum key length is {} B", get_max_local(m_pool.page_size()));
        return Err {message.invalid_argument()};
    }
    CCO_TRY_CREATE(was_found, m_internal.find_external(key, true));
    auto [node, index, found_eq] = std::move(was_found);

    if (found_eq) {
        CCO_TRY(m_internal.positioned_modify({std::move(node), index}, value));
        return false;
    } else {
        CCO_TRY(m_internal.positioned_insert({std::move(node), index}, key, value));
        return true;
    }
}

auto Tree::erase(Cursor cursor) -> Result<bool>
{
    if (cursor.is_valid()) {
        CCO_TRY_CREATE(node, m_pool.acquire(PID {cursor.id()}, true));
        CCO_TRY(m_internal.positioned_remove({std::move(node), cursor.index()}));
        return true;
    }
    return false;
}

auto maybe_reposition(NodePool &pool, Node &node, Index &index) -> Result<void>
{
    if (index == node.cell_count() && !node.right_sibling_id().is_null()) {
        CCO_TRY_STORE(node, pool.acquire(node.right_sibling_id(), false));
        index = 0;
    }
    return {};
}

auto Tree::find_aux(BytesView key, bool &found_exact_out) -> Cursor
{
    CCO_EXPECT_FALSE(key.is_empty());
    Cursor cursor {&m_pool, &m_internal};
    auto was_found = m_internal.find_external(key, false);
    if (!was_found.has_value()) {
        cursor.set_error(was_found.error());
        return cursor;
    }
    auto [node, index, found_exact] = std::move(*was_found);

    if (index == node.cell_count() && !node.right_sibling_id().is_null()) {
        const auto id = node.right_sibling_id();
        if (auto result = m_pool.release(std::move(node)); !result.has_value()) {
            cursor.set_error(result.error());
            return cursor;
        }
        if (auto next = m_pool.acquire(id, false)) {
            node = std::move(*next);
            index = 0;
        } else {
            cursor.set_error(next.error());
            return cursor;
        }
    }
    cursor.move_to(std::move(node), index);
    found_exact_out = found_exact;
    return cursor;
}

auto Tree::find_exact(BytesView key) -> Cursor
{
    bool found_exact {};
    auto cursor = find_aux(key, found_exact);
    if (!found_exact)
        cursor.invalidate();
    return cursor;
}

auto Tree::find(BytesView key) -> Cursor
{
    bool found_exact {};
    return find_aux(key, found_exact);
}

auto Tree::find_minimum() -> Cursor
{
    Cursor cursor {&m_pool, &m_internal};
    auto root = Tree::root(false);
    if (!root.has_value()) {
        cursor.set_error(root.error());
        return cursor;
    }
    auto temp = m_internal.find_local_min(std::move(*root));
    if (!temp.has_value()) {
        cursor.set_error(temp.error());
        return cursor;
    }
    auto [node, index] = std::move(*temp);
    CCO_EXPECT_EQ(index, 0);
    cursor.move_to(std::move(node), index);
    return cursor;
}

auto Tree::find_maximum() -> Cursor
{
    Cursor cursor {&m_pool, &m_internal};
    auto root = Tree::root(false);
    if (!root.has_value()) {
        cursor.set_error(root.error());
        return cursor;
    }
    auto temp = m_internal.find_local_max(std::move(*root));
    if (!temp.has_value()) {
        cursor.set_error(temp.error());
        return cursor;
    }
    auto [node, index] = std::move(*temp);
    CCO_EXPECT_EQ(index, node.cell_count() - 1);
    cursor.move_to(std::move(node), index);
    return cursor;
}

auto Tree::root(bool is_writable) -> Result<Node>
{
    return m_internal.find_root(is_writable);
}

auto Tree::save_header(FileHeader &header) const -> void
{
    m_internal.save_header(header);
}

auto Tree::load_header(const FileHeader &header) -> void
{
    m_internal.load_header(header);
}

} // calico