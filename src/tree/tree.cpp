#include "tree.h"
#include "calico/cursor.h"
#include "page/file_header.h"
#include "page/node.h"
#include "pool/interface.h"
#include "utils/layout.h"
#include "utils/logging.h"

namespace calico {

Tree::Tree(Parameters param)
    : m_pool {{param.buffer_pool, param.free_start, param.free_count, param.node_count}},
      m_internal {{&m_pool, param.cell_count}},
      m_logger {logging::create_logger(param.log_sink, "Tree")}
{
    m_logger->trace("constructing Tree object");

    if (m_pool.node_count() == 0)
        m_pool.allocate(PageType::EXTERNAL_NODE);
}

auto Tree::insert(BytesView key, BytesView value) -> bool
{
    static constexpr auto ERROR_PRIMARY = "cannot write record";

    if (key.is_empty()) {
        logging::MessageGroup group;
        group.set_primary(ERROR_PRIMARY);
        group.set_detail("key is empty");
        group.set_hint("use a nonempty key");
        throw std::invalid_argument {group.error(*m_logger)};
    }

    auto [node, index, found_eq] = m_internal.find_external(key, true);

    if (key.size() > get_max_local(node.size())) {
        logging::MessageGroup group;
        group.set_primary(ERROR_PRIMARY);
        group.set_detail("key of length {} B is too long", key.size());
        group.set_hint("maximum key length is {} B", get_max_local(m_pool.page_size()));
        throw std::invalid_argument {group.error(*m_logger)};
    }

    if (found_eq) {
        m_internal.positioned_modify({std::move(node), index}, value);
        return false;
    } else {
        m_internal.positioned_insert({std::move(node), index}, key, value);
        return true;
    }
}

auto Tree::erase(Cursor cursor) -> bool
{
    if (cursor.is_valid()) {
        m_internal.positioned_remove({
            m_pool.acquire(PID {cursor.id()}, true),
            cursor.index(),
        });
        return true;
    }
    return false;
}

auto maybe_reposition(NodePool &pool, Node &node, Index &index)
{
    if (index == node.cell_count() && !node.right_sibling_id().is_null()) {
        node = pool.acquire(node.right_sibling_id(), false);
        index = 0;
    }
}

auto Tree::find_aux(BytesView key, bool &found_exact_out) -> Cursor
{
    CALICO_EXPECT_FALSE(key.is_empty());
    auto [node, index, found_exact] = m_internal.find_external(key, false);

    if (index == node.cell_count() && !node.right_sibling_id().is_null()) {
        node = m_pool.acquire(node.right_sibling_id(), false);
        index = 0;
    }
    Cursor cursor {&m_pool, &m_internal};
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
    auto [node, index] = m_internal.find_local_min(root(false));
    CALICO_EXPECT_EQ(index, 0);
    Cursor cursor {&m_pool, &m_internal};
    cursor.move_to(std::move(node), index);
    return cursor;
}

auto Tree::find_maximum() -> Cursor
{
    auto [node, index] = m_internal.find_local_max(root(false));
    CALICO_EXPECT_EQ(index, node.cell_count() - 1);
    Cursor cursor {&m_pool, &m_internal};
    cursor.move_to(std::move(node), index);
    return cursor;
}

auto Tree::root(bool is_writable) -> Node
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