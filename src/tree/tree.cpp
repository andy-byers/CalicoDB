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
    if (key.is_empty()) {
        logging::MessageGroup group;
        group.set_primary("cannot write record");
        group.set_detail("key is empty");
        throw std::invalid_argument {group.err(*m_logger)};
    }

    auto [node, index, found_eq] = m_internal.find_external(key, true);

    if (key.size() > get_max_local(node.size())) {
        logging::MessageGroup group;
        group.set_primary("cannot write record");
        group.set_detail("key of length {} B is too long", key.size());
        group.set_hint("maximum key length is {} B", get_max_local(m_pool.page_size()));
        throw std::invalid_argument {group.err(*m_logger)};
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

auto Tree::find(BytesView key, bool require_exact) -> Cursor
{
    CALICO_EXPECT_FALSE(key.is_empty());
    auto [node, index, found_exact] = m_internal.find_external(key, false);
    if (index == node.cell_count() && !node.right_sibling_id().is_null()) {
        CALICO_EXPECT_FALSE(found_exact);
        node = m_pool.acquire(node.right_sibling_id(), false);
        index = 0;
    }
    Cursor cursor {&m_pool, &m_internal};
    if (found_exact || !require_exact)
        cursor.move_to(std::move(node), index);
    return cursor;
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