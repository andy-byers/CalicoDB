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

Tree::Tree(const Parameters &param)
    : m_pool {{param.buffer_pool, param.free_start, param.free_count, param.node_count}},
      m_internal {{&m_pool, param.cell_count}},
      m_logger {create_logger(param.log_sink, "tree")} {}

auto Tree::open(const Parameters &param) -> Result<std::unique_ptr<ITree>>
{
    auto tree = std::unique_ptr<Tree>(new Tree {param});
    tree->m_logger->trace("opening");
    return tree;
}

auto Tree::allocate_root() -> Result<page::Node>
{
    CCO_EXPECT_EQ(m_pool.node_count(), 0);
    CCO_TRY_CREATE(root, m_pool.allocate(PageType::EXTERNAL_NODE));
    return root;
}

auto run_key_check(BytesView key, Size max_key_size, spdlog::logger &logger, const std::string &primary) -> Status
{
    if (key.is_empty()) {
        LogMessage message {logger};
        message.set_primary(primary);
        message.set_detail("key is empty");
        message.set_hint("use a nonempty key");
        return message.invalid_argument();
    }

    if (key.size() > max_key_size) {
        LogMessage message {logger};
        message.set_primary(primary);
        message.set_detail("key of length {} B is too long", key.size());
        message.set_hint("maximum key length is {} B", max_key_size);
        return message.invalid_argument();
    }
    return Status::ok();
}

auto Tree::insert(BytesView key, BytesView value) -> Result<bool>
{
    if (const auto r = run_key_check(key, m_internal.maximum_key_size(), *m_logger, "cannot write record"); !r.is_ok())
        return Err {r};

    CCO_TRY_CREATE(search_result, m_internal.find_external_(key));
    auto [id, index, found_eq] = search_result;
    CCO_TRY_CREATE(node, m_pool.acquire(id, true));

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
    } else if (!cursor.status().is_ok()) {
        return Err {cursor.status()};
    }
    return false;
}

auto Tree::find_aux(BytesView key) -> Result<Internal::FindResult>
{
    if (const auto r = run_key_check(key, m_internal.maximum_key_size(), *m_logger, "cannot write record"); !r.is_ok())
        return Err {r};

    CCO_TRY_CREATE(find_result, m_internal.find_external_(key));
    auto [id, index, found_exact] = find_result;
    CCO_TRY_CREATE(node, m_pool.acquire(id, false));

    if (index == node.cell_count() && !node.right_sibling_id().is_null()) {
        CCO_EXPECT_FALSE(found_exact);
        id = node.right_sibling_id();
        CCO_TRY(m_pool.release(std::move(node)));
        CCO_TRY_CREATE(next, m_pool.acquire(id, false));
        node = std::move(next);
        index = 0;
    }
    return Internal::FindResult {std::move(node), index, found_exact};
}

auto Tree::find_exact(BytesView key) -> Cursor
{
    Cursor cursor {&m_pool, &m_internal};
    auto result = find_aux(key);
    if (!result.has_value()) {
        cursor.invalidate(result.error());
    } else if (result->flag) {
        auto [node, index, found_exact] = std::move(*result);
        CCO_EXPECT_TRUE(found_exact);
        cursor.move_to(std::move(node), index);
    }
    return cursor;
}

auto Tree::find(BytesView key) -> Cursor
{
    Cursor cursor {&m_pool, &m_internal};
    auto result = find_aux(key);
    if (!result.has_value()) {
        cursor.invalidate(result.error());
    } else {
        auto [node, index, found_exact] = std::move(*result);
        cursor.move_to(std::move(node), index);
    }
    return cursor;
}

auto Tree::find_minimum() -> Cursor
{
    Cursor cursor {&m_pool, &m_internal};
    auto temp = m_internal.find_minimum();
    if (!temp.has_value()) {
        cursor.invalidate(temp.error());
        return cursor;
    }
    auto [id, index, was_found] = *temp;
    if (!was_found) {
        cursor.invalidate(Status::not_found());
        return cursor;
    }
    auto node = m_pool.acquire(id, false);
    if (!node.has_value()) {
        cursor.invalidate(temp.error());
        return cursor;
    }
    CCO_EXPECT_EQ(index, 0);
    cursor.move_to(std::move(*node), index);
    return cursor;
}

auto Tree::find_maximum() -> Cursor
{
    Cursor cursor {&m_pool, &m_internal};
    auto temp = m_internal.find_maximum();
    if (!temp.has_value()) {
        cursor.invalidate(temp.error());
        return cursor;
    }
    auto [id, index, was_found] = *temp;
    if (!was_found) {
        cursor.invalidate(Status::not_found());
        return cursor;
    }
    auto node = m_pool.acquire(id, false);
    if (!node.has_value()) {
        cursor.invalidate(temp.error());
        return cursor;
    }
    CCO_EXPECT_EQ(index, node->cell_count() - 1);
    cursor.move_to(std::move(*node), index);
    return cursor;
}

auto Tree::root(bool is_writable) -> Result<Node>
{
    CCO_EXPECT_GT(m_pool.node_count(), 0);
    return m_internal.find_root(is_writable);
}

auto Tree::save_header(FileHeaderWriter &header) const -> void
{
    m_internal.save_header(header);
}

auto Tree::load_header(const FileHeaderReader &header) -> void
{
    m_internal.load_header(header);
}

} // cco