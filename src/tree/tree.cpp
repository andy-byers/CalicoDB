#include "tree.h"
#include "calico/cursor.h"
#include "cursor_internal.h"
#include "page/file_header.h"
#include "page/node.h"
#include "pool/interface.h"
#include "utils/layout.h"
#include "utils/logging.h"

namespace cco {

Tree::Tree(const Parameters &param)
    : m_pool {{param.buffer_pool, param.free_start}},
      m_internal {{&m_pool, param.cell_count}},
      m_logger {create_logger(param.log_sink, "tree")}
{}

auto Tree::open(const Parameters &param) -> Result<std::unique_ptr<ITree>>
{
    auto tree = std::unique_ptr<Tree>(new Tree {param});
    tree->m_logger->trace("opening");
    return tree;
}

auto Tree::allocate_root() -> Result<Node>
{
    return m_pool.allocate(PageType::EXTERNAL_NODE);
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

    CCO_TRY_CREATE(search_result, m_internal.find_external(key));
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
        CCO_TRY_CREATE(node, m_pool.acquire(PageId {CursorInternal::id(cursor)}, true));
        CCO_TRY(m_internal.positioned_remove({std::move(node), CursorInternal::index(cursor)}));
        return true;
    } else if (!cursor.status().is_ok()) {
        return Err {cursor.status()};
    }
    return false;
}

auto Tree::find_aux(BytesView key) -> Result<SearchResult>
{
    if (const auto r = run_key_check(key, m_internal.maximum_key_size(), *m_logger, "cannot write record"); !r.is_ok())
        return Err {r};

    CCO_TRY_CREATE(find_result, m_internal.find_external(key));
    auto [id, index, found_exact] = find_result;
    CCO_TRY_CREATE(node, m_pool.acquire(id, false));

    // TODO: We shouldn't even need to check the cell count here. Instead, we should make sure all the pointers are updated.
    //       There shouldn't be a right sibling ID, i.e. it should be 0, if the cell count is 0.
    if (m_internal.cell_count() && index == node.cell_count() && !node.right_sibling_id().is_null()) {
        CCO_EXPECT_FALSE(found_exact);
        id = node.right_sibling_id();
        CCO_TRY(m_pool.release(std::move(node)));
        CCO_TRY_CREATE(next, m_pool.acquire(id, false));
        node = std::move(next);
        index = 0;
    }
    return SearchResult {std::move(node), index, found_exact};
}

auto Tree::find_exact(BytesView key) -> Cursor
{
    auto cursor = CursorInternal::make_cursor(m_pool, m_internal);
    auto result = find_aux(key);
    if (!result.has_value()) {
        CursorInternal::invalidate(cursor, result.error());
    } else if (result->was_found) {
        auto [node, index, found_exact] = std::move(*result);
        CCO_EXPECT_TRUE(found_exact);
        CursorInternal::move_to(cursor, std::move(node), index);
    }
    return cursor;
}

auto Tree::find(BytesView key) -> Cursor
{
    auto cursor = CursorInternal::make_cursor(m_pool, m_internal);
    auto result = find_aux(key);
    if (!result.has_value()) {
        CursorInternal::invalidate(cursor, result.error());
    } else {
        auto [node, index, found_exact] = std::move(*result);
        CursorInternal::move_to(cursor, std::move(node), index);
    }
    return cursor;
}

auto Tree::find_minimum() -> Cursor
{
    auto cursor = CursorInternal::make_cursor(m_pool, m_internal);
    auto temp = m_internal.find_minimum();
    if (!temp.has_value()) {
        CursorInternal::invalidate(cursor, temp.error());
        return cursor;
    }
    auto [id, index, was_found] = *temp;
    if (!was_found) {
        CursorInternal::invalidate(cursor, Status::not_found());
        return cursor;
    }
    auto node = m_pool.acquire(id, false);
    if (!node.has_value()) {
        CursorInternal::invalidate(cursor, temp.error());
        return cursor;
    }
    CCO_EXPECT_EQ(index, 0);
    CursorInternal::move_to(cursor, std::move(*node), index);
    return cursor;
}

auto Tree::find_maximum() -> Cursor
{
    auto cursor = CursorInternal::make_cursor(m_pool, m_internal);
    auto temp = m_internal.find_maximum();
    if (!temp.has_value()) {
        CursorInternal::invalidate(cursor, temp.error());
        return cursor;
    }
    auto [id, index, was_found] = *temp;
    if (!was_found) {
        CursorInternal::invalidate(cursor, Status::not_found());
        return cursor;
    }
    auto node = m_pool.acquire(id, false);
    if (!node.has_value()) {
        CursorInternal::invalidate(cursor, temp.error());
        return cursor;
    }
    CCO_EXPECT_EQ(index, node->cell_count() - 1);
    CursorInternal::move_to(cursor, std::move(*node), index);
    return cursor;
}

auto Tree::root(bool is_writable) -> Result<Node>
{
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

auto Tree::TEST_validate_node(PageId id) -> void
{
    auto result = m_pool.acquire(id, false);
    if (!result.has_value()) {
        fmt::print("(1/2) tree: cannot acquire node {}", id.value);
        fmt::print("(2/2) tree: (reason) {}", result.error().what());
        std::exit(EXIT_FAILURE);
    }
    auto node = std::move(*result);
    node.TEST_validate();
}

} // namespace cco