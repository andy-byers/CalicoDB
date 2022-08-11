#include "bplus_tree.h"
#include "calico/cursor.h"
#include "cursor_internal.h"
#include "page/node.h"
#include "pager/pager.h"
#include "utils/layout.h"
#include "utils/logging.h"

namespace cco {

BPlusTree::BPlusTree(Pager &pager, spdlog::sink_ptr log_sink, Size page_size)
    : m_pool {pager, page_size},
      m_internal {m_pool},
      m_logger {create_logger(std::move(log_sink), "tree")}
{}

auto BPlusTree::open(Pager &pager, spdlog::sink_ptr log_sink, Size page_size) -> Result<std::unique_ptr<BPlusTree>>
{
    auto tree = std::unique_ptr<BPlusTree>(new BPlusTree {pager, std::move(log_sink), page_size});
    tree->m_logger->trace("opening");

    // Allocate the root page.
    if (pager.page_count() == 0) {
        auto r1 = tree->m_pool.allocate(PageType::EXTERNAL_NODE);
        auto r2 = tree->m_pool.release(std::move(*r1));
        auto s = !r1 ? r1.error() : (!r2 ? r2.error() : Status::ok());
        if (!s.is_ok()) {
            tree->m_logger->error("cannot allocate root page");
            tree->m_logger->error("(reason) {}", s.what());
            return Err {s};
        }
    }
    return tree;
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

auto BPlusTree::insert(BytesView key, BytesView value) -> Result<bool>
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

auto BPlusTree::erase(Cursor cursor) -> Result<bool>
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

auto BPlusTree::find_aux(BytesView key) -> Result<SearchResult>
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

auto BPlusTree::find_exact(BytesView key) -> Cursor
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

auto BPlusTree::find(BytesView key) -> Cursor
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

auto BPlusTree::find_minimum() -> Cursor
{
    auto cursor = CursorInternal::make_cursor(m_pool, m_internal);
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
        CursorInternal::invalidate(cursor, temp.error());
        return cursor;
    }
    CCO_EXPECT_EQ(index, 0);
    CursorInternal::move_to(cursor, std::move(*node), index);
    return cursor;
}

auto BPlusTree::find_maximum() -> Cursor
{
    auto cursor = CursorInternal::make_cursor(m_pool, m_internal);
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
        CursorInternal::invalidate(cursor, temp.error());
        return cursor;
    }
    CCO_EXPECT_EQ(index, node->cell_count() - 1);
    CursorInternal::move_to(cursor, std::move(*node), index);
    return cursor;
}

auto BPlusTree::root(bool is_writable) -> Result<Node>
{
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

auto BPlusTree::TEST_validate_node(PageId id) -> void
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