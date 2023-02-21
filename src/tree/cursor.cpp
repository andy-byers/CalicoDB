#include "cursor_impl.h"
#include "node.h"
#include "pager/pager.h"
#include "tree.h"

namespace Calico {

[[nodiscard]]
static auto default_error_status() -> Status
{
    return Status::not_found("cursor is invalid");
}

auto CursorImpl::is_valid() const -> bool
{
    return m_status.is_ok();
}

auto CursorImpl::status() const -> Status
{
    return m_status;
}

auto CursorImpl::key() const -> Slice
{
    CALICO_EXPECT_TRUE(is_valid());
    if (m_key_size == 0) {
        if (auto node = m_tree->acquire(m_loc.pid)) {
            auto cell = read_cell(*node, m_loc.index);
            if (auto key = m_tree->collect_key(m_key, cell)) {
                m_key_size = key->size();
                // Go ahead and read the value if it isn't on overflow pages.
                if (m_value.empty() && !cell.has_remote) {
                    m_value = Slice {
                        cell.key + cell.key_size,
                        cell.local_size - cell.key_size,
                    }.to_string();
                    m_value_size = m_value.size();
                }
            } else if (m_status.is_ok()) {
                m_status = key.error();
            }
            m_tree->release(std::move(*node));
        } else if (m_status.is_ok()) {
            m_status = node.error();
        }
    }
    return Slice {m_key}.truncate(m_key_size);
}

auto CursorImpl::value() const -> Slice
{
    CALICO_EXPECT_TRUE(is_valid());
    if (m_value_size == 0) {
        if (auto node = m_tree->acquire(m_loc.pid)) {
            const auto cell = read_cell(*node, m_loc.index);
            if (auto value = m_tree->collect_value(m_value, cell)) {
                m_value_size = value->size();
            } else if (m_status.is_ok()) {
                m_status = value.error();
            }
            m_tree->release(std::move(*node));
        } else if (m_status.is_ok()) {
            m_status = node.error();
        }
    }
    return Slice {m_value}.truncate(m_value_size);
}

auto CursorImpl::seek_first() -> void
{
    if (auto lowest = m_tree->lowest()) {
        if (lowest->header.cell_count) {
            seek_to(std::move(*lowest), 0);
        } else {
            m_tree->release(std::move(*lowest));
            m_status = Status::not_found("database is empty");
        }
    } else {
        m_status = lowest.error();
    }
}

auto CursorImpl::seek_last() -> void
{
    if (auto highest = m_tree->highest()) {
        if (const auto count = highest->header.cell_count) {
            seek_to(std::move(*highest), count - 1);
        } else {
            m_tree->release(std::move(*highest));
            m_status = Status::not_found("database is empty");
        }
    } else {
        m_status = highest.error();
    }
}

auto CursorImpl::next() -> void
{
    CALICO_EXPECT_TRUE(is_valid());
    m_key_size = 0;
    m_value_size = 0;
    if (++m_loc.index < m_loc.count) {
        return;
    }
    if (auto node = m_tree->acquire(Id {m_loc.pid})) {
        const auto next_id = node->header.next_id;
        m_tree->release(std::move(*node));

        if (next_id.is_null()) {
            m_status = default_error_status();
            return;
        }
        node = m_tree->acquire(next_id);
        if (!node.has_value()) {
            m_status = node.error();
            return;
        }
        m_loc.pid = node->page.id();
        m_loc.count = node->header.cell_count;
        m_loc.index = 0;
        m_tree->release(std::move(*node));
    } else {
        m_status = node.error();
    }
}

auto CursorImpl::previous() -> void
{
    if (!is_valid()) {
        return;
    }
    m_key_size = 0;
    m_value_size = 0;
    if (m_loc.index != 0) {
        m_loc.index--;
    } else if (auto node = m_tree->acquire(Id {m_loc.pid})) {
        const auto prev_id = node->header.prev_id;
        m_tree->release(std::move(*node));

        if (prev_id.is_null()) {
            m_status = default_error_status();
            return;
        }
        node = m_tree->acquire(prev_id);
        if (!node.has_value()) {
            m_status = node.error();
            return;
        }
        m_loc.pid = node->page.id();
        m_loc.count = node->header.cell_count;
        m_loc.index = node->header.cell_count - 1;
        m_tree->release(std::move(*node));
    } else {
        m_status = node.error();
    }
}

auto CursorImpl::seek_to(Node node, Size index) -> void
{
    CALICO_EXPECT_TRUE(node.header.is_external);
    const auto pid = node.page.id();
    const auto count = node.header.cell_count;
    m_tree->release(std::move(node));

    m_key_size = 0;
    m_value_size = 0;

    if (count && index < count) {
        m_loc.index = static_cast<PageSize>(index);
        m_loc.count = static_cast<PageSize>(count);
        m_loc.pid = pid;
        m_status = Status::ok();
    } else {
        m_status = default_error_status();
    }
}

auto CursorImpl::seek(const Slice &key) -> void
{
    if (auto slot = m_tree->search(key)) {
        auto [node, index, _] = std::move(*slot);
        seek_to(std::move(node), index);
    } else {
        m_status = slot.error();
    }
}

auto CursorInternal::make_cursor(BPlusTree &tree) -> Cursor *
{
    auto *cursor = new(std::nothrow) CursorImpl {tree};
    if (cursor != nullptr) {
        invalidate(*cursor, default_error_status());
    }
    return cursor;
}

auto CursorInternal::invalidate(const Cursor &cursor, const Status &status) -> void
{
    CALICO_EXPECT_FALSE(status.is_ok());
    reinterpret_cast<const CursorImpl &>(cursor).m_status = status;
}

auto CursorInternal::TEST_validate(const Cursor &cursor) -> void
{
    if (cursor.is_valid()) {
        const auto &impl = reinterpret_cast<const CursorImpl &>(cursor);
        auto node = impl.m_tree->acquire(Id {impl.m_loc.pid}).value();
        node.TEST_validate();
        impl.m_tree->release(std::move(node));
    }
}

} // namespace Calico
