#include "table_impl.h"
#include "db_impl.h"

namespace calicodb {

TableImpl::TableImpl(DBImpl &db, TableState &state, Status &status)
    : m_db {&db},
      m_state {&state},
      m_status {&status}
{
}

TableImpl::~TableImpl()
{
    m_db->close_table(m_state->tree->m_root);
}

auto TableImpl::new_cursor() const -> Cursor *
{
    auto *cursor = CursorInternal::make_cursor(*m_state->tree);
    if (!m_db->status().is_ok()) {
        CursorInternal::invalidate(*cursor, m_db->status());
    }
    return cursor;
}

auto TableImpl::get(const Slice &key, std::string *value) const -> Status
{
    CDB_TRY(m_db->status());
    return m_state->tree->get(key, value);
}

auto TableImpl::put(const Slice &key, const Slice &value) -> Status
{
    if (key.is_empty()) {
        return Status::invalid_argument("key is empty");
    }
    CDB_TRY(*m_status);

    if (auto s = m_state->tree->put(key, value); !s.is_ok()) {
        if (m_status->is_ok()) {
            *m_status = s;
        }
        return s;
    }
    m_batch_size++;
    return Status::ok();
}

auto TableImpl::erase(const Slice &key) -> Status
{
    CDB_TRY(*m_status);

    auto s = m_state->tree->erase(key);
    if (s.is_ok()) {
        m_batch_size++;
    } else if (!s.is_not_found()) {
        if (m_status->is_ok()) {
            *m_status = s;
        }
    }
    return s;
}

auto TableImpl::commit() -> Status
{
    CDB_TRY(*m_status);
    if (m_batch_size != 0) {
        if (auto s = m_db->commit_table(m_state->tree->m_root, *m_state); !s.is_ok()) {
            if (m_status->is_ok()) {
                *m_status = s;
            }
            return s;
        }
    }
    return Status::ok();
}

} // namespace calicodb