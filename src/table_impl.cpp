#include "table_impl.h"
#include "db_impl.h"

namespace calicodb
{

TableImpl::TableImpl(std::string name, DBImpl &db, TableState &state, DBState &db_state)
    : m_name {std::move(name)},
      m_db_state {&db_state},
      m_state {&state},
      m_db {&db}

{
}

TableImpl::~TableImpl()
{
    m_db->close_table(m_name, root_id());
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
    CDB_TRY(m_db_state->status);

    bool record_exists;
    if (auto s = m_state->tree->put(key, value, &record_exists); !s.is_ok()) {
        if (m_db_state->status.is_ok()) {
            m_db_state->status = s;
        }
        return s;
    }
    m_db_state->record_count += !record_exists;
    m_db_state->bytes_written += key.size() + value.size();
    ++m_db_state->batch_size;
    return Status::ok();
}

auto TableImpl::erase(const Slice &key) -> Status
{
    CDB_TRY(m_db_state->status);

    auto s = m_state->tree->erase(key);
    if (s.is_ok()) {
        ++m_db_state->batch_size;
        --m_db_state->record_count;
    } else if (!s.is_not_found()) {
        if (m_db_state->status.is_ok()) {
            m_db_state->status = s;
        }
    }
    return s;
}

auto TableImpl::root_id() const -> LogicalPageId
{
    return m_state->root_id;
}

} // namespace calicodb