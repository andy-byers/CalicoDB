
#include "calico/statistics.h"
#include "database_impl.h"
#include "pager/pager.h"
#include "tree/bplus_tree.h"

namespace Calico {

Statistics::Statistics(DatabaseImpl &impl)
    : m_impl {&impl}
{}

auto Statistics::record_count() const -> Size
{
    return m_impl->record_count;
}

auto Statistics::page_count() const -> Size
{
    return m_impl->pager->page_count();
}

auto Statistics::page_size() const -> Size
{
    return m_impl->pager->page_size();
}

auto Statistics::maximum_key_size() const -> Size
{
    return m_impl->maximum_key_size;
}

auto Statistics::cache_hit_ratio() const -> double
{
    return m_impl->pager->hit_ratio();
}

auto Statistics::registered_updates() const -> Size
{
    return m_impl->wal->flushed_lsn().value - m_impl->pager->recovery_lsn().value;
}

auto Statistics::pager_throughput() const -> Size
{
    return m_impl->pager->bytes_written();
}

auto Statistics::data_throughput() const -> Size
{
    return m_impl->bytes_written;
}

auto Statistics::wal_throughput() const -> Size
{
    return m_impl->wal->bytes_written();
}

} // namespace Calico
