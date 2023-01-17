
#include "core.h"
#include "calico/statistics.h"
#include "pager/pager.h"
#include "tree/tree.h"
#include "utils/layout.h"

namespace Calico {

auto Statistics::record_count() const -> Size
{
    return m_core->tree->record_count();
}

auto Statistics::page_count() const -> Size
{
    return m_core->pager->page_count();
}

auto Statistics::page_size() const -> Size
{
    return m_core->pager->page_size();
}

auto Statistics::maximum_key_size() const -> Size
{
    return get_max_local(page_size());
}

auto Statistics::cache_hit_ratio() const -> double
{
    return m_core->pager->hit_ratio();
}

auto Statistics::data_throughput() const -> Size
{
    return m_core->pager->bytes_written();
}

auto Statistics::wal_throughput() const -> Size
{
    return m_core->wal->bytes_written();
}

} // namespace Calico
