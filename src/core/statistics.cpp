
#include "core.h"
#include "calico/calico.h"
#include "calico/storage.h"
#include "calico/transaction.h"
#include "pager/basic_pager.h"
#include "pager/pager.h"
#include "recovery.h"
#include "storage/helpers.h"
#include "storage/posix_storage.h"
#include "tree/bplus_tree.h"
#include "tree/cursor_internal.h"
#include "utils/crc.h"
#include "utils/header.h"
#include "utils/layout.h"
#include "utils/system.h"
#include "wal/basic_wal.h"

namespace Calico {

auto Statistics::record_count() const -> Size
{
    return m_core->tree().record_count();
}

auto Statistics::page_count() const -> Size
{
    return m_core->pager().page_count();
}

auto Statistics::page_size() const -> Size
{
    return m_core->pager().page_size();
}

auto Statistics::maximum_key_size() const -> Size
{
    return get_max_local(page_size());
}

auto Statistics::cache_hit_ratio() const -> double
{
    return m_core->pager().hit_ratio();
}

} // namespace Calico
