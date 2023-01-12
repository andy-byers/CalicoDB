#include "cleanup.h"

namespace Calico {

WalCleanup::WalCleanup(const Parameters &param)
    : m_prefix {param.prefix.to_string()},
      m_limit {param.limit},
      m_storage {param.storage},
      m_system {param.system},
      m_set {param.set}
{
    CALICO_EXPECT_FALSE(m_prefix.empty());
    CALICO_EXPECT_NE(m_storage, nullptr);
    CALICO_EXPECT_NE(m_system, nullptr);
    CALICO_EXPECT_NE(m_set, nullptr);
}

auto WalCleanup::cleanup() -> void
{
    const auto limit = m_limit->load();

    const auto first = m_set->first();
    if (first.is_null())
        return;

    const auto second = m_set->id_after(first);
    if (second.is_null())
        return;

    auto r = read_first_lsn(*m_storage, m_prefix, second, *m_set);
    if (!r.has_value()) {
        CALICO_ERROR(r.error());
        return;
    }

    if (*r > limit)
        return;

    auto s = m_storage->remove_file(m_prefix + first.to_name());
    if (s.is_ok()) {
        m_set->remove_before(second);
    } else {
        CALICO_ERROR(s);
    }
}

} // namespace Calico