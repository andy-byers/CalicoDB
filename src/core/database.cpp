
#include "calico/cursor.h"
#include "calico/statistics.h"
#include "calico/transaction.h"
#include "core.h"
#include "pager/basic_pager.h"
#include "pager/pager.h"
#include "storage/posix_storage.h"
#include "tree/bplus_tree.h"
#include "tree/cursor_internal.h"
#include "utils/system.h"

namespace Calico {

namespace fs = std::filesystem;

Database::Database() noexcept = default;

auto Database::open(const Slice &path, const Options &options) -> Status
{
    CALICO_EXPECT_EQ(m_core, nullptr);
    m_core = std::unique_ptr<Core> {new(std::nothrow) Core};
    if (m_core == nullptr)
        return system_error("cannot open database: out of memory");

    auto s = m_core->open(path, options);
    if (!s.is_ok()) m_core.reset();
    return s;
}

auto Database::close() -> Status
{
    CALICO_EXPECT_NE(m_core, nullptr);
    auto s = m_core->close();
    m_core.reset();
    return s;
}

auto Database::destroy() && -> Status
{
    CALICO_EXPECT_NE(m_core, nullptr);
    auto s = m_core->destroy();
    m_core.reset();
    return s;
}

Database::~Database()
{
    if (m_core) (void)close();
}

Database::Database(Database &&) noexcept = default;

auto Database::operator=(Database &&) noexcept -> Database & = default;

auto Database::find_exact(const Slice &key) const -> Cursor
{
    CALICO_EXPECT_NE(m_core, nullptr);
    return m_core->find_exact(key);
}

auto Database::find(const Slice &key) const -> Cursor
{
    CALICO_EXPECT_NE(m_core, nullptr);
    return m_core->find(key);
}

auto Database::first() const -> Cursor
{
    CALICO_EXPECT_NE(m_core, nullptr);
    return m_core->first();
}

auto Database::last() const -> Cursor
{
    CALICO_EXPECT_NE(m_core, nullptr);
    return m_core->last();
}

auto Database::insert(const Slice &key, const Slice &value) -> Status
{
    CALICO_EXPECT_NE(m_core, nullptr);
    return m_core->insert(key, value);
}

auto Database::erase(const Slice &key) -> Status
{
    CALICO_EXPECT_NE(m_core, nullptr);
    return m_core->erase(key);
}

auto Database::statistics() const -> Statistics
{
    CALICO_EXPECT_NE(m_core, nullptr);
    return m_core->statistics();
}

auto Database::status() const -> Status
{
    CALICO_EXPECT_NE(m_core, nullptr);
    return m_core->status();
}

auto Database::transaction() -> Transaction
{
    CALICO_EXPECT_NE(m_core, nullptr);
    return m_core->transaction();
}

} // namespace Calico