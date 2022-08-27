
#include "calico/cursor.h"
#include "calico/info.h"
#include "calico/transaction.h"
#include "core.h"
#include "pager/basic_pager.h"
#include "pager/pager.h"
#include "store/disk.h"
#include "tree/bplus_tree.h"
#include "tree/cursor_internal.h"
#include "utils/logging.h"

namespace calico {

namespace fs = std::filesystem;

Database::Database() noexcept = default;

auto Database::open(const std::string &path, const Options &options) -> Status
{
    CALICO_EXPECT_EQ(m_core, nullptr);
    m_core = std::make_unique<Core>();
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

auto Database::destroy(Database db) -> Status
{
    CALICO_EXPECT_NE(db.m_core, nullptr);
    auto s = db.m_core->destroy();
    db.m_core.reset();
    return s;
}

Database::~Database()
{
    if (m_core) (void)close();
}

Database::Database(Database &&) noexcept = default;

auto Database::operator=(Database &&) noexcept -> Database & = default;

auto Database::find_exact(BytesView key) const -> Cursor
{
    CALICO_EXPECT_NE(m_core, nullptr);
    return m_core->find_exact(key);
}

auto Database::find_exact(const std::string &key) const -> Cursor
{
    return find_exact(stob(key));
}

auto Database::find(BytesView key) const -> Cursor
{
    CALICO_EXPECT_NE(m_core, nullptr);
    return m_core->find(key);
}

auto Database::find(const std::string &key) const -> Cursor
{
    return find(stob(key));
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

auto Database::insert(BytesView key, BytesView value) -> Status
{
    CALICO_EXPECT_NE(m_core, nullptr);
    return m_core->insert(key, value);
}

auto Database::insert(const std::string &key, const std::string &value) -> Status
{
    return insert(stob(key), stob(value));
}

auto Database::erase(BytesView key) -> Status
{
    return erase(find_exact(key));
}

auto Database::erase(const std::string &key) -> Status
{
    return erase(stob(key));
}

auto Database::erase(const Cursor &cursor) -> Status
{
    CALICO_EXPECT_NE(m_core, nullptr);
    return m_core->erase(cursor);
}

auto Database::info() const -> Info
{
    CALICO_EXPECT_NE(m_core, nullptr);
    return m_core->info();
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

} // namespace calico