
#include "calico/cursor.h"
#include "calico/info.h"
#include "core.h"
#include "pager/basic_pager.h"
#include "pager/pager.h"
#include "store/disk.h"
#include "tree/bplus_tree.h"
#include "tree/cursor_internal.h"
#include "utils/logging.h"
#include "wal/writer.h"

namespace calico {

namespace fs = std::filesystem;

auto sanitize_options(const Options &options) -> Options
{
    auto sanitized = options;
    if (sanitized.log_level > MAXIMUM_LOG_LEVEL)
        sanitized.log_level = DEFAULT_LOG_LEVEL;
    return sanitized;
}

Database::Database() noexcept = default;

auto Database::is_open() const -> bool
{
    return m_core != nullptr;
}

auto Database::open(const std::string &path, const Options &options) -> Status
{
    if (is_open()) {
        ThreePartMessage message;
        message.set_primary("cannot open database");
        message.set_detail("database is already open");
        return message.logic_error();
    }
    m_core = std::make_unique<Core>();
    auto s = m_core->open(path, sanitize_options(options));
    if (!s.is_ok()) m_core.reset();
    return s;
}

auto Database::close() -> Status
{
    auto s = m_core->close();
    m_core.reset();
    return s;
}

auto Database::destroy(Database db) -> Status
{
    auto s = db.m_core->destroy();
    db.m_core.reset();
    return s;
}

Database::~Database()
{
    if (is_open()) (void)close();
}

Database::Database(Database &&) noexcept = default;

auto Database::operator=(Database &&) noexcept -> Database & = default;

auto Database::find_exact(BytesView key) const -> Cursor
{
    return m_core->find_exact(key);
}

auto Database::find_exact(const std::string &key) const -> Cursor
{
    return find_exact(stob(key));
}

auto Database::find(BytesView key) const -> Cursor
{
    return m_core->find(key);
}

auto Database::find(const std::string &key) const -> Cursor
{
    return find(stob(key));
}

auto Database::find_minimum() const -> Cursor
{
    return m_core->find_minimum();
}

auto Database::find_maximum() const -> Cursor
{
    return m_core->find_maximum();
}

auto Database::insert(BytesView key, BytesView value) -> Status
{
    return m_core->insert(key, value);
}

auto Database::insert(const std::string &key, const std::string &value) -> Status
{
    return insert(stob(key), stob(value));
}

auto Database::insert(const Record &record) -> Status
{
    const auto &[key, value] = record;
    return insert(key, value);
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
    return m_core->erase(cursor);
}

auto Database::info() const -> Info
{
    CALICO_EXPECT_TRUE(is_open());
    return m_core->info();
}

auto Database::status() const -> Status
{
    CALICO_EXPECT_TRUE(is_open());
    return m_core->status();
}

auto Database::commit() -> Status
{
    return m_core->commit();
}

auto Database::abort() -> Status
{
    return m_core->abort();
}

} // namespace calico