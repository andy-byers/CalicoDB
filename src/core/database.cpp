
#include "calico/cursor.h"
#include "calico/statistics.h"
#include "calico/transaction.h"
#include "database_impl.h"
#include "pager/pager.h"
#include "tree/cursor_internal.h"
#include "tree/tree.h"
#include "utils/system.h"

namespace Calico {

namespace fs = std::filesystem;

Database::Database()
    : m_impl {std::make_unique<DatabaseImpl>()}
{}

auto Database::open(const Slice &path, const Options &options) -> Status
{
    return m_impl->open(path, options);
}

auto Database::close() -> Status
{
    return m_impl->close();
}

auto Database::destroy() && -> Status
{
    return m_impl->destroy();
}

Database::~Database()
{
    (void)close();
}

auto Database::cursor() const -> Cursor
{
    return m_impl->cursor();
}

auto Database::get(const Slice &key, std::string &value) const -> Status
{
    return m_impl->get(key, value);
}

auto Database::put(const Slice &key, const Slice &value) -> Status
{
    return m_impl->put(key, value);
}

auto Database::erase(const Slice &key) -> Status
{
    return m_impl->erase(key);
}

auto Database::statistics() const -> Statistics
{
    return m_impl->statistics();
}

auto Database::status() const -> Status
{
    return m_impl->status();
}

auto Database::start() -> Transaction
{
    return m_impl->start();
}

} // namespace Calico