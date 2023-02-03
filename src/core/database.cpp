
#include "calico/cursor.h"
#include "calico/statistics.h"
#include "calico/transaction.h"
#include "database_impl.h"
#include "pager/pager.h"
#include "tree/cursor_internal.h"
#include "tree/tree.h"
#include "wal/cleanup.h"
#include "wal/writer.h"

namespace Calico {

namespace fs = std::filesystem;

Database::Database()
    : m_impl {std::make_unique<DatabaseImpl>()}
{}

auto Database::open(const Slice &path, const Options &options, Database **db) -> Status
{
    auto *ptr = new(std::nothrow) Database;
    if (ptr == nullptr) {
        return system_error("cannot allocate database object: out of memory");
    }
    if (auto s = ptr->m_impl->open(path, options); !s.is_ok()) {
        delete ptr;
        return s;
    }
    *db = ptr;
    return ok();
}

auto Database::destroy(const Slice &path, const Options &options) -> Status
{
    return DatabaseImpl::destroy(path.to_string(), options);
}

Database::~Database()
{
    (void)m_impl->close();
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