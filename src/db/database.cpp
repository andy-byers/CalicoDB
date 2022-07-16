
#include "database_impl.h"
#include "calico/cursor.h"
#include "pool/buffer_pool.h"
#include "pool/interface.h"
#include "storage/directory.h"
#include "tree/tree.h"
#include "utils/logging.h"
#include "wal/wal_reader.h"
#include "wal/wal_writer.h"

namespace cco {

namespace fs = std::filesystem;
using namespace page;
using namespace utils;

#define DB_TRY(expr) \
    do { \
        const auto db_try_result = (expr); \
        if (!db_try_result.has_value()) \
            return db_try_result.error(); \
    } while (0)

Database::Database(const Options &options):
    m_options {options} {}

auto Database::is_open() const -> bool
{
    return m_impl != nullptr;
}

auto Database::open() -> Status
{
    Impl::Parameters param;
    param.options = m_options;

    if (!m_options.path.empty()) {
        auto home = Directory::open(m_options.path);
        if (!home.has_value())
            return home.error();

        param.sink = create_sink(m_options.path, m_options.log_level);
        auto impl = Impl::open(std::move(param), std::move(*home));
        if (!impl.has_value())
            return impl.error();
        m_impl = std::move(*impl);
    } else {
        auto impl = Impl::open(std::move(param));
        if (!impl.has_value())
            return impl.error();
        m_impl = std::move(*impl);
    }
    return Status::ok();
}

auto Database::close() -> Status
{
    const auto r = m_impl->close();
    m_impl.reset();
    if (!r.has_value())
        return r.error();
    return Status::ok();
}

auto Database::destroy(Database db) -> Status
{
    if (db.m_impl->is_temp())
        return Status::ok();

    if (const auto &path = db.m_impl->path(); !path.empty()) {
        if (std::error_code error; !fs::remove_all(path, error))
            return Status::system_error(error.message());
    }
    return Status::ok();
}

Database::Database() = default;

Database::~Database() = default;

Database::Database(Database&&) noexcept = default;

auto Database::operator=(Database&&) noexcept -> Database& = default;

auto Database::find_exact(BytesView key) const -> Cursor
{
    return m_impl->find_exact(key);
}

auto Database::find_exact(const std::string &key) const -> Cursor
{
    return find_exact(stob(key));
}

auto Database::find(BytesView key) const -> Cursor
{
    return m_impl->find(key);
}

auto Database::find(const std::string &key) const -> Cursor
{
    return m_impl->find(stob(key));
}

auto Database::find_minimum() const -> Cursor
{
    return m_impl->find_minimum();
}

auto Database::find_maximum() const -> Cursor
{
    return m_impl->find_maximum();
}

auto Database::insert(BytesView key, BytesView value) -> Status
{
    DB_TRY(m_impl->insert(key, value));
    return Status::ok();
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
    auto r = m_impl->erase(cursor);
    if (!r.has_value()) {
        return Status {r.error()};
    } else if (!r.value()) {
        return Status::not_found();
    }
    return Status::ok();
}

auto Database::commit() -> Status
{
    DB_TRY(m_impl->commit());
    return Status::ok();
}

auto Database::abort() -> Status
{
    DB_TRY(m_impl->abort());
    return Status::ok();
}

auto Database::info() const -> Info
{
    return m_impl->info();
}

#undef DB_TRY

} // cco