
#include "calico/cursor.h"
#include "calico/info.h"
#include "database_impl.h"
#include "pool/buffer_pool.h"
#include "pool/interface.h"
#include "storage/disk.h"
#include "tree/bplus_tree.h"
#include "utils/logging.h"
#include "wal/wal_writer.h"

namespace cco {

namespace fs = std::filesystem;

#define DB_TRY(expr)                       \
    do {                                   \
        const auto db_try_result = (expr); \
        if (!db_try_result.has_value())    \
            return db_try_result.error();  \
    } while (0)

Database::Database(Options options) noexcept
    : m_options {std::move(options)}
{}

auto Database::is_open() const -> bool
{
    return m_impl != nullptr;
}

auto Database::open() -> Status
{
    Impl::Parameters param;
    param.options = m_options;

    if (!m_options.path.empty()) {
        auto home = DiskStorage::open(m_options.path);
        if (!home.has_value())
            return home.error();

        param.sink = create_sink(m_options.path, m_options.log_level);
        auto temp = create_logger(param.sink, "open");
        auto impl = Impl::open(std::move(param), std::move(*home));
        if (!impl.has_value()) {
            temp->error("cannot open database");
            temp->error("(reason) {}", impl.error().what());
            return impl.error();
        }
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
    auto r = m_impl->close();
    m_impl.reset();
    return r ? Status::ok() : r.error();
}

auto Database::destroy(Database db) -> Status
{
    auto s = Status::ok();
    if (db.is_open())
        s = db.close();

    // remove() and remove_all() return this on failure. See https://en.cppreference.com/w/cpp/filesystem/remove.
    static constexpr auto error_value = static_cast<std::uintmax_t>(-1);
    if (const auto &path = db.m_options.path; !path.empty()) {
        if (std::error_code error; fs::remove_all(path, error) == error_value)
            return Status::system_error(error.message());
    }

    return s;
}

Database::Database() = default;

Database::~Database() = default;

Database::Database(Database &&) noexcept = default;

auto Database::operator=(Database &&) noexcept -> Database & = default;

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

auto Database::info() const -> Info
{
    return m_impl->info();
}

auto Database::status() const -> Status
{
    return m_impl->status();
}

auto Database::commit() -> Status
{
    DB_TRY(m_impl->commit());
    return Status::ok();
}

auto Database::abort() -> Status
{
    DB_TRY(m_impl->abort());
    return Status::ok();}

#undef DB_TRY

} // namespace cco