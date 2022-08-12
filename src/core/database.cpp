
#include "calico/cursor.h"
#include "calico/info.h"
#include "core.h"
#include "pager/basic_pager.h"
#include "pager/pager.h"
#include "storage/disk.h"
#include "tree/bplus_tree.h"
#include "utils/logging.h"
#include "wal/wal_writer.h"

namespace cco {

namespace fs = std::filesystem;

auto sanitize_options(const Options &options) -> Options
{
    auto sanitized = options;
    if (sanitized.log_level >= spdlog::level::n_levels)
        sanitized.log_level = spdlog::level::off;
    return sanitized;
//    ThreePartMessage message;
//    message.set_primary("cannot sanitize options");
//
//    if (options.page_size < MINIMUM_PAGE_SIZE) {
//        message.set_detail("page size {} is too small", options.page_size);
//        message.set_hint("must be greater than or equal to {}", MINIMUM_PAGE_SIZE);
//        return message.invalid_argument();
//    }
//    if (options.page_size > MAXIMUM_PAGE_SIZE) {
//        message.set_detail("page size {} is too large", options.page_size);
//        message.set_hint("must be less than or equal to {}", MAXIMUM_PAGE_SIZE);
//        return message.invalid_argument();
//    }
//    if (!is_power_of_two(options.page_size)) {
//        message.set_detail("page size {} is invalid", options.page_size);
//        message.set_hint("must be a power of 2");
//        return message.invalid_argument();
//    }
//    return options;
}

[[nodiscard]]
static auto not_open_error(const std::string &primary)
{
    ThreePartMessage message;
    message.set_primary("cannot file_close database");
    message.set_detail("database is not open");
    message.set_hint("open a database and try again");
    return message.logic_error();
}

//#define DB_TRY(expr)                       \newline_goes_here
//    do {                                   \newline_goes_here
//        const auto db_try_result = (expr); \newline_goes_here
//        if (!db_try_result.has_value())    \newline_goes_here
//            return db_try_result.error();  \newline_goes_here
//    } while (0)
//
Database::Database() noexcept = default;

auto Database::is_open() const -> bool
{
    return m_core != nullptr;
}

auto Database::open(const std::string &path, const Options &options) -> Status
{
    m_core = std::make_unique<Core>(path, options);
    return m_core->open();
}

auto Database::close() -> Status
{
    if (!is_open()) return not_open_error("cannot file_close database");

    auto r = m_core->close();
    m_core.reset();
    return r ? Status::ok() : r.error();
}

auto Database::destroy(Database db) -> Status
{
    if (!db.is_open()) return not_open_error("cannot destroy database");
    auto &store = *db.m_core->m_store;
    auto s = Status::ok();

    std::vector<std::string> children;
    s = store.get_file_names(children);
    if (!s.is_ok()) return s;

    for (const auto &name: children)
        s = store.remove_file(name);
    return store.remove_directory(db.m_core->m_path);
}
//
//Database::Database() = default;
//
//Database::~Database() = default;
//
//Database::Database(Database &&) noexcept = default;
//
//auto Database::operator=(Database &&) noexcept -> Database & = default;
//
//auto Database::find_exact(BytesView key) const -> Cursor
//{
//    return m_impl->find_exact(key);
//}
//
//auto Database::find_exact(const std::string &key) const -> Cursor
//{
//    return find_exact(stob(key));
//}
//
//auto Database::find(BytesView key) const -> Cursor
//{
//    return m_impl->find(key);
//}
//
//auto Database::find(const std::string &key) const -> Cursor
//{
//    return m_impl->find(stob(key));
//}
//
//auto Database::find_minimum() const -> Cursor
//{
//    return m_impl->find_minimum();
//}
//
//auto Database::find_maximum() const -> Cursor
//{
//    return m_impl->find_maximum();
//}
//
//auto Database::insert(BytesView key, BytesView value) -> Status
//{
//    DB_TRY(m_impl->insert(key, value));
//    return Status::ok();
//}
//
//auto Database::insert(const std::string &key, const std::string &value) -> Status
//{
//    return insert(stob(key), stob(value));
//}
//
//auto Database::insert(const Record &record) -> Status
//{
//    const auto &[key, value] = record;
//    return insert(key, value);
//}
//
//auto Database::erase(BytesView key) -> Status
//{
//    return erase(find_exact(key));
//}
//
//auto Database::erase(const std::string &key) -> Status
//{
//    return erase(stob(key));
//}
//
//auto Database::erase(const Cursor &cursor) -> Status
//{
//    auto r = m_impl->erase(cursor);
//    if (!r.has_value()) {
//        return Status {r.error()};
//    } else if (!r.value()) {
//        return Status::not_found();
//    }
//    return Status::ok();
//}
//
//auto Database::info() const -> Info
//{
//    return m_impl->info();
//}
//
//auto Database::status() const -> Status
//{
//    return m_impl->status();
//}
//
//auto Database::commit() -> Status
//{
//    DB_TRY(m_impl->commit());
//    return Status::ok();
//}
//
//auto Database::abort() -> Status
//{
//    DB_TRY(m_impl->abort());
//    return Status::ok();}
//
//#undef DB_TRY
//
} // namespace cco