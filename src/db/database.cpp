
#include "database_impl.h"
#include "calico/cursor.h"
#include "file/file.h"
#include "file/system.h"
#include "page/file_header.h"
#include "pool/in_memory.h"
#include "utils/logging.h"

namespace calico {

auto Database::open(const std::string &path, Options options) -> Database
{
    auto state = get_initial_state(path, options);
    options.use_transactions = state.uses_transactions;
    auto files = get_open_files(path, options);

#if not CALICO_HAS_O_DIRECT
    if (options.use_direct_io) {
        files.tree_file->use_direct_io();
        if (options.use_transactions) {
            files.wal_reader_file->use_direct_io();
            files.wal_writer_file->use_direct_io();
        }
    }
#endif // not CALICO_HAS_O_DIRECT

    Database db;
    db.m_impl = std::make_unique<Impl>(Impl::Parameters {
        path,
        std::move(files.tree_file),
        std::move(files.wal_reader_file),
        std::move(files.wal_writer_file),
        std::move(state.header),
        std::move(options),
    });
    return db;
}

auto Database::temp(Options options) -> Database
{
    Impl::Parameters param;
    param.options = std::move(options);

    Database db;
    db.m_impl = std::make_unique<Impl>(std::move(param), Impl::InMemoryTag {});
    return db;
}

auto Database::destroy(Database db) -> void
{
    if (const auto &path = db.m_impl->path(); !path.empty())
        system::unlink(path);
}

Database::Database() = default;

Database::~Database() = default;

Database::Database(Database&&) noexcept = default;

auto Database::operator=(Database&&) noexcept -> Database& = default;

auto Database::find(BytesView key, bool allow_greater) const -> Cursor
{
    return m_impl->find(key, !allow_greater);
}

auto Database::find_minimum() const -> Cursor
{
    return m_impl->find_minimum();
}

auto Database::find_maximum() const -> Cursor
{
    return m_impl->find_maximum();
}

auto Database::insert(BytesView key, BytesView value) -> bool
{
    return m_impl->insert(key, value);
}

auto Database::insert(const Record &record) -> bool
{
    const auto &[key, value] = record;
    return m_impl->insert(stob(key), stob(value));
}

auto Database::erase(BytesView key) -> bool
{
    return m_impl->erase(key);
}

auto Database::erase(Cursor cursor) -> bool
{
    return m_impl->erase(cursor);
}

auto Database::commit() -> bool
{
    return m_impl->commit();
}

auto Database::abort() -> bool
{
    return m_impl->abort();
}

auto Database::info() const -> Info
{
    return m_impl->info();
}

} // calico