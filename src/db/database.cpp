
#include "database_impl.h"
#include "calico/cursor.h"
#include "pool/buffer_pool.h"
#include "pool/interface.h"
#include "storage/directory.h"
#include "tree/tree.h"
#include "utils/logging.h"

namespace cco {

namespace fs = std::filesystem;
using namespace page;
using namespace utils;

auto initialize_log(spdlog::logger &logger, const std::string &base)
{
    logger.info("starting CalicoDB v{} at \"{}\"", VERSION_NAME, base);
    logger.info("tree is located at \"{}/{}\"", base, DATA_NAME);
    logger.info("log is located at \"{}/{}\"", base, LOG_NAME);
}

auto Database::open(const std::string &path, Options options) -> Result<Database>
{
    CCO_TRY_CREATE(directory, Directory::open(path));

    auto sink = utils::create_sink(path, options.log_level);
    auto logger = utils::create_logger(sink, "Database");
    initialize_log(*logger, path);

    CCO_TRY_CREATE(initial_state, setup(*directory, options, *logger));
    logger->trace("constructing Database object");

    Database db;
    db.m_impl = std::make_unique<Impl>(Impl::Parameters {
        std::move(directory),
        sink,
        initial_state.state,
        initial_state.revised,
    });
    auto [state, revised, is_new] = std::move(initial_state);
    auto &impl = *db.m_impl;

    CCO_TRY_STORE(impl.m_pool, BufferPool::open(BufferPool::Parameters {
        *impl.m_home,
        sink,
        revised.frame_count,
        state.page_count(),
        state.page_size(),
        revised.permissions,
    }));

    CCO_TRY_STORE(impl.m_tree, Tree::open(Tree::Parameters {
        impl.m_pool.get(),
        sink,
        state.free_start(),
        state.free_count(),
        state.record_count(),
        state.node_count(),
    }));

    if (is_new) {
        CCO_TRY_CREATE(root, impl.m_tree->root(true));
        FileHeader header {root};
        header.update_magic_code();
        header.set_page_size(state.page_size());
        header.set_block_size(state.block_size());
        CCO_TRY(impl.m_pool->release(root.take()));
    }
    return db;
}

auto Database::close(Database db) -> Result<void>
{
    return db.m_impl->close();
}

auto Database::temp(Options options) -> Result<Database>
{
    Impl::Parameters param;
    param.options = options;
    Database db;
    db.m_impl = std::make_unique<Impl>(std::move(param), Impl::InMemoryTag {});
    return db;
}

auto Database::destroy(Database db) -> Result<void>
{
    if (db.m_impl->is_temp())
        return {};

    if (const auto &path = db.m_impl->path(); !path.empty()) {
        if (std::error_code error; !fs::remove_all(path, error))
            return Err {Error::system_error(error.message())};
    }
    return {};
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

auto Database::insert(BytesView key, BytesView value) -> Result<bool>
{
    return m_impl->insert(key, value);
}

auto Database::insert(const std::string &key, const std::string &value) -> Result<bool>
{
    return insert(stob(key), stob(value));
}

auto Database::insert(const Record &record) -> Result<bool>
{
    const auto &[key, value] = record;
    return insert(key, value);
}

auto Database::erase(BytesView key) -> Result<bool>
{
    return m_impl->erase(key);
}

auto Database::erase(const std::string &key) -> Result<bool>
{
    return erase(stob(key));
}

auto Database::erase(const Cursor &cursor) -> Result<bool>
{
    return m_impl->erase(cursor);
}

auto Database::commit() -> Result<void>
{
    return m_impl->commit();
}

auto Database::info() const -> Info
{
    return m_impl->info();
}

} // calico