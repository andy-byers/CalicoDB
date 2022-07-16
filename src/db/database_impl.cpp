
#include "database_impl.h"
#include "calico/cursor.h"
#include "calico/status.h"
#include "page/file_header.h"
#include "pool/buffer_pool.h"
#include "pool/memory_pool.h"
#include "storage/file.h"
#include "tree/tree.h"
#include "utils/layout.h"
#include "utils/logging.h"
#include <filesystem>

namespace cco {

namespace fs = std::filesystem;
using namespace page;
using namespace utils;

auto Info::cache_hit_ratio() const -> double
{
    return m_db->cache_hit_ratio();
}

auto Info::record_count() const -> Size
{
    return m_db->record_count();
}

auto Info::page_count() const -> Size
{
    return m_db->page_count();
}

auto Info::page_size() const -> Size
{
    return m_db->page_size();
}

auto Info::maximum_key_size() const -> Size
{
    return get_max_local(page_size());
}

auto Info::is_temp() const -> bool
{
    return m_db->is_temp();
}

auto initialize_log(spdlog::logger &logger, const std::string &base)
{
    logger.info("starting CalicoDB v{} at \"{}\"", VERSION_NAME, base);
    logger.info("tree is located at \"{}/{}\"", base, DATA_NAME);
    logger.info("log is located at \"{}/{}\"", base, LOG_NAME);
}

auto Database::Impl::open(Parameters param, std::unique_ptr<IDirectory> home) -> Result<std::unique_ptr<Impl>>
{
    const auto path = home->path();
    auto sink = utils::create_sink(path, param.options.log_level);
    auto logger = utils::create_logger(sink, "Database");
    initialize_log(*logger, path);

    CCO_TRY_CREATE(initial_state, setup(*home, param.options, *logger));
    logger->trace("constructing Database object");

    auto impl = std::make_unique<Impl>();
    auto [state, revised, is_new] = std::move(initial_state);

    CCO_TRY_STORE(impl->m_pool, BufferPool::open({
        *home,
        sink,
        state.flushed_lsn(),
        revised.frame_count,
        state.page_count(),
        state.page_size(),
        revised.permissions,
        revised.use_xact,
    }));

    CCO_TRY_STORE(impl->m_tree, Tree::open({
        impl->m_pool.get(),
        sink,
        state.free_start(),
        state.free_count(),
        state.record_count(),
        state.node_count(),
    }));

    impl->m_sink = std::move(sink);
    impl->m_logger = create_logger(impl->m_sink, "Database");
    impl->m_home = std::move(home);

    if (is_new) {
        CCO_TRY_CREATE(root, impl->m_tree->allocate_root());
        FileHeader header {root};
        header.update_magic_code();
        header.set_page_size(state.page_size());
        header.set_block_size(state.block_size());
        CCO_TRY(impl->m_pool->release(root.take()));
    } else if (revised.use_xact) {
        CCO_TRY(impl->m_pool->recover());
    }
    return impl;
}

auto Database::Impl::open(Parameters param) -> Result<std::unique_ptr<Impl>>
{
    auto impl = std::make_unique<Impl>();
    impl->m_sink = create_sink();
    impl->m_logger = create_logger(impl->m_sink, "Database");
    impl->m_pool = std::make_unique<MemoryPool>(param.options.page_size, false, impl->m_sink);
    impl->m_tree = Tree::open({impl->m_pool.get(), impl->m_sink, PID::null(), 0, 0, 0}).value();
    impl->m_is_temp = true;
    return impl;
}

auto Database::Impl::status() const -> Status
{
    return m_pool->status();
}

auto Database::Impl::path() const -> std::string
{
    return m_home->path();
}

auto Database::Impl::info() -> Info
{
    Info info;
    info.m_db = this;
    return info;
}

auto Database::Impl::find_exact(BytesView key) -> Cursor
{
    return m_tree->find_exact(key);
}

auto Database::Impl::find(BytesView key) -> Cursor
{
    return m_tree->find(key);
}

auto Database::Impl::find_minimum() -> Cursor
{
    return m_tree->find_minimum();
}

auto Database::Impl::find_maximum() -> Cursor
{
    return m_tree->find_maximum();
}

auto Database::Impl::insert(BytesView key, BytesView value) -> Result<bool>
{
    return m_tree->insert(key, value);
}

auto Database::Impl::erase(BytesView key) -> Result<bool>
{
    return erase(m_tree->find_exact(key));
}

auto Database::Impl::erase(Cursor cursor) -> Result<bool>
{
    return m_tree->erase(cursor);
}

auto Database::Impl::commit() -> Result<void>
{
    return save_header()
        .and_then([this]() -> Result<void> {
            CCO_TRY(m_pool->commit());
            m_logger->trace("commit succeeded");
            return {};
        })
        .or_else([this](const Status &status) -> Result<void> {
            LogMessage message {*m_logger};
            message.set_primary("cannot commit");
            message.log(spdlog::level::err);
            return Err {status};
        });
}

auto Database::Impl::abort() -> Result<void>
{
    return m_pool->abort()
        .and_then([this]() -> Result<void> {
            CCO_TRY(m_pool->commit());
            m_logger->trace("abort succeeded");
            return {};
        })
        .or_else([this](const Status &status) -> Result<void> {
            LogMessage message {*m_logger};
            message.set_primary("cannot abort");
            message.log(spdlog::level::err);
            return Err {status};
        });
}

auto Database::Impl::close() -> Result<void>
{
    CCO_TRY(commit());
    CCO_TRY(m_tree->close());
    CCO_TRY(m_pool->close());
    return m_home->close();
}

auto Database::Impl::save_header() -> Result<void>
{
    CCO_TRY_CREATE(root, m_tree->root(true));
    FileHeader header {root};
    m_pool->save_header(header);
    m_tree->save_header(header);
    header.update_header_crc();
    return {};
}

auto Database::Impl::load_header() -> Result<void>
{
    CCO_TRY_CREATE(root, m_tree->root(true));
    FileHeader header {root};
    m_pool->load_header(header);
    m_tree->load_header(header);
    return {};
}

auto Database::Impl::cache_hit_ratio() const -> double
{
    return m_pool->hit_ratio();
}

auto Database::Impl::record_count() const -> Size
{
    return m_tree->cell_count();
}

auto Database::Impl::page_count() const -> Size
{
    return m_pool->page_count();
}

auto Database::Impl::page_size() const -> Size
{
    return m_pool->page_size();
}

auto Database::Impl::is_temp() const -> bool
{
    return m_is_temp;
}

auto setup(IDirectory &directory, const Options &options, spdlog::logger &logger) -> Result<InitialState>
{
    static constexpr auto ERROR_PRIMARY = "cannot open database";
    LogMessage message {logger};
    message.set_primary(ERROR_PRIMARY);

    const auto perm = options.permissions;
    auto revised = options;
    FileHeader header;
    bool is_new {};

    CCO_TRY_CREATE(exists, directory.exists(DATA_NAME));

    if (exists) {
        CCO_TRY_CREATE(file, directory.open_file(DATA_NAME, Mode::READ_ONLY, perm));
        CCO_TRY_CREATE(file_size, file->size());

        if (file_size < FileLayout::HEADER_SIZE) {
            message.set_detail("database is too small to read the file header");
            message.set_hint("file header is {} B", FileLayout::HEADER_SIZE);
            return Err {message.corruption()};
        }
        if (!read_exact(*file, header.data())) {
            message.set_detail("cannot read file header");
            return Err {message.corruption()};
        }
        // NOTE: This check is omitted if the page size is 0 to avoid getting a floating-point exception. If this is
        //       the case, we'll find out below when we make sure the page size is valid.
        if (header.page_size() && file_size % header.page_size()) {
            message.set_detail("database has an invalid size");
            message.set_hint("database must contain an integral number of pages");
            return Err {message.corruption()};
        }
        if (!header.is_magic_code_consistent()) {
            message.set_detail("path does not point to a Calico DB database");
            message.set_hint("magic code is {}, but should be {}", header.magic_code(), FileHeader::MAGIC_CODE);
            return Err {message.invalid_argument()};
        }
        if (!header.is_header_crc_consistent()) {
            message.set_detail("header has an inconsistent CRC");
            message.set_hint("CRC is {}", header.header_crc());
            return Err {message.corruption()};
        }
        revised.use_xact = !header.flushed_lsn().is_null();

    } else {
        header.update_magic_code();
        header.set_page_size(options.page_size);
        header.set_flushed_lsn(LSN::base());
        header.update_header_crc();
        is_new = true;

        // Try to create the file. If it doesn't work this time, we've got a problem.
        std::unique_ptr<IFile> file;
        const auto mode = Mode::READ_WRITE | Mode::CREATE;
        CCO_TRY_STORE(file, directory.open_file(DATA_NAME, mode, perm));
    }

    const auto choose_error = [is_new, &message] {
        return Err {is_new ? message.invalid_argument() : message.corruption()};
    };

    if (header.page_size() < MINIMUM_PAGE_SIZE) {
        message.set_detail("page size {} is too small", header.page_size());
        message.set_hint("must be greater than or equal to {}", MINIMUM_PAGE_SIZE);
        return choose_error();
    }
    if (header.page_size() > MAXIMUM_PAGE_SIZE) {
        message.set_detail("page size {} is too large", header.page_size());
        message.set_hint("must be less than or equal to {}", MAXIMUM_PAGE_SIZE);
        return choose_error();
    }
    if (!is_power_of_two(header.page_size())) {
        message.set_detail("page size {} is invalid", header.page_size());
        message.set_hint("must be a power of 2");
        return choose_error();
    }
    revised.page_size = header.page_size();
    return InitialState {std::move(header), revised, is_new};
}

} // calico

auto operator<(const cco::Record &lhs, const cco::Record &rhs) -> bool
{
    return cco::stob(lhs.key) < cco::stob(rhs.key);
}

auto operator>(const cco::Record &lhs, const cco::Record &rhs) -> bool
{
    return cco::stob(lhs.key) > cco::stob(rhs.key);
}

auto operator<=(const cco::Record &lhs, const cco::Record &rhs) -> bool
{
    return cco::stob(lhs.key) <= cco::stob(rhs.key);
}

auto operator>=(const cco::Record &lhs, const cco::Record &rhs) -> bool
{
    return cco::stob(lhs.key) >= cco::stob(rhs.key);
}

auto operator==(const cco::Record &lhs, const cco::Record &rhs) -> bool
{
    return cco::stob(lhs.key) == cco::stob(rhs.key);
}

auto operator!=(const cco::Record &lhs, const cco::Record &rhs) -> bool
{
    return cco::stob(lhs.key) != cco::stob(rhs.key);
}
