
#include "database_impl.h"
#include <filesystem>
#include "calico/cursor.h"
#include "calico/error.h"
#include "page/file_header.h"
#include "pool/buffer_pool.h"
#include "pool/memory_pool.h"
#include "storage/file.h"
#include "storage/io.h"
#include "tree/tree.h"
#include "utils/layout.h"
#include "utils/logging.h"

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

Database::Impl::~Impl()
{
    if (const auto committed = commit(); !committed.has_value()) {

    }
}

Database::Impl::Impl(Parameters param):
      m_sink {std::move(param.sink)},
      m_logger {create_logger(m_sink, "Database")},
      m_home {std::move(param.home)} {}

Database::Impl::Impl(Parameters param, InMemoryTag)
    : m_sink {create_sink("", spdlog::level::off)},
      m_logger {create_logger(m_sink, "Database")},
      m_pool {std::make_unique<MemoryPool>(param.options.page_size, false, m_sink)},
      m_tree {*Tree::open(Tree::Parameters {m_pool.get(), m_sink, PID::null(), 0, 0, 0})},
      m_is_temp {true} {}

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
            CCO_TRY(m_pool->flush());
            m_logger->trace("commit succeeded");
            return {};
        })
        .or_else([this](const Error &error) -> Result<void> {
            LogMessage message {*m_logger};
            message.set_primary("cannot commit");
            message.log(spdlog::level::err);
            return Err {error};
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
        auto reader = file->open_reader();
        CCO_TRY_CREATE(file_size, file->size());

        if (file_size < FileLayout::HEADER_SIZE) {
            message.set_detail("database is too small to read the file header");
            message.set_hint("file header is {} B", FileLayout::HEADER_SIZE);
            return Err {message.corruption()};
        }
        if (!read_exact(*reader, header.data())) {
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
