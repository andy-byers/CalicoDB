
#include "database_impl.h"
#include <filesystem>
#include "calico/cursor.h"
#include "calico/error.h"
#include "page/file_header.h"
#include "pool/buffer_pool.h"
#include "pool/memory_pool.h"
#include "storage/directory.h"
#include "storage/file.h"
#include "storage/io.h"
#include "tree/tree.h"
#include "utils/layout.h"
#include "utils/logging.h"

namespace calico {

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
        utils::NumberedGroup group;
        group.push_line("database failed to commit in destructor");
        group.push_line("{}", btos(committed.error().what()));
        group.log(*m_logger, spdlog::level::err);
    }
}

Database::Impl::Impl(Parameters param):
      m_sink {std::move(param.sink)},
      m_logger {utils::create_logger(m_sink, "Database")},
      m_directory {std::move(param.directory)} {}

Database::Impl::Impl(Parameters param, InMemoryTag)
    : m_sink {utils::create_sink("", spdlog::level::off)},
      m_logger {utils::create_logger(m_sink, "Database")},
      m_pool {std::make_unique<MemoryPool>(param.options.page_size, false, m_sink)},
      m_tree {*Tree::open(Tree::Parameters {m_pool.get(), m_sink, PID::null(), 0, 0, 0})},
      m_is_temp {true} {}

auto Database::Impl::path() const -> std::string
{
    return m_directory->path();
}

auto Database::Impl::info() -> Info
{
    Info info;
    info.m_db = this;
    return info;
}

inline auto check_key(BytesView key, spdlog::logger &logger)
{
    if (key.is_empty()) {
        utils::ErrorMessage group;
        group.set_primary("cannot read record");
        group.set_detail("key cannot be empty");
        throw std::invalid_argument {group.error(logger)};
    }
}

auto Database::Impl::find_exact(BytesView key) -> Cursor
{
    check_key(key, *m_logger);
    return m_tree->find_exact(key);
}

auto Database::Impl::find(BytesView key) -> Cursor
{
    check_key(key, *m_logger);
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
    CCO_TRY(save_header());
    return m_pool->flush();
}

auto Database::Impl::save_header() -> Result<void>
{
    CCO_TRY_CREATE(Node, root, m_tree->root(true));
    FileHeader header {root};
    m_pool->save_header(header);
    m_tree->save_header(header);
    header.update_header_crc();
    return {};
}

auto Database::Impl::load_header() -> Result<void>
{
    CCO_TRY_CREATE(Node, root, m_tree->root(true));
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
    utils::ErrorMessage group;
    group.set_primary(ERROR_PRIMARY);

    const auto perm = options.permissions;
    auto revised = options;
    FileHeader header;
    bool is_new {};

    if (auto temp = directory.open_file(DATA_NAME, Mode::READ_ONLY, perm)) {
        auto file = std::move(*temp);
        auto reader = file->open_reader();
        CCO_TRY_CREATE(Size, file_size, file->size());

        if (file_size < FileLayout::HEADER_SIZE) {
            group.set_detail("database is too small to read the file header");
            group.set_hint("file header is {} B", FileLayout::HEADER_SIZE);
            return ErrorResult {group.corruption(logger)};
        }
        if (!read_exact(*reader, header.data())) {
            group.set_detail("cannot read file header");
            return ErrorResult {group.corruption(logger)};
        }
        // TODO: Check page size before modulus to avoid potential FPE.
        if (file_size % header.page_size()) {
            group.set_detail("database has an invalid size");
            group.set_hint("database must contain an integral number of pages");
            return ErrorResult {group.corruption(logger)};
        }
        if (!header.is_magic_code_consistent()) {
            group.set_detail("path does not point to a Calico DB database");
            group.set_hint("magic code is {}, but should be {}", header.magic_code(), FileHeader::MAGIC_CODE);
            return ErrorResult {group.invalid_argument(logger)};
        }
        if (!header.is_header_crc_consistent()) {
            group.set_detail("header has an inconsistent CRC");
            group.set_hint("CRC is {}", header.header_crc());
            return ErrorResult {group.corruption(logger)};
        }

    } else {
        if (!temp.error().is_system_error())
            return ErrorResult {temp.error()};

        header.update_magic_code();
        header.set_page_size(options.page_size);
        header.update_header_crc();
        is_new = true;

        // Try to create the file. If it doesn't work this time, we've got a problem.
        std::unique_ptr<IFile> file;
        const auto mode = Mode::READ_WRITE | Mode::CREATE;
        CCO_TRY_ASSIGN(file, directory.open_file(DATA_NAME, mode, perm));
    }

    const auto choose_error = [is_new, &logger](utils::ErrorMessage group) {
        return ErrorResult {is_new
            ? group.invalid_argument(logger)
            : group.corruption(logger)
        };
    };

    if (header.page_size() < MINIMUM_PAGE_SIZE) {
        group.set_detail("page size is too small");
        group.set_hint("must be greater than or equal to {}", MINIMUM_PAGE_SIZE);
        return choose_error(group);
    }
    if (header.page_size() > MAXIMUM_PAGE_SIZE) {
        group.set_detail("page size is too large");
        group.set_hint("must be less than or equal to {}", MAXIMUM_PAGE_SIZE);
        return choose_error(group);
    }
    if (!is_power_of_two(header.page_size())) {
        group.set_detail("page size is invalid");
        group.set_hint("must be a power of 2");
        return choose_error(group);
    }

    return InitialState {
        std::move(header),
        revised,
        is_new,
    };
}

} // calico

auto operator<(const calico::Record &lhs, const calico::Record &rhs) -> bool
{
    return calico::stob(lhs.key) < calico::stob(rhs.key);
}

auto operator>(const calico::Record &lhs, const calico::Record &rhs) -> bool
{
    return calico::stob(lhs.key) > calico::stob(rhs.key);
}

auto operator<=(const calico::Record &lhs, const calico::Record &rhs) -> bool
{
    return calico::stob(lhs.key) <= calico::stob(rhs.key);
}

auto operator>=(const calico::Record &lhs, const calico::Record &rhs) -> bool
{
    return calico::stob(lhs.key) >= calico::stob(rhs.key);
}

auto operator==(const calico::Record &lhs, const calico::Record &rhs) -> bool
{
    return calico::stob(lhs.key) == calico::stob(rhs.key);
}

auto operator!=(const calico::Record &lhs, const calico::Record &rhs) -> bool
{
    return calico::stob(lhs.key) != calico::stob(rhs.key);
}
