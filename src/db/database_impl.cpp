
#include "database_impl.h"
#include "cursor_impl.h"
#include "exception.h"
#include "file/file.h"
#include "file/system.h"
#include "page/file_header.h"
#include "pool/buffer_pool.h"
#include "pool/in_memory.h"
#include "tree/tree.h"
#include "utils/layout.h"
#include "utils/utils.h"
#include "wal/wal_reader.h"
#include "wal/wal_writer.h"

namespace cub {

auto Record::operator<(const Record &rhs) const -> bool
{
    return compare_three_way(_b(key), _b(rhs.key)) == ThreeWayComparison::LT;
}

Info::Info(Database::Impl *db)
    : m_db{db} {}

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

auto Info::transaction_size() const -> Size
{
    return m_db->transaction_size();
}

Database::Impl::~Impl()
{
    try {
        if (m_pool->can_commit()) {
            commit();
            system::unlink(get_wal_path(m_path));
        }
    } catch (...) {
        // Leave recovery to the next startup.
    }
}

Database::Impl::Impl(Parameters param)
    : m_path{param.path}
{
    auto wal_reader = std::make_unique<WALReader>(
        std::move(param.wal_reader_file),
        param.file_header.block_size());

    auto wal_writer = std::make_unique<WALWriter>(
        std::move(param.wal_writer_file),
        param.file_header.block_size());

    m_pool = std::make_unique<BufferPool>(BufferPool::Parameters{
        std::move(param.database_file),
        std::move(wal_reader),
        std::move(wal_writer),
        param.file_header.flushed_lsn(),
        param.frame_count,
        param.file_header.page_count(),
        param.file_header.page_size(),
    });

    m_tree = std::make_unique<Tree>(Tree::Parameters{
        m_pool.get(),
        param.file_header.free_start(),
        param.file_header.free_count(),
        param.file_header.key_count(),
        param.file_header.node_count(),
    });

    if (m_pool->page_count()) {
        m_pool->recover();
    } else {
        auto root = m_tree->allocate_node(PageType::EXTERNAL_NODE);
        FileHeader header{root};
        header.update_magic_code();
        header.set_page_size(param.file_header.page_size());
        header.set_block_size(param.file_header.block_size());
        root.take();
        commit();
    }
}

Database::Impl::Impl(Size page_size)
    : m_path {"<Temp DB>"}
{
    m_pool = std::make_unique<InMemory>(page_size);
    m_tree = std::make_unique<Tree>(Tree::Parameters{m_pool.get(), PID::null(), 0, 0, 0});
    m_tree->allocate_node(PageType::EXTERNAL_NODE);
    save_header();
    commit();
}

auto Database::Impl::recover() -> void
{
    if (m_pool->recover())
        load_header();
}

auto Database::Impl::commit() -> void
{
    std::unique_lock lock{m_mutex};
    if (m_pool->can_commit()) {
        save_header();
        m_pool->commit();
        m_transaction_size = 0;
    }
}

auto Database::Impl::save_header() -> void
{
    auto root = m_tree->find_root(true);
    FileHeader header{root};
    m_pool->save_header(header);
    m_tree->save_header(header);
    header.update_header_crc();
}

auto Database::Impl::load_header() -> void
{
    auto root = m_tree->find_root(true);
    FileHeader header{root};
    m_pool->load_header(header);
    m_tree->load_header(header);
}

auto Database::Impl::abort() -> void
{
    std::unique_lock lock{m_mutex};
    if (m_pool->can_commit()) {
        m_pool->abort();
        m_transaction_size = 0;
        load_header();
    }
}

auto Database::Impl::get_info() -> Info
{
    return Info{this};
}

auto Database::Impl::get_cursor() -> Cursor
{
    Cursor cursor;
    cursor.m_impl = std::make_unique<Cursor::Impl>(m_tree.get(), m_mutex);
    return cursor;
}

auto Database::Impl::lookup(BytesView key, bool exact) -> std::optional<Record>
{
    auto cursor = get_cursor();
    const auto found_exact = cursor.find(key);
    const auto found_greater = !cursor.is_maximum();
    if (found_exact || (found_greater && !exact))
        return Record {_s(cursor.key()), cursor.value()};
    return std::nullopt;
}

auto Database::Impl::lookup_minimum() -> std::optional<Record>
{
    std::shared_lock lock{m_mutex};
    if (!m_tree->cell_count())
        return std::nullopt;
    auto cursor = get_cursor();
    cursor.find_minimum();
    return Record {_s(cursor.key()), cursor.value()};
}

auto Database::Impl::lookup_maximum() -> std::optional<Record>
{
    std::shared_lock lock{m_mutex};
    if (!m_tree->cell_count())
        return std::nullopt;
    auto cursor = get_cursor();
    cursor.find_maximum();
    return Record {_s(cursor.key()), cursor.value()};
}

auto Database::Impl::insert(BytesView key, BytesView value) -> void
{
    std::unique_lock lock{m_mutex};
    m_tree->insert(key, value);
    m_transaction_size++;
}

auto Database::Impl::remove(BytesView key) -> bool
{
    std::unique_lock lock{m_mutex};
    if (m_tree->remove(key)) {
        m_transaction_size++;
        return true;
    }
    return false;
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

auto Database::Impl::transaction_size() const -> Size
{
    return m_transaction_size;
}

auto Database::open(const std::string &path, const Options &options) -> Database
{
    if (path.empty())
        throw InvalidArgumentError{"Path argument cannot be empty"};

    std::string backing(FileLayout::HEADER_SIZE, '\x00');
    FileHeader header{_b(backing)};

    try {
        ReadOnlyFile file{path, {}, options.permissions};

        if (file.size() < FileLayout::HEADER_SIZE)
            throw CorruptionError{"Database is too small to read the file header"};

        read_exact(file, _b(backing));

        if (!header.is_magic_code_consistent())
            throw InvalidArgumentError{"Path does not point to a Cub DB database file"};

        if (!header.is_header_crc_consistent())
            throw CorruptionError{"Database has a corrupted file header"};

        if (file.size() < header.page_size())
            throw CorruptionError{"Database size is invalid"};

    } catch (const SystemError &error) {
        if (error.code() != std::errc::no_such_file_or_directory)
            throw;

        header.update_magic_code();
        header.set_page_size(options.page_size);
        header.set_block_size(options.block_size);
    }

    const auto mode = Mode::CREATE | Mode::DIRECT | Mode::SYNCHRONOUS;
    auto database_file = std::make_unique<ReadWriteFile>(path, mode, options.permissions);
    auto wal_reader_file = std::make_unique<ReadOnlyFile>(get_wal_path(path), mode, options.permissions);
    auto wal_writer_file = std::make_unique<LogFile>(get_wal_path(path), mode, options.permissions);

#if !CUB_HAS_O_DIRECT
    try {
        database_file->use_direct_io();
        wal_reader_file->use_direct_io();
        wal_writer_file->use_direct_io();
    } catch (const SystemError &error) {
        // We don't need to have kernel page caching turned off, but it can greatly
        // boost performance. TODO: Log, or otherwise notify the user? Maybe a flag in Options to control whether we fail here?
        throw; // TODO: Rethrowing for now.
    }
#endif

    Database db;
    db.m_impl = std::make_unique<Impl>(Impl::Parameters{
        path,
        std::move(database_file),
        std::move(wal_reader_file),
        std::move(wal_writer_file),
        header,
        options.frame_count,
    });
    return db;
}

auto Database::temp(Size page_size) -> Database
{
    Database db;
    db.m_impl = std::make_unique<Impl>(page_size);
    return db;
}

Database::Database() = default;
Database::~Database() = default;
Database::Database(Database&&) noexcept = default;
auto Database::operator=(Database&&) noexcept -> Database& = default;

auto Database::lookup(BytesView key, bool exact) -> std::optional<Record>
{
    return m_impl->lookup(key, exact);
}

auto Database::lookup_minimum() -> std::optional<Record>
{
    return m_impl->lookup_minimum();
}

auto Database::lookup_maximum() -> std::optional<Record>
{
    return m_impl->lookup_maximum();
}

auto Database::insert(BytesView key, BytesView value) -> void
{
    m_impl->insert(key, value);
}

auto Database::remove(BytesView key) -> bool
{
    return m_impl->remove(key);
}

auto Database::commit() -> void
{
    m_impl->commit();
}

auto Database::abort() -> void
{
    m_impl->abort();
}

auto Database::get_cursor() -> Cursor
{
    return m_impl->get_cursor();
}

auto Database::get_info() -> Info
{
    return m_impl->get_info();
}

} // cub