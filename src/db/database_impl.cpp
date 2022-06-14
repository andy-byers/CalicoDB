
#include "database_impl.h"
#include "cursor_impl.h"
#include "batch_impl.h"
#include "cub/exception.h"
#include "cub/batch.h"
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
    return compare_three_way(_b(key), _b(rhs.key)) == Comparison::LT;
}

Info::Info(Database::Impl *db)
    : m_db {db} {}

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

Database::Impl::~Impl()
{
    try {
        unlocked_commit();
        system::unlink(get_wal_path(m_path));
    } catch (...) {
        // Leave recovery to the next startup.
    }
}

Database::Impl::Impl(Parameters param)
    : m_path {param.path}
{
    auto wal_reader = std::make_unique<WALReader>(
        std::move(param.wal_reader_file),
        param.file_header.block_size());

    auto wal_writer = std::make_unique<WALWriter>(
        std::move(param.wal_writer_file),
        param.file_header.block_size());

    m_pool = std::make_unique<BufferPool>(BufferPool::Parameters {
        std::move(param.database_file),
        std::move(wal_reader),
        std::move(wal_writer),
        param.file_header.flushed_lsn(),
        param.frame_count,
        param.file_header.page_count(),
        param.file_header.page_size(),
    });

    m_tree = std::make_unique<Tree>(Tree::Parameters {
        m_pool.get(),
        param.file_header.free_start(),
        param.file_header.free_count(),
        param.file_header.record_count(),
        param.file_header.node_count(),
    });

    if (m_pool->page_count()) {
        m_pool->recover();
    } else {
        auto root = m_tree->allocate_node(PageType::EXTERNAL_NODE);
        FileHeader header {root};
        header.update_magic_code();
        header.set_page_size(param.file_header.page_size());
        header.set_block_size(param.file_header.block_size());
        root.take();
        unlocked_commit();
    }
}

Database::Impl::Impl(Size page_size)
    : m_path {"<Temp DB>"}
    , m_pool {std::make_unique<InMemory>(page_size)}
    , m_tree {std::make_unique<Tree>(Tree::Parameters {m_pool.get(), PID::null(), 0, 0, 0})}
{
    m_tree->allocate_node(PageType::EXTERNAL_NODE);
    unlocked_commit();
}

auto Database::Impl::recover() -> void
{
    if (m_pool->recover())
        load_header();
}

auto Database::Impl::read(BytesView key, Comparison comparison) -> std::optional<Record>
{
    std::shared_lock lock {m_mutex};
    return unlocked_read(key, comparison);
}

auto Database::Impl::read_minimum() -> std::optional<Record>
{
    std::shared_lock lock {m_mutex};
    return unlocked_read_minimum();
}

auto Database::Impl::read_maximum() -> std::optional<Record>
{
    std::shared_lock lock {m_mutex};
    return unlocked_read_maximum();
}

auto Database::Impl::write(BytesView key, BytesView value) -> bool
{
    std::unique_lock lock {m_mutex};
    return unlocked_write(key, value);
}

auto Database::Impl::erase(BytesView key) -> bool
{
    std::unique_lock lock {m_mutex};
    return unlocked_erase(key);
}

auto Database::Impl::commit() -> void
{
    std::unique_lock lock {m_mutex};
    unlocked_commit();
}

auto Database::Impl::abort() -> void
{
    std::unique_lock lock {m_mutex};
    unlocked_abort();
}

auto Database::Impl::get_info() -> Info
{
    return Info {this};
}

auto Database::Impl::get_iterator() -> Iterator
{
    return Iterator{m_tree.get()};
}

auto Database::Impl::get_cursor() -> Cursor
{
    Cursor reader;
    reader.m_impl = std::make_unique<Cursor::Impl>(m_tree.get(), m_mutex);
    return reader;
}

auto Database::Impl::get_batch() -> Batch
{
    Batch writer;
    writer.m_impl = std::make_unique<Batch::Impl>(this, m_mutex);
    return writer;
}

auto Database::Impl::unlocked_read(BytesView key, Comparison comparison) -> std::optional<Record>
{
    if (auto itr = get_iterator(); itr.has_record()) {
        const auto found_exact = itr.find(key);
        switch (comparison) {
            case Comparison::EQ:
                if (!found_exact)
                    return std::nullopt;
                break;
            case Comparison::GT:
                if (itr.is_maximum() && (found_exact || itr.key() < key))
                    return std::nullopt;
                if (found_exact && !itr.increment())
                    return std::nullopt;
                break;
            case Comparison::LT:
                if (itr.is_maximum() && itr.key() < key)
                    break;
                if (!itr.decrement())
                    return std::nullopt;
                break;
        }
        return Record {_s(itr.key()), itr.value()};
    }
    CUB_EXPECT_EQ(m_tree->cell_count(), 0);
    return std::nullopt;
}

auto Database::Impl::unlocked_read_minimum() -> std::optional<Record>
{
    if (auto cursor = get_iterator(); cursor.has_record()) {
        cursor.find_minimum();
        return Record {_s(cursor.key()), cursor.value()};
    }
    return std::nullopt;
}

auto Database::Impl::unlocked_read_maximum() -> std::optional<Record>
{
    if (auto cursor = get_iterator(); cursor.has_record()) {
        cursor.find_maximum();
        return Record {_s(cursor.key()), cursor.value()};
    }
    return std::nullopt;
}

auto Database::Impl::unlocked_write(BytesView key, BytesView value) -> bool
{
    return m_tree->insert(key, value);
}

auto Database::Impl::unlocked_erase(BytesView key) -> bool
{
    return m_tree->remove(key);
}

auto Database::Impl::unlocked_commit() -> bool
{
    if (m_pool->can_commit()) {
        save_header();
        m_pool->commit();
        return true;
    }
    return false;
}

auto Database::Impl::unlocked_abort() -> bool
{
    if (m_pool->can_commit()) {
        m_pool->abort();
        load_header();
        return true;
    }
    return false;
}

auto Database::Impl::save_header() -> void
{
    auto root = m_tree->find_root(true);
    FileHeader header {root};
    m_pool->save_header(header);
    m_tree->save_header(header);
    header.update_header_crc();
}

auto Database::Impl::load_header() -> void
{
    auto root = m_tree->find_root(true);
    FileHeader header {root};
    m_pool->load_header(header);
    m_tree->load_header(header);
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

auto Database::open(const std::string &path, const Options &options) -> Database
{
    if (path.empty())
        throw std::invalid_argument {"Path argument cannot be empty"};

    std::string backing(FileLayout::HEADER_SIZE, '\x00');
    FileHeader header {_b(backing)};

    try {
        ReadOnlyFile file {path, {}, options.permissions};
        const auto file_size = file.size();

        if (file_size < FileLayout::HEADER_SIZE)
            throw CorruptionError {"Database is too small to read the file header"};

        read_exact(file, _b(backing));

        if (!header.is_magic_code_consistent())
            throw std::invalid_argument {"Path does not point to a Cub DB database, or the file is corrupted"};

        if (!header.is_header_crc_consistent())
            throw CorruptionError {"Database has an inconsistent header CRC"};

        if (file_size < header.page_size())
            throw CorruptionError {"Database cannot be less than one page in size"};

    } catch (const std::system_error &error) {
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
    } catch (const std::system_error &error) {
        // TODO: Log, or otherwise notify the user? Maybe a flag in Options to control whether we fail here?
        throw; // TODO: Rethrowing for now.
    }
#endif

    Database db;
    db.m_impl = std::make_unique<Impl>(Impl::Parameters {
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

auto Database::read(BytesView key, Comparison comparison) const -> std::optional<Record>
{
    return m_impl->read(key, comparison);
}

auto Database::read_minimum() const -> std::optional<Record>
{
    return m_impl->read_minimum();
}

auto Database::read_maximum() const -> std::optional<Record>
{
    return m_impl->read_maximum();
}

auto Database::write(BytesView key, BytesView value) -> bool
{
    return m_impl->write(key, value);
}

auto Database::erase(BytesView key) -> bool
{
    return m_impl->erase(key);
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

auto Database::get_batch() -> Batch
{
    return m_impl->get_batch();
}

auto Database::get_info() -> Info
{
    return m_impl->get_info();
}

} // cub