
#include "database_impl.h"
#include "cursor_impl.h"
#include "cub/exception.h"
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
    return _b(key) < _b(rhs.key);
}

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

Database::Impl::~Impl()
{
    try {
        commit();
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
        commit();
    }
}

Database::Impl::Impl(Size page_size)
    : m_pool {std::make_unique<InMemory>(page_size)}
    , m_tree {std::make_unique<Tree>(Tree::Parameters {m_pool.get(), PID::null(), 0, 0, 0})}
{
    m_tree->allocate_node(PageType::EXTERNAL_NODE);
    commit();
}

auto Database::Impl::recover() -> void
{
    if (m_pool->recover())
        load_header();
}

auto Database::Impl::get_info() -> Info
{
    Info info;
    info.m_db = this;
    return info;
}

auto Database::Impl::get_cursor() -> Cursor
{
    Cursor reader;
    reader.m_impl = std::make_unique<Cursor::Impl>(m_tree.get());
    return reader;
}

auto Database::Impl::read(BytesView key, Ordering ordering) -> std::optional<Record>
{
    if (auto cursor = get_cursor(); cursor.has_record()) {
        const auto found_exact = cursor.find(key);
        switch (ordering) {
            case Ordering::EQ:
                if (!found_exact)
                    return std::nullopt;
                break;
            case Ordering::GE:
                if (found_exact)
                    break;
            case Ordering::GT:
                if (cursor.is_maximum() && (found_exact || cursor.key() < key))
                    return std::nullopt;
                if (found_exact && !cursor.increment())
                    return std::nullopt;
                break;
            case Ordering::LE:
                if (found_exact)
                    break;
            case Ordering::LT:
                if (cursor.is_maximum() && cursor.key() < key)
                    break;
                if (!cursor.decrement())
                    return std::nullopt;
                break;
            default:
                throw std::invalid_argument {"Unknown ordering"};
        }
        return Record {_s(cursor.key()), cursor.value()};
    }
    CUB_EXPECT_EQ(m_tree->cell_count(), 0);
    return std::nullopt;
}

auto Database::Impl::read_minimum() -> std::optional<Record>
{
    if (auto cursor = get_cursor(); cursor.has_record()) {
        cursor.find_minimum();
        return Record {_s(cursor.key()), cursor.value()};
    }
    return std::nullopt;
}

auto Database::Impl::read_maximum() -> std::optional<Record>
{
    if (auto cursor = get_cursor(); cursor.has_record()) {
        cursor.find_maximum();
        return Record {_s(cursor.key()), cursor.value()};
    }
    return std::nullopt;
}

auto Database::Impl::write(BytesView key, BytesView value) -> bool
{
    return m_tree->insert(key, value);
}

auto Database::Impl::erase(BytesView key) -> bool
{
    return m_tree->remove(key);
}

auto Database::Impl::commit() -> bool
{
    if (m_pool->can_commit()) {
        save_header();
        m_pool->commit();
        return true;
    }
    return false;
}

auto Database::Impl::abort() -> bool
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

auto Database::Impl::page_size() const -> Size
{
    return m_pool->page_size();
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

    const auto mode = Mode::CREATE | (options.use_direct_io ? Mode::DIRECT : Mode {});
    auto database_file = std::make_unique<ReadWriteFile>(path, mode, options.permissions);
    auto wal_reader_file = std::make_unique<ReadOnlyFile>(get_wal_path(path), mode, options.permissions);
    auto wal_writer_file = std::make_unique<LogFile>(get_wal_path(path), mode, options.permissions);

#if !CUB_HAS_O_DIRECT
    try {
        if (options.use_direct_io) {
            database_file->use_direct_io();
            wal_reader_file->use_direct_io();
            wal_writer_file->use_direct_io();
        }
    } catch (const std::system_error &error) {
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

auto Database::destroy(Database db) -> void
{
    if (const auto &path = db.m_impl->path(); !path.empty()) {
        system::unlink(path);
        system::unlink(get_wal_path(path));
    }
}

Database::Database() = default;

Database::~Database() = default;

Database::Database(Database&&) noexcept = default;

auto Database::operator=(Database&&) noexcept -> Database& = default;

auto Database::read(BytesView key, Ordering ordering) const -> std::optional<Record>
{
    return m_impl->read(key, ordering);
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

auto Database::write(const Record &record) -> bool
{
    const auto &[key, value] = record;
    return m_impl->write(_b(key), _b(value));
}

auto Database::erase(BytesView key) -> bool
{
    return m_impl->erase(key);
}

auto Database::commit() -> bool
{
    return m_impl->commit();
}

auto Database::abort() -> bool
{
    return m_impl->abort();
}

auto Database::get_cursor() const -> Cursor
{
    return m_impl->get_cursor();
}

auto Database::get_info() const -> Info
{
    return m_impl->get_info();
}

} // cub

auto operator<(const cub::Record &lhs, const cub::Record &rhs) -> bool
{
    return cub::_b(lhs.key) < cub::_b(rhs.key);
}

auto operator>(const cub::Record &lhs, const cub::Record &rhs) -> bool
{
    return cub::_b(lhs.key) > cub::_b(rhs.key);
}

auto operator<=(const cub::Record &lhs, const cub::Record &rhs) -> bool
{
    return cub::_b(lhs.key) <= cub::_b(rhs.key);
}

auto operator>=(const cub::Record &lhs, const cub::Record &rhs) -> bool
{
    return cub::_b(lhs.key) >= cub::_b(rhs.key);
}

auto operator==(const cub::Record &lhs, const cub::Record &rhs) -> bool
{
    return cub::_b(lhs.key) == cub::_b(rhs.key);
}

auto operator!=(const cub::Record &lhs, const cub::Record &rhs) -> bool
{
    return cub::_b(lhs.key) != cub::_b(rhs.key);
}