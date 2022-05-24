
#include "database.h"
#include "database_impl.h"
#include "exception.h"
#include "cursor.h"
#include "file/file.h"
#include "page/file_header.h"
#include "pool/buffer_pool.h"
#include "cursor_impl.h"
#include "tree/tree.h"
#include "utils/layout.h"
#include "utils/utils.h"
#include "wal/wal_reader.h"
#include "wal/wal_writer.h"

namespace cub {

Database::Impl::~Impl()
{
    // TODO: We could try to abort if the commit() fails. For now we leave any recovery to the next startup.
    if (m_pool->can_commit())
        try {commit();} catch (...) {}
}

Database::Impl::Impl(Parameters param)
{
    auto wal_reader = std::make_unique<WALReader>(
        std::move(param.wal_reader_file),
        param.file_header.block_size()
    );

    auto wal_writer = std::make_unique<WALWriter>(
        std::move(param.wal_writer_file),
        param.file_header.block_size()
    );

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
        FileHeader header {root};
        header.update_magic_code();
        header.set_page_size(param.file_header.page_size());
        header.set_block_size(param.file_header.block_size());
        root.take();
        commit();
    }
}

auto Database::Impl::commit() -> void
{
    auto root = m_tree->find_root(true);
    FileHeader header {root};
    m_pool->save_header(header);
    m_tree->save_header(header);
    header.update_header_crc();
    root.reset();
    m_pool->commit();
}

auto Database::Impl::abort() -> void
{
    m_pool->abort();
}

auto Database::Impl::get_info() -> Info
{
    return {};
}

auto Database::Impl::get_cursor() -> Cursor
{
    Cursor cursor;
    cursor.m_impl = std::make_unique<Cursor::Impl>(m_tree.get());
    return cursor;
}

auto Database::Impl::lookup(BytesView key, std::string &result) -> bool
{
    return m_tree->lookup(key, result);
}

auto Database::Impl::insert(BytesView key, BytesView value) -> void
{
    m_tree->insert(key, value);
}

auto Database::Impl::remove(BytesView key) -> bool
{
    return m_tree->remove(key);
}

auto Database::open(const std::string &path, const Options &options) -> Database
{
    if (path.empty())
        throw InvalidArgumentError{"Path argument must be nonempty"};

    std::string backing(FileLayout::HEADER_SIZE, '\x00');
    FileHeader header {_b(backing)};

    try {
        ReadOnlyFile file {path, {}, options.permissions};

        if (file.size() < FileLayout::HEADER_SIZE)
            throw CorruptionError {"Database is too small to read the file header"};

        read_exact(file, _b(backing));

        if (!header.is_magic_code_consistent())
            throw InvalidArgumentError {"Path does not point to a Cub DB database file"};

        // TODO: Check for a WAL and try to recover. Put this whole method in another method and catch CorruptionErrors?
        if (!header.is_header_crc_consistent())
            throw CorruptionError {"Database has a corrupted file header"};

        if (file.size() < header.page_size())
            throw CorruptionError {"Database size is invalid"};

    } catch (const SystemError &error) {
        if (error.code() != std::errc::no_such_file_or_directory)
            throw;

        header.update_magic_code();
        header.set_page_size(options.page_size);
        header.set_block_size(options.block_size);
    }

    const auto mode = Mode::CREATE | Mode::DIRECT | Mode::SYNCHRONOUS;
    auto database_file = std::make_unique<ReadWriteFile>(path, mode, options.permissions);
    auto wal_reader_file = std::make_unique<ReadOnlyFile>(compose_wal_path(path), mode, options.permissions);
    auto wal_writer_file = std::make_unique<LogFile>(compose_wal_path(path), mode, options.permissions);

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
        std::move(database_file),
        std::move(wal_reader_file),
        std::move(wal_writer_file),
        header,
        options.frame_count,
    });
    return db;
}

Database::Database() = default;
Database::~Database() = default;
Database::Database(Database&&) noexcept = default;
auto Database::operator=(Database&&) noexcept -> Database& = default;

auto Database::lookup(BytesView key, std::string &result) -> bool
{
    return m_impl->lookup(key, result);
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