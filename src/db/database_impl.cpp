
#include "database_impl.h"
#include <spdlog/sinks/basic_file_sink.h>
#include "calico/cursor.h"
#include "file/file.h"
#include "file/system.h"
#include "pool/buffer_pool.h"
#include "pool/in_memory.h"
#include "tree/node_pool.h"
#include "tree/tree.h"
#include "utils/layout.h"
#include "utils/logging.h"
#include "utils/utils.h"
#include "wal/wal_reader.h"
#include "wal/wal_writer.h"

namespace calico {

auto initialize_log(spdlog::logger &logger, const Database::Impl::Parameters &param)
{
    logger.info("starting CalicoDB v{} at \"{}\"", VERSION_NAME, param.path);
    logger.info("WAL is located at \"{}\"", get_wal_path(param.path));
    logger.info("log is located at \"{}\"", param.options.log_path);
}

auto Record::operator<(const Record &rhs) const -> bool
{
    return stob(key) < stob(rhs.key);
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

auto Info::maximum_key_size() const -> Size
{
    return get_max_local(page_size());
}

auto Info::uses_transactions() const -> bool
{
    return m_db->uses_transactions();
}

auto Info::is_temp() const -> bool
{
    return m_db->is_temp();
}

Database::Impl::~Impl()
{
    auto committed = true;

    try {
        commit();
    } catch (const std::exception &error) {
        logging::MessageGroup group;
        group.set_primary("cannot commit");
        group.set_detail("{}", error.what());
        group.err(*m_logger);
        committed = false;
    }

    try {
        if (uses_transactions() && committed && !m_is_temp)
            system::unlink(get_wal_path(m_path));
    } catch (const std::exception &error) {
        logging::MessageGroup group;
        group.set_primary("cannot unlink WAL");
        group.set_detail("{}", error.what());
        group.err(*m_logger);
    }
}

Database::Impl::Impl(Parameters param)
    : m_sink {logging::create_sink(param.options.log_path, param.options.log_level)}
    , m_logger {logging::create_logger(m_sink, "Database")}
    , m_path {param.path}
{
    initialize_log(*m_logger, param);
    m_logger->trace("constructing Database object");

    std::unique_ptr<WALReader> wal_reader;
    std::unique_ptr<WALWriter> wal_writer;

    if (param.options.use_transactions) {
        CALICO_EXPECT_NOT_NULL(param.wal_reader_file);
        const auto wal_path = get_wal_path(m_path);
        wal_reader = std::make_unique<WALReader>(WALReader::Parameters {
            wal_path,
            std::move(param.wal_reader_file),
            m_sink,
            param.header.block_size(),
        });
        CALICO_EXPECT_NOT_NULL(param.wal_writer_file);
        wal_writer = std::make_unique<WALWriter>(WALWriter::Parameters {
            wal_path,
            std::move(param.wal_writer_file),
            m_sink,
            param.header.block_size(),
        });
    }

    m_pool = std::make_unique<BufferPool>(BufferPool::Parameters {
        std::move(param.database_file),
        std::move(wal_reader),
        std::move(wal_writer),
        m_sink,
        param.header.flushed_lsn(),
        param.options.frame_count,
        param.header.page_count(),
        param.header.page_size(),
        param.options.use_transactions,
    });

    m_tree = std::make_unique<Tree>(Tree::Parameters {
        m_pool.get(),
        m_sink,
        param.header.free_start(),
        param.header.free_count(),
        param.header.record_count(),
        param.header.node_count(),
    });

    if (m_pool->page_count()) {
        // This will do nothing if the WAL is empty.
        if (param.options.use_transactions)
            m_pool->recover();
    } else {
        auto root = m_tree->root(true);
        FileHeader header {root};
        header.update_magic_code();
        header.set_page_size(param.header.page_size());
        header.set_block_size(param.header.block_size());
        root.take();
        commit();
    }
}

Database::Impl::Impl(Parameters param, InMemoryTag)
    : m_sink {logging::create_sink(param.options.log_path, param.options.log_level)}
    , m_logger {logging::create_logger(m_sink, "Database")}
    , m_is_temp {true}
{
    initialize_log(*m_logger, param);
    m_pool = std::make_unique<InMemory>(param.options.page_size, param.options.use_transactions, m_sink);
    m_tree = std::make_unique<Tree>(Tree::Parameters {m_pool.get(), m_sink, PID::null(), 0, 0, 0});
    if (param.options.use_transactions)
        commit();
}

auto Database::Impl::recover() -> void
{
    if (m_pool->recover())
        load_header();
}

auto Database::Impl::info() -> Info
{
    Info info;
    info.m_db = this;
    return info;
}

auto Database::Impl::find(BytesView key, bool require_exact) -> Cursor
{
    if (key.is_empty()) {
        logging::MessageGroup group;
        group.set_primary("cannot read record");
        group.set_detail("key cannot be empty");
        throw std::invalid_argument {group.err(*m_logger)};
    }
    return m_tree->find(key, require_exact);
}

auto Database::Impl::find_minimum() -> Cursor
{
    return m_tree->find_minimum();
}

auto Database::Impl::find_maximum() -> Cursor
{
    return m_tree->find_maximum();
}

auto Database::Impl::insert(BytesView key, BytesView value) -> bool
{
    return m_tree->insert(key, value);
}

auto Database::Impl::erase(BytesView key) -> bool
{
    return erase(m_tree->find(key, true));
}

auto Database::Impl::erase(Cursor cursor) -> bool
{
    return m_tree->remove(cursor);
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
    if (!m_pool->uses_transactions()) {
        logging::MessageGroup group;
        group.set_primary("cannot abort transaction");
        group.set_detail("transactions are disabled");
        throw std::logic_error {group.err(*m_logger)};
    }

    if (m_pool->can_commit()) {
        m_pool->abort();
        load_header();
        return true;
    }
    return false;
}

auto Database::Impl::save_header() -> void
{
    auto root = m_tree->root(true);
    FileHeader header {root};
    m_pool->save_header(header);
    m_tree->save_header(header);
    header.update_header_crc();
}

auto Database::Impl::load_header() -> void
{
    auto root = m_tree->root(true);
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

auto Database::Impl::uses_transactions() const -> Size
{
    return m_pool->uses_transactions();
}

auto Database::Impl::is_temp() const -> bool
{
    return m_is_temp;
}

auto get_initial_state(const std::string &path, const Options &options) -> InitialState
{
    if (path.empty())
        throw std::invalid_argument {"could not open database: path argument cannot be empty"};

    auto use_transactions = options.use_transactions;
    FileHeader header;

    try {
        ReadOnlyFile file {path, {}, options.permissions};
        const auto file_size = file.size();

        if (file_size < FileLayout::HEADER_SIZE)
            throw CorruptionError {"could not read file header: database is too small"};

        read_exact(file, header.data());

        if (!header.is_magic_code_consistent())
            throw std::invalid_argument {"cannot read file header: path does not point to a Cub DB database"};

        if (!header.is_header_crc_consistent())
            throw CorruptionError {"cannot read file header: header has an inconsistent CRC"};

        if (file_size < header.page_size())
            throw CorruptionError {"cannot read file header: database is less than one page in size"};

        // If the database does not use transactions, this field will always be 0.
        use_transactions = !header.flushed_lsn().is_null();

    } catch (const std::system_error &error) {
        if (error.code() != std::errc::no_such_file_or_directory)
            throw;

        header.update_magic_code();
        header.set_page_size(options.page_size);
        header.set_block_size(options.block_size);
        header.update_header_crc();
    }
    return {std::move(header), use_transactions};
}

auto get_open_files(const std::string &path, const Options &options) -> OpenFiles
{
    const auto mode = Mode::CREATE | (options.use_direct_io ? Mode::DIRECT : Mode {});
    auto database_file = std::make_unique<ReadWriteFile>(path, mode, options.permissions);
    std::unique_ptr<ReadOnlyFile> wal_reader_file;
    std::unique_ptr<LogFile> wal_writer_file;

    if (options.use_transactions) {
        wal_reader_file = std::make_unique<ReadOnlyFile>(get_wal_path(path), mode, options.permissions);
        wal_writer_file = std::make_unique<LogFile>(get_wal_path(path), mode, options.permissions);
    }

    return {
        std::move(database_file),
        std::move(wal_reader_file),
        std::move(wal_writer_file),
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
