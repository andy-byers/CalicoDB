
#include "core.h"
#include "calico/calico.h"
#include "calico/storage.h"
#include "calico/transaction.h"
#include "pager/basic_pager.h"
#include "pager/pager.h"
#include "recovery.h"
#include "storage/helpers.h"
#include "storage/posix_storage.h"
#include "tree/bplus_tree.h"
#include "tree/cursor_internal.h"
#include "utils/crc.h"
#include "utils/header.h"
#include "utils/layout.h"
#include "utils/system.h"
#include "wal/basic_wal.h"

namespace Calico {

[[nodiscard]] static auto sanitize_options(const Options &options) -> Options
{
    static constexpr Size KiB {1024};

    const auto page_size = options.page_size;
    const auto scratch_size = wal_scratch_size(page_size);
    auto page_cache_size = options.page_cache_size;
    auto wal_buffer_size = options.wal_buffer_size;

    if (options.page_size <= 2 * KiB) {
        page_cache_size = 2048 * page_size;
        wal_buffer_size = 1024 * scratch_size;
    } else if (options.page_size <= 16 * KiB) {
        page_cache_size = 256 * page_size;
        wal_buffer_size = 128 * scratch_size;
    } else {
        page_cache_size = 128 * page_size;
        wal_buffer_size = 64 * scratch_size;
    }

    auto sanitized = options;
    if (sanitized.page_cache_size == 0)
        sanitized.page_cache_size = page_cache_size;
    if (sanitized.wal_buffer_size == 0)
        sanitized.wal_buffer_size = wal_buffer_size;
    return sanitized;
}

auto Statistics::record_count() const -> Size
{
    return m_core->tree().record_count();
}

auto Statistics::page_count() const -> Size
{
    return m_core->pager().page_count();
}

auto Statistics::page_size() const -> Size
{
    return m_core->pager().page_size();
}

auto Statistics::maximum_key_size() const -> Size
{
    return get_max_local(page_size());
}

auto Statistics::cache_hit_ratio() const -> double
{
    return m_core->pager().hit_ratio();
}

auto Core::open(Slice path, const Options &options) -> Status
{
    auto sanitized = sanitize_options(options);

    m_prefix = path.to_string();
    if (!m_prefix.ends_with('/'))
        m_prefix += '/';

    m_system = std::make_unique<System>(m_prefix, sanitized);
    m_log = m_system->create_log("core");

    // m_log->info("starting CalicoDB v{}.{}.{} at \"{}\"", CALICO_VERSION_MAJOR,
    //             CALICO_VERSION_MINOR, CALICO_VERSION_PATCH, path.to_string());
    // m_log->info("tree is located at \"{}{}\"", m_prefix, DATA_FILENAME);
    // m_log->info("log is located at \"{}{}\"", m_prefix, LOG_FILENAME);
    if (sanitized.wal_prefix.is_empty()) {
        // m_log->info("WAL prefix is \"{}{}\"", m_prefix, WAL_PREFIX);
    } else {
        // m_log->info("WAL prefix is \"{}\"", sanitized.wal_prefix.to_string());
    }

    m_store = sanitized.storage;
    if (m_store == nullptr) {
        m_store = new PosixStorage;
        m_owns_store = true;
    }

    auto initial = setup(m_prefix, *m_store, sanitized);
    if (!initial.has_value()) return initial.error();
    auto [state, is_new] = *initial;

    // The database will store 0 in the "page_size" header field if the maximum page size is used (1 << 16 cannot be held
    // in a std::uint16_t).
    if (!is_new) sanitized.page_size = decode_page_size(state.page_size);

    // Allocate the WAL object and buffers.
    {
        const auto scratch_size = wal_scratch_size(sanitized.page_size);
        const auto buffer_count = sanitized.wal_buffer_size / scratch_size;

        m_scratch = std::make_unique<LogScratchManager>(
            scratch_size,
            buffer_count);

        // The WAL segments may be stored elsewhere.
        auto wal_prefix = sanitized.wal_prefix.is_empty()
            ? m_prefix : sanitized.wal_prefix.to_string();
        if (!wal_prefix.ends_with('/'))
            wal_prefix += '/';

        const auto wal_limit = buffer_count * 32;

        auto r = BasicWriteAheadLog::open({
            wal_prefix,
            m_store,
            m_system.get(),
            sanitized.page_size,
            wal_limit,
            buffer_count,
        });
        if (!r.has_value())
            return r.error();
        m_wal = std::move(*r);
    }

    // Allocate the pager object and cache frames.
    {
        auto r = BasicPager::open({
            m_prefix,
            m_store,
            m_scratch.get(),
            &m_images,
            m_wal.get(),
            m_system.get(),
            sanitized.page_cache_size / sanitized.page_size,
            sanitized.page_size,
        });
        if (!r.has_value())
            return r.error();
        m_pager = std::move(*r);
        m_pager->load_state(state);
    }

    // Allocate the tree object.
    {
        auto r = BPlusTree::open(*m_pager, *m_system, sanitized.page_size);
        if (!r.has_value())
            return r.error();
        m_tree = std::move(*r);
        m_tree->load_state(state);
    }

    m_recovery = std::make_unique<Recovery>(*m_pager, *m_wal, *m_system);

    auto s = ok();
    if (is_new) {
        // The first call to root() allocates the root page.
        auto root = m_tree->root(true);
        if (!root.has_value())
            CALICO_TRY_S(root.error());
        CALICO_EXPECT_EQ(m_pager->page_count(), 1);

        state.page_count = 1;
        state.header_crc = compute_header_crc(state);
        write_header(root->page(), state);
        CALICO_TRY_S(m_pager->release(root->take()));

        // This is safe right now because the WAL has not been started. If successful, we will have the root page
        // set up and saved to the database file.
        CALICO_TRY_S(m_pager->flush({}));

    } else {
        // This should be a no-op if the database closed normally last time.
        CALICO_TRY_S(ensure_consistency_on_startup());
    }
    CALICO_ERROR_IF(m_wal->start_workers());
    return status();
}

Core::~Core()
{
    m_wal.reset();

    if (m_owns_store)
        delete m_store;
}

auto Core::destroy() -> Status
{
    // m_log->trace("destroy");
    auto s = ok();
    m_wal.reset();

    std::vector<std::string> children;
    s = m_store->get_children(m_prefix, children);

    if (s.is_ok()) {
        for (const auto &name: children)
            CALICO_WARN_IF(m_store->remove_file(name));

        // Remove the directory, which should be empty now. TODO: We may need some additional flag to force deletion if one of the files failed to unlink.
        CALICO_ERROR_IF(m_store->remove_directory(m_prefix));
    } else {
        CALICO_ERROR(s);
    }
    return s;
}

auto Core::status() const -> Status
{
    return m_system->has_error() ? m_system->original_error().status : ok();
}

auto Core::path() const -> std::string
{
    return m_prefix;
}

auto Core::statistics() -> Statistics
{
    return Statistics {*this};
}

auto Core::handle_errors() -> Status
{
    if (auto s = status(); !s.is_ok())
        return s;
    return ok();
}

#define MAYBE_FORWARD_AS_CURSOR \
    do { \
        if (auto s = handle_errors(); !s.is_ok()) { \
            auto c = CursorInternal::make_cursor(nullptr); \
            CursorInternal::invalidate(c, s); \
            return c; \
        } \
    } while (0)

auto Core::find_exact(Slice key) -> Cursor
{
    MAYBE_FORWARD_AS_CURSOR;
    return m_tree->find_exact(key);
}

auto Core::find(Slice key) -> Cursor
{
    MAYBE_FORWARD_AS_CURSOR;
    return m_tree->find(key);
}

auto Core::first() -> Cursor
{
    MAYBE_FORWARD_AS_CURSOR;
    return m_tree->find_minimum();
}

auto Core::last() -> Cursor
{
    MAYBE_FORWARD_AS_CURSOR;
    return m_tree->find_maximum();
}

#undef MAYBE_FORWARD_AS_CURSOR

auto Core::insert(Slice key, Slice value) -> Status
{
    CALICO_TRY_S(handle_errors());
    if (m_system->has_xact) {
        return m_tree->insert(key, value);
    } else {
        return atomic_insert(key, value);
    }
}

auto Core::erase(Slice key) -> Status
{
    CALICO_TRY_S(handle_errors());
    return erase(m_tree->find_exact(key));
}

auto Core::erase(const Cursor &cursor) -> Status
{
    CALICO_TRY_S(handle_errors());
    if (m_system->has_xact) {
        return m_tree->erase(cursor);
    } else {
        return atomic_erase(cursor);
    }
}

auto Core::atomic_insert(Slice key, Slice value) -> Status
{
    auto xact = transaction();
    auto s = m_tree->insert(key, value);
    if (s.is_ok()) {
        return xact.commit();
    } else {
        CALICO_ERROR_IF(xact.abort());
        return s;
    }
}
auto Core::atomic_erase(const Cursor &cursor) -> Status
{
    auto xact = transaction();
    auto s = m_tree->erase(cursor);
    if (s.is_ok()) {
        return xact.commit();
    } else if (!s.is_not_found()) {
        CALICO_ERROR_IF(xact.abort());
    }
    return s;
}

auto Core::commit() -> Status
{
    // m_log->trace("commit");
    CALICO_ERROR_IF(do_commit());

    auto s = status();
    if (s.is_ok()) {
        // m_log->info("commit {}", m_wal->flushed_lsn().value);
        CALICO_EXPECT_EQ(m_system->commit_lsn, m_wal->flushed_lsn());
    }
    return s;
}

auto Core::do_commit() -> Status
{
    const auto last_commit_lsn = m_system->commit_lsn.load();

    if (!m_system->has_xact)
        return logic_error("transaction has not been started");

    CALICO_TRY_S(handle_errors());
    CALICO_TRY_S(save_state());

    // Write a commit record to the WAL.
    const auto lsn = m_wal->current_lsn();
    WalPayloadIn payload {lsn, m_scratch->get()};
    const auto size = encode_commit_payload(payload.data());
    payload.shrink_to_fit(size);

    m_wal->log(payload);
    m_wal->advance();

    // advance() blocks until it is finished. If an error was encountered, it'll show up in the
    // System object at this point.
    CALICO_TRY_S(status());

    // Make sure every dirty page that hasn't been written back since the last commit is on disk. Note that this will
    // cause all dirty pages to be flushed on the first commit.
    CALICO_TRY_S(m_pager->flush(last_commit_lsn));

    // Clean up obsolete WAL segments.
    m_wal->cleanup(m_pager->recovery_lsn());

    m_images.clear();
    m_system->commit_lsn = lsn;
    m_system->has_xact = false;
    return ok();
}

auto Core::abort() -> Status
{
    // m_log->trace("abort");
    CALICO_ERROR_IF(do_abort());

    auto s = status();
    if (s.is_ok()) {
        // m_log->info("abort {}", m_system->commit_lsn.load().value);
        CALICO_EXPECT_LE(m_system->commit_lsn.load(), m_wal->flushed_lsn());
    }
    return s;
}

auto Core::do_abort() -> Status
{
    if (!m_system->has_xact)
        return logic_error(
            "could not abort: a transaction is not active (start a transaction and try again)");

    m_system->has_xact = false;
    m_images.clear();
    m_wal->advance();

    CALICO_TRY_S(handle_errors());
    CALICO_TRY_S(m_recovery->start_abort());
    CALICO_TRY_S(load_state());
    CALICO_TRY_S(m_recovery->finish_abort());
    return ok();
}

auto Core::close() -> Status
{
    // m_log->trace("close");

    if (m_system->has_xact && !m_system->has_error()) {
        auto s = logic_error("could not close: a transaction is active (finish the transaction and try again)");
        CALICO_WARN(s);
        return s;
    }
    m_wal->flush();

    // We already waited on the WAL to be done writing so this should happen immediately.
    CALICO_ERROR_IF(m_pager->flush({}));

    m_wal.reset();
    m_pager.reset();

    return status();
}

auto Core::ensure_consistency_on_startup() -> Status
{
    CALICO_TRY_S(m_recovery->start_recovery());
    CALICO_TRY_S(load_state());
    CALICO_TRY_S(m_recovery->finish_recovery());
    return ok();
}

auto Core::transaction() -> Transaction
{
    // m_log->trace("transaction");
    CALICO_EXPECT_FALSE(m_system->has_xact);
    m_system->has_xact = true;
    return Transaction {*this};
}

auto Core::save_state() -> Status
{
    // m_log->trace("save_state");
    auto root = m_pager->acquire(Id::root(), true);
    if (!root.has_value()) return root.error();

    auto state = read_header(*root);
    m_pager->save_state(state);
    m_tree->save_state(state);
    state.header_crc = compute_header_crc(state);
    write_header(*root, state);

    return m_pager->release(std::move(*root));
}

auto Core::load_state() -> Status
{
    // m_log->trace("load_state");
    auto root = m_pager->acquire(Id::root(), false);
    if (!root.has_value()) return root.error();

    auto state = read_header(*root);
    if (state.header_crc != compute_header_crc(state))
        return corruption(
            "cannot load database state: file header is corrupted (header CRC is {} but should be {})",
            state.header_crc, compute_header_crc(state));

    const auto before_count = m_pager->page_count();

    m_pager->load_state(state);
    m_tree->load_state(state);

    auto s = m_pager->release(std::move(*root));
    if (s.is_ok() && m_pager->page_count() < before_count) {
        const auto after_size = m_pager->page_count() * m_pager->page_size();
        return m_store->resize_file(m_prefix + DATA_FILENAME, after_size);
    }
    return s;
}

auto setup(const std::string &prefix, Storage &store, const Options &options) -> tl::expected<InitialState, Status>
{
    static constexpr Size MINIMUM_BUFFER_COUNT {16};
    const auto MSG = fmt::format("cannot initialize database at \"{}\"", prefix);

    FileHeader header {};
    Bytes bytes {reinterpret_cast<Byte*>(&header), sizeof(FileHeader)};

    if (options.page_size < MINIMUM_PAGE_SIZE)
        return tl::make_unexpected(invalid_argument(
            "{}: page size of {} is too small (must be greater than or equal to {})", MSG, options.page_size, MINIMUM_PAGE_SIZE));

    if (options.page_size > MAXIMUM_PAGE_SIZE)
        return tl::make_unexpected(invalid_argument(
            "{}: page size of {} is too large (must be less than or equal to {})", MSG, options.page_size, MAXIMUM_PAGE_SIZE));

    if (!is_power_of_two(options.page_size))
        return tl::make_unexpected(invalid_argument(
            "{}: page size of {} is invalid (must be a power of 2)", MSG, options.page_size));

    if (options.page_cache_size < options.page_size * MINIMUM_BUFFER_COUNT)
        return tl::make_unexpected(invalid_argument(
            "{}: page cache of size {} B is too small (minimum size is {} B)", MSG, options.page_cache_size, options.page_size * MINIMUM_BUFFER_COUNT));

    if (options.wal_buffer_size < wal_scratch_size(options.page_size) * MINIMUM_BUFFER_COUNT)
        return tl::make_unexpected(invalid_argument(
            "{}: WAL write buffer of size {} B is too small (minimum size is {} B)", MSG, options.wal_buffer_size, wal_scratch_size(options.page_size) * MINIMUM_BUFFER_COUNT));

    if (options.max_log_size < MINIMUM_LOG_MAX_SIZE)
        return tl::make_unexpected(invalid_argument(
            "{}: log file maximum size of {} B is too small (minimum size is {} B)", MSG, options.max_log_size, MINIMUM_LOG_MAX_SIZE));

    if (options.max_log_size > MAXIMUM_LOG_MAX_SIZE)
        return tl::make_unexpected(invalid_argument(
            "{}: log file maximum size of {} B is too large (maximum size is {} B)", MSG, options.max_log_size, MAXIMUM_LOG_MAX_SIZE));

    if (options.max_log_files < MINIMUM_LOG_MAX_FILES)
        return tl::make_unexpected(invalid_argument(
            "{}: log maximum file count of {} is too small (minimum count is {})", MSG, options.max_log_files, MINIMUM_LOG_MAX_FILES));

    if (options.max_log_files > MAXIMUM_LOG_MAX_FILES)
        return tl::make_unexpected(invalid_argument(
            "{}: log maximum file count of {} is too large (maximum count is {})", MSG, options.max_log_files, MAXIMUM_LOG_MAX_FILES));

    {
        // May have already been created by spdlog.
        auto s = store.create_directory(prefix);
        if (!s.is_ok() && !s.is_logic_error())
            return tl::make_unexpected(s);
    }

    if (!options.wal_prefix.is_empty()) {
        auto s = store.create_directory(options.wal_prefix.to_string());
        if (!s.is_ok() && !s.is_logic_error())
            return tl::make_unexpected(s);
    }

    const auto path = prefix + DATA_FILENAME;
    std::unique_ptr<RandomReader> reader;
    RandomReader *reader_temp {};
    bool exists {};

    if (auto s = store.open_random_reader(path, &reader_temp); s.is_ok()) {
        reader.reset(reader_temp);
        Size file_size {};
        s = store.file_size(path, file_size);
        if (!s.is_ok()) return tl::make_unexpected(s);

        if (file_size < sizeof(FileHeader))
            return tl::make_unexpected(corruption(
                "{}: database is too small to read the file header (file header is {} bytes)", MSG, sizeof(FileHeader)));

        s = read_exact(*reader, bytes, 0);
        if (!s.is_ok()) return tl::make_unexpected(s);

        if (file_size % decode_page_size(header.page_size))
            return tl::make_unexpected(corruption(
                "{}: database size of {} B is invalid (database must contain an integral number of pages)", MSG, file_size));

        if (header.magic_code != MAGIC_CODE)
            return tl::make_unexpected(invalid_argument(
                "{}: path does not point to a Calico DB database (magic code is {} but should be {})", MSG, header.magic_code, MAGIC_CODE));

        if (header.header_crc != compute_header_crc(header))
            return tl::make_unexpected(corruption(
                "{}: header has an inconsistent CRC (CRC is {} but should be {})", MSG, header.header_crc, compute_header_crc(header)));

        exists = true;

    } else if (s.is_not_found()) {
        header.magic_code = MAGIC_CODE;
        header.page_size = encode_page_size(options.page_size);
        header.recovery_lsn = Id::root().value;
        header.header_crc = compute_header_crc(header);

    } else {
        return tl::make_unexpected(s);
    }

    const auto page_size = decode_page_size(header.page_size);

    if (page_size < MINIMUM_PAGE_SIZE)
        return tl::make_unexpected(corruption(
            "{}: header page size {} is too small (must be greater than or equal to {})", MSG, page_size));

    if (!is_power_of_two(page_size))
        return tl::make_unexpected(corruption(
            "{}: header page size {} is invalid (must either be 0 or a power of 2)", MSG, page_size));

    return InitialState {header, !exists};
}

} // namespace Calico
