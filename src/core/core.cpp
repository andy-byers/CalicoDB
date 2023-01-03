
#include "core.h"
#include "recovery.h"
#include "calico/calico.h"
#include "calico/storage.h"
#include "calico/transaction.h"
#include "pager/basic_pager.h"
#include "pager/pager.h"
#include "storage/helpers.h"
#include "storage/posix_storage.h"
#include "tree/bplus_tree.h"
#include "tree/cursor_internal.h"
#include "utils/crc.h"
#include "utils/header.h"
#include "utils/layout.h"
#include "utils/system.h"
#include "wal/basic_wal.h"
#include "wal/disabled_wal.h"

namespace Calico {

[[nodiscard]] static auto sanitize_options(const Options &options) -> Options
{
    return options; // TODO: NOOP for now.
}

auto Info::record_count() const -> Size
{
    return m_core->tree().record_count();
}

auto Info::page_count() const -> Size
{
    return m_core->pager().page_count();
}

auto Info::page_size() const -> Size
{
    return m_core->pager().page_size();
}

auto Info::maximum_key_size() const -> Size
{
    return get_max_local(page_size());
}

auto Info::cache_hit_ratio() const -> double
{
    return m_core->pager().hit_ratio();
}

auto Core::open(const std::string &path, const Options &options) -> Status
{
    auto sanitized = sanitize_options(options);

    m_prefix = path + (path.back() == '/' ? "" : "/");
    m_system = std::make_unique<System>(m_prefix, sanitized.log_level, sanitized.log_target);
    m_log = m_system->create_log("core");

    const auto version_name = fmt::format("v{}.{}.{}", CALICO_VERSION_MAJOR, CALICO_VERSION_MINOR, CALICO_VERSION_PATCH);
    m_log->info("starting CalicoDB {} at \"{}\"", version_name, path);
    m_log->info("tree is located at \"{}{}\"", m_prefix, DATA_FILENAME);
    m_log->info("log is located at \"{}{}\"", m_prefix, LOG_FILENAME);
    if (sanitized.wal_limit != DISABLE_WAL)
        m_log->info("WAL is located at \"{}\"", sanitized.wal_path.to_string());

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

    if (sanitized.wal_limit != DISABLE_WAL) {
        m_scratch = std::make_unique<LogScratchManager>(wal_scratch_size(sanitized.page_size));

        // The WAL segments may be stored elsewhere.
        const auto wal_prefix = sanitized.wal_path.is_empty()
            ? m_prefix : sanitized.wal_path.to_string() + "/";

        auto r = BasicWriteAheadLog::open({
            wal_prefix,
            m_store,
            m_system.get(),
            sanitized.page_size,
            sanitized.wal_limit,
        });
        if (!r.has_value())
            return r.error();
        m_wal = std::move(*r);
    } else {
        m_wal = std::make_unique<DisabledWriteAheadLog>();
    }

    {
        auto r = BasicPager::open({
            m_prefix,
            m_store,
            m_scratch.get(),
            &m_images,
            m_wal.get(),
            m_system.get(),
            sanitized.cache_size,
            sanitized.page_size,
        });
        if (!r.has_value())
            return r.error();
        m_pager = std::move(*r);
        m_pager->load_state(state);
    }

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
            CALICO_ERROR(root.error());
        CALICO_EXPECT_EQ(m_pager->page_count(), 1);

        state.page_count = 1;
        state.header_crc = compute_header_crc(state);
        write_header(root->page(), state);
        CALICO_TRY_S(m_pager->release(root->take()));

        // This is safe right now because the WAL has not been started. If successful, we will have the root page
        // set up and saved to the database file.
        s = m_pager->flush({});

        if (s.is_ok() && m_wal->is_enabled())
            s = m_wal->start_workers();

    } else if (m_wal->is_enabled()) {
        // This should be a no-op if the database closed normally last time.
        s = ensure_consistency_on_startup();
    }

    return s;
}

Core::~Core()
{
    m_wal.reset();

    if (m_owns_store)
        delete m_store;
}

auto Core::destroy() -> Status
{
    m_log->trace("destroy");
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

auto Core::info() -> Info
{
    return Info {*this};
}

auto Core::handle_errors() -> Status
{
    if (m_system->has_error())
        return m_system->original_error().status;
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

auto Core::find_exact(BytesView key) -> Cursor
{
    MAYBE_FORWARD_AS_CURSOR;
    return m_tree->find_exact(key);
}

auto Core::find(BytesView key) -> Cursor
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

auto Core::insert(BytesView key, BytesView value) -> Status
{
    CALICO_TRY_S(handle_errors());
    if (m_system->has_xact) {
        return m_tree->insert(key, value);
    } else {
        return atomic_insert(key, value);
    }
}

auto Core::erase(BytesView key) -> Status
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

auto Core::atomic_insert(BytesView key, BytesView value) -> Status
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
    m_log->trace("commit");
    CALICO_ERROR_IF(do_commit());

    auto s = status();
    if (s.is_ok()) {
        m_log->info("commit {}", m_wal->flushed_lsn().value);
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

    CALICO_TRY_S(m_wal->log(payload));
    CALICO_TRY_S(m_wal->advance());

    // Make sure every dirty page that hasn't been written back since the last commit is on disk.
    CALICO_TRY_S(m_pager->flush(last_commit_lsn));

    // Clean up obsolete WAL segments.
    CALICO_TRY_S(m_wal->remove_before(m_pager->recovery_lsn()));

    m_images.clear();
    m_system->commit_lsn = lsn;
    m_system->has_xact = false;
    return ok();
}

auto Core::abort() -> Status
{
    m_log->trace("abort");
    CALICO_ERROR_IF(do_abort());

    auto s = status();
    if (s.is_ok()) {
        m_log->info("abort {}", m_system->commit_lsn.load().value);
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
    CALICO_TRY_S(handle_errors());
    CALICO_TRY_S(m_recovery->start_abort());
    CALICO_TRY_S(load_state());
    CALICO_TRY_S(m_recovery->finish_abort());
    return ok();
}

auto Core::close() -> Status
{
    m_log->trace("close");

    if (m_system->has_xact && !m_system->has_error()) {
        auto s = logic_error("could not close: a transaction is active (finish the transaction and try again)");
        CALICO_WARN(s);
        return s;
    }
    if (m_wal->is_working())
        CALICO_WARN_IF(m_wal->stop_workers());

    // We already waited on the WAL to be done writing so this should happen immediately.
    CALICO_ERROR_IF(m_pager->flush({}));

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
    m_log->trace("transaction");
    CALICO_EXPECT_FALSE(m_system->has_xact);
    m_system->has_xact = true;
    return Transaction {*this};
}

auto Core::save_state() -> Status
{
    m_log->trace("save_state");
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
    m_log->trace("load_state");
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
    const auto MSG = fmt::format("cannot initialize database at \"{}\"", prefix);

    FileHeader header {};
    Bytes bytes {reinterpret_cast<Byte*>(&header), sizeof(FileHeader)};

    if (options.cache_size < MINIMUM_CACHE_SIZE)
        return tl::make_unexpected(invalid_argument(
            "{}: frame count of {} is too small (minimum frame count is {})", MSG, options.cache_size, MINIMUM_CACHE_SIZE));

    if (options.cache_size > MAXIMUM_CACHE_SIZE)
        return tl::make_unexpected(invalid_argument(
            "{}: frame count of {} is too large (maximum frame count is {})", MSG, options.cache_size, MAXIMUM_CACHE_SIZE));

    if (options.page_size < MINIMUM_PAGE_SIZE)
        return tl::make_unexpected(invalid_argument(
            "{}: page size of {} is too small (must be greater than or equal to {})", MSG, options.page_size, MINIMUM_PAGE_SIZE));

    if (options.page_size > MAXIMUM_PAGE_SIZE)
        return tl::make_unexpected(invalid_argument(
            "{}: page size of {} is too large (must be less than or equal to {})", MSG, options.page_size, MAXIMUM_PAGE_SIZE));

    if (!is_power_of_two(options.page_size))
        return tl::make_unexpected(invalid_argument(
            "{}: page size of {} is invalid (must be a power of 2)", MSG, options.page_size));

    if (options.wal_limit != DISABLE_WAL && options.wal_limit < MINIMUM_WAL_LIMIT)
        return tl::make_unexpected(invalid_argument(
            "{}: WAL segment limit of {} is too small (must be greater than or equal to {} blocks)", MSG, options.wal_limit, MINIMUM_WAL_LIMIT));

    if (options.wal_limit > MAXIMUM_WAL_LIMIT)
        return tl::make_unexpected(invalid_argument(
            "{}: WAL segment limit of {} is too large (must be less than or equal to {} blocks)", MSG, options.wal_limit, MAXIMUM_WAL_LIMIT));

    {
        // May have already been created by spdlog.
        auto s = store.create_directory(prefix);
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
