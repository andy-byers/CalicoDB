
#include "database_impl.h"
#include "calico/calico.h"
#include "calico/storage.h"
#include "calico/transaction.h"
#include "pager/pager.h"
#include "recovery.h"
#include "storage/helpers.h"
#include "storage/posix_storage.h"
#include "tree/bplus_tree.h"
#include "tree/cursor_internal.h"
#include "tree/header.h"
#include "utils/crc.h"
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

auto DatabaseImpl::open(const Slice &path, const Options &options) -> Status
{
    auto sanitized = sanitize_options(options);

    m_prefix = path.to_string();
    if (m_prefix.back() != '/')
        m_prefix += '/';

    system = std::make_unique<System>(m_prefix, sanitized);
    m_log = system->create_log("core");

    m_log->info("starting CalicoDB v{}.{}.{} at \"{}\"", CALICO_VERSION_MAJOR,
                CALICO_VERSION_MINOR, CALICO_VERSION_PATCH, path.to_string());
    m_log->info("tree is located at \"{}data\"", m_prefix);
    if (sanitized.wal_prefix.is_empty()) {
        m_log->info("wal prefix is \"{}{}\"", m_prefix, WAL_PREFIX);
    } else {
        m_log->info("wal prefix is \"{}\"", sanitized.wal_prefix.to_string());
    }

    // Any error during initialization is fatal.
    CALICO_ERROR_IF(do_open(sanitized));
    return status();
}

auto DatabaseImpl::do_open(Options sanitized) -> Status
{
    if (sanitized.log_level != LogLevel::OFF) {
        switch (sanitized.log_target) {
            case LogTarget::FILE:
                m_log->info("log is located at \"{}{}\"", m_prefix, LOG_FILENAME);
                break;
            case LogTarget::STDOUT:
            case LogTarget::STDOUT_COLOR:
                m_log->info("logging to stdout");
                break;
            case LogTarget::STDERR:
            case LogTarget::STDERR_COLOR:
                m_log->info("logging to stderr");
        }
    }

    m_store = sanitized.storage;
    if (m_store == nullptr) {
        m_store = new PosixStorage;
        m_owns_store = true;
    }

    auto initial = setup(m_prefix, *m_store, sanitized);
    if (!initial.has_value()) return initial.error();
    auto [state, is_new] = *initial;
    if (!is_new) sanitized.page_size = state.page_size;

    maximum_key_size = compute_max_local(sanitized.page_size);

    {
        const auto scratch_size = wal_scratch_size(sanitized.page_size);
        const auto buffer_count = sanitized.wal_buffer_size / scratch_size;

        m_scratch = std::make_unique<LogScratchManager>(
            scratch_size,
            buffer_count);

        // The WAL segments may be stored elsewhere.
        auto wal_prefix = sanitized.wal_prefix.is_empty()
            ? m_prefix : sanitized.wal_prefix.to_string();
        if (wal_prefix.back() != '/') {
            wal_prefix += '/';
        }

        auto r = BasicWriteAheadLog::open({
            wal_prefix,
            m_store,
            system.get(),
            sanitized.page_size,
            buffer_count * 32,
            buffer_count,
        });
        if (!r.has_value()) {
            return r.error();
        }
        wal = std::move(*r);
    }

    {
        auto r = Pager::open({
            m_prefix,
            m_store,
            m_scratch.get(),
            wal.get(),
            system.get(),
            sanitized.page_cache_size / sanitized.page_size,
            sanitized.page_size,
        });
        if (!r.has_value()) {
            return r.error();
        }
        pager = std::move(*r);
        pager->load_state(state);
    }

    tree = std::make_unique<BPlusTree>(*pager);
    tree->load_state(state);
    m_recovery = std::make_unique<Recovery>(*pager, *wal, *system);

    auto s = ok();
    if (is_new) {
        m_log->info("setting up a new database");
        auto page = pager->allocate();
        if (!page.has_value()) {
            return page.error();
        }
        pager->upgrade(*page);
        Node root {std::move(*page), nullptr};
        CALICO_EXPECT_EQ(pager->page_count(), 1);

        state.page_count = 1;
        state.header_crc = state.compute_crc();
        state.write(root.page);
        pager->release(std::move(root).take());

        // This is safe right now because the WAL has not been started. If successful, we will have the root page
        // set up and saved to the database file.
        CALICO_TRY_S(pager->flush({}));

    } else {
        m_log->info("ensuring consistency of an existing database");
        // This should be a no-op if the database closed normally last time.
        CALICO_TRY_S(ensure_consistency_on_startup());
    }
    m_log->info("pager recovery lsn is {}", pager->recovery_lsn().value);
    m_log->info("wal flushed lsn is {}", wal->flushed_lsn().value);
    m_log->info("commit lsn is {}", system->commit_lsn.load().value);

    s = wal->start_workers();
    if (!s.is_ok()) {
        m_log->info("failed to initialize database");
    } else {
        m_log->info("successfully initialized database");
    }
    return s;
}

DatabaseImpl::~DatabaseImpl()
{
    wal.reset();

    if (m_owns_store) {
        delete m_store;
    }
}

auto DatabaseImpl::destroy() -> Status
{
    auto s = ok();
    wal.reset();

    std::vector<std::string> children;
    s = m_store->get_children(m_prefix, children);

    if (s.is_ok()) {
        for (const auto &name: children) {
            CALICO_WARN_IF(m_store->remove_file(name));
        }

        // Remove the directory, which should be empty now.
        CALICO_ERROR_IF(m_store->remove_directory(m_prefix));
    } else {
        CALICO_ERROR(s);
    }
    return s;
}

auto DatabaseImpl::status() const -> Status
{
    return system->has_error() ? system->original_error().status : ok();
}

auto DatabaseImpl::path() const -> std::string
{
    return m_prefix;
}

auto DatabaseImpl::statistics() -> Statistics
{
    return Statistics {*this};
}

auto DatabaseImpl::check_key(const Slice &key, const char *message) const -> Status
{
    if (key.is_empty()) {
        auto s = invalid_argument("{}: key is empty (use a nonempty key)", message);
        CALICO_WARN(s);
        return s;
    }
    if (key.size() > maximum_key_size) {
        auto s = invalid_argument("{}: key of length {} B is too long", message, key.size(), maximum_key_size);
        CALICO_WARN(s);
        return s;
    }
    return ok();
}

#define MAYBE_FORWARD_AS_STATUS \
    do { \
        if (auto calico_s = status(); !calico_s.is_ok()) { \
            return calico_s; \
        } \
    } while (0)

#define MAYBE_FORWARD_AS_CURSOR \
    do { \
        if (auto calico_s = status(); !calico_s.is_ok()) { \
            auto calico_c = CursorInternal::make_cursor(*tree); \
            CursorInternal::invalidate(calico_c, calico_s); \
            return calico_c; \
        } \
    } while (0)

auto DatabaseImpl::get(const Slice &key, std::string &value) const -> Status
{
    if (auto slot = tree->search(key)) {
        auto [node, index, exact] = std::move(*slot);
        if (!exact) {
            return not_found("not found");
        }
        if (auto result = tree->collect(std::move(node), index)) {
            value = std::move(*result);
            return ok();
        } else {
            return result.error();
        }
    } else {
        return slot.error();
    }
}

auto DatabaseImpl::cursor() const -> Cursor
{
    MAYBE_FORWARD_AS_CURSOR;
    return CursorInternal::make_cursor(*tree);
}

#undef MAYBE_FORWARD_AS_CURSOR

auto DatabaseImpl::put(const Slice &key, const Slice &value) -> Status
{
    CALICO_TRY_S(status());
    CALICO_TRY_S(check_key(key, "insert"));
    bytes_written += key.size() + value.size();
    if (system->has_xact) {
        if (const auto r = tree->insert(key, value)) {
            record_count += *r;
            return ok();
        } else {
            return r.error();
        }
    } else {
        return atomic_insert(key, value);
    }
}

auto DatabaseImpl::erase(const Slice &key) -> Status
{
    CALICO_TRY_S(status());
    CALICO_TRY_S(check_key(key, "erase"));
    if (system->has_xact) {
        if (const auto r = tree->erase(key)) {
            return ok();
        } else {
            return r.error();
        }
    } else {
        return atomic_erase(key);
    }
}

auto DatabaseImpl::atomic_insert(const Slice &key, const Slice &value) -> Status
{
    CALICO_TRY_S(check_key(key, "insert"));
    auto xact = start();

    if (const auto r = tree->insert(key, value)) {
        record_count += *r;
        return xact.commit();
    } else {
        CALICO_ERROR_IF(xact.abort());
        return r.error();
    }
}

auto DatabaseImpl::atomic_erase(const Slice &key) -> Status
{
    CALICO_TRY_S(check_key(key, "erase"));
    auto xact = start();
    if (const auto r = tree->erase(key)) {
        record_count--;
        return xact.commit();
    } else {
        if (!r.error().is_not_found()) {
            CALICO_ERROR_IF(xact.abort());
        }
        return r.error();
    }
}

auto DatabaseImpl::commit() -> Status
{
    CALICO_ERROR_IF(do_commit());
    return status();
}

auto DatabaseImpl::do_commit() -> Status
{
    const auto last_commit_lsn = system->commit_lsn.load();

    if (!system->has_xact)
        return logic_error("transaction has not been started");

    CALICO_TRY_S(status());
    CALICO_TRY_S(save_state());

    // Write a commit record to the WAL.
    const auto lsn = wal->current_lsn();
    wal->log(encode_commit_payload(lsn, *m_scratch->get()));
    wal->advance();

    // advance() blocks until it is finished. If an error was encountered, it'll show up in the
    // System object at this point.
    CALICO_TRY_S(status());

    const auto checkpoint = pager->recovery_lsn().value;
    if (static constexpr Size CUTOFF {1'024}; CUTOFF < lsn.value - checkpoint) {
        CALICO_TRY_S(pager->flush(last_commit_lsn));
        wal->cleanup(pager->recovery_lsn());
    }

    system->commit_lsn = lsn;
    system->has_xact = false;
    return ok();
}

auto DatabaseImpl::abort() -> Status
{
    // m_log->trace("abort");
    CALICO_ERROR_IF(do_abort());

    auto s = status();
    if (s.is_ok()) {
        // m_log->info("abort {}", system->commit_lsn.load().value);
        CALICO_EXPECT_LE(system->commit_lsn.load(), wal->flushed_lsn());
    }
    return s;
}

auto DatabaseImpl::do_abort() -> Status
{
    if (!system->has_xact)
        return logic_error(
            "could not abort: a transaction is not active (start a transaction and try again)");

    system->has_xact = false;
    wal->advance();

    CALICO_TRY_S(status());
    CALICO_TRY_S(m_recovery->start_abort());
    CALICO_TRY_S(load_state());
    CALICO_TRY_S(m_recovery->finish_abort());
    return ok();
}

auto DatabaseImpl::close() -> Status
{
    // m_log->trace("close");

    if (system->has_xact && !system->has_error()) {
        auto s = logic_error("could not close: a transaction is active (finish the transaction and try again)");
        CALICO_WARN(s);
        return s;
    }
    if (wal && pager) {
        wal->flush();

        // We already waited on the WAL to be done writing so this should happen immediately.
        CALICO_ERROR_IF(pager->flush({}));
    }
    wal.reset();
    pager.reset();
    return status();
}

auto DatabaseImpl::ensure_consistency_on_startup() -> Status
{
    CALICO_TRY_S(m_recovery->start_recovery());
    CALICO_TRY_S(load_state());
    CALICO_TRY_S(m_recovery->finish_recovery());
    return ok();
}

auto DatabaseImpl::start() -> Transaction
{
    CALICO_EXPECT_FALSE(system->has_xact);
    system->has_xact = true;
    return Transaction {*this};
}

auto DatabaseImpl::save_state() -> Status
{
    auto root = pager->acquire(Id::root());
    if (!root.has_value()) {
        return root.error();
    }
    pager->upgrade(*root);
    FileHeader header {*root};
    pager->save_state(header);
    tree->save_state(header);
    header.header_crc = header.compute_crc();
    header.write(*root);

    pager->release(std::move(*root));
    return ok();
}

auto DatabaseImpl::load_state() -> Status
{
    auto root = pager->acquire(Id::root());
    if (!root.has_value()) {
        return root.error();
    }

    FileHeader header {*root};
    if (header.header_crc != header.compute_crc()) {
        return corruption(
            "cannot load database state: file header is corrupted (header CRC is {} but should be {})",
            header.header_crc, header.compute_crc());
    }

    const auto before_count = pager->page_count();

    pager->load_state(header);
    tree->load_state(header);

    pager->release(std::move(*root));
    if (pager->page_count() < before_count) {
        const auto after_size = pager->page_count() * pager->page_size();
        return m_store->resize_file(m_prefix + "data", after_size);
    }
    return ok();
}

auto setup(const std::string &prefix, Storage &store, const Options &options) -> tl::expected<InitialState, Status>
{
    static constexpr Size MINIMUM_BUFFER_COUNT {16};
    FileHeader header;

    if (options.page_size < MINIMUM_PAGE_SIZE) {
        return tl::make_unexpected(invalid_argument(
            "page size of {} is too small (must be greater than or equal to {})", options.page_size, MINIMUM_PAGE_SIZE));
    }

    if (options.page_size > MAXIMUM_PAGE_SIZE) {
        return tl::make_unexpected(invalid_argument(
            "page size of {} is too large (must be less than or equal to {})", options.page_size, MAXIMUM_PAGE_SIZE));
    }

    if (!is_power_of_two(options.page_size)) {
        return tl::make_unexpected(invalid_argument(
            "page size of {} is invalid (must be a power of 2)", options.page_size));
    }

    if (options.page_cache_size < options.page_size * MINIMUM_BUFFER_COUNT) {
        return tl::make_unexpected(invalid_argument(
            "page cache of size {} is too small (minimum size is {})", options.page_cache_size, options.page_size * MINIMUM_BUFFER_COUNT));
    }

    if (options.wal_buffer_size < wal_scratch_size(options.page_size) * MINIMUM_BUFFER_COUNT) {
        return tl::make_unexpected(invalid_argument(
            "WAL write buffer of size {} is too small (minimum size is {})", options.wal_buffer_size, wal_scratch_size(options.page_size) * MINIMUM_BUFFER_COUNT));
    }

    if (options.max_log_size < MINIMUM_LOG_MAX_SIZE) {
        return tl::make_unexpected(invalid_argument(
            "log file maximum size of {} is too small (minimum size is {})", options.max_log_size, MINIMUM_LOG_MAX_SIZE));
    }

    if (options.max_log_size > MAXIMUM_LOG_MAX_SIZE) {
        return tl::make_unexpected(invalid_argument(
            "log file maximum size of {} is too large (maximum size is {})", options.max_log_size, MAXIMUM_LOG_MAX_SIZE));
    }

    if (options.max_log_files < MINIMUM_LOG_MAX_FILES) {
        return tl::make_unexpected(invalid_argument(
            "log maximum file count of {} is too small (minimum count is {})", options.max_log_files, MINIMUM_LOG_MAX_FILES));
    }

    if (options.max_log_files > MAXIMUM_LOG_MAX_FILES) {
        return tl::make_unexpected(invalid_argument(
            "log maximum file count of {} is too large (maximum count is {})", options.max_log_files, MAXIMUM_LOG_MAX_FILES));
    }

    {
        // May have already been created by spdlog.
        auto s = store.create_directory(prefix);
        if (!s.is_ok() && !s.is_logic_error()) {
            return tl::make_unexpected(s);
        }
    }

    if (!options.wal_prefix.is_empty()) {
        auto s = store.create_directory(options.wal_prefix.to_string());
        if (!s.is_ok() && !s.is_logic_error()) {
            return tl::make_unexpected(s);
        }
    }

    const auto path = prefix + "data";
    std::unique_ptr<RandomReader> reader;
    RandomReader *reader_temp {};
    bool exists {};

    if (auto s = store.open_random_reader(path, &reader_temp); s.is_ok()) {
        reader.reset(reader_temp);
        Size file_size {};
        s = store.file_size(path, file_size);
        if (!s.is_ok()) {
            return tl::make_unexpected(s);
        }

        if (file_size < FileHeader::SIZE) {
            return tl::make_unexpected(corruption(
                "database is too small to read the file header (file header is {} bytes)", FileHeader::SIZE));
        }

        Byte buffer[FileHeader::SIZE];
        Span span {buffer, sizeof(buffer)};
        s = read_exact(*reader, span, 0);
        header = FileHeader {Page {Id::root(), span, false}};

        if (!s.is_ok()) {
            return tl::make_unexpected(s);
        }

        if (file_size % header.page_size) {
            return tl::make_unexpected(corruption(
                "database size of {} is invalid (database must contain an integral number of pages)", file_size));
        }

        if (header.magic_code != FileHeader::MAGIC_CODE) {
            return tl::make_unexpected(invalid_argument(
                "path does not point to a Calico DB database (magic code is {} but should be {})", header.magic_code, FileHeader::MAGIC_CODE));
        }
        if (header.header_crc != header.compute_crc()) {
            return tl::make_unexpected(corruption(
                "header has an inconsistent CRC (CRC is {} but should be {})", header.header_crc, header.compute_crc()));
        }

        exists = true;

    } else if (s.is_not_found()) {
        header.page_size = static_cast<std::uint16_t>(options.page_size);
        header.recovery_lsn = Id::root();
        header.header_crc = header.compute_crc();

    } else {
        return tl::make_unexpected(s);
    }

    if (header.page_size < MINIMUM_PAGE_SIZE) {
        return tl::make_unexpected(corruption(
            "header page size {} is too small (must be greater than or equal to {})", header.page_size));
    }
    if (!is_power_of_two(header.page_size)) {
        return tl::make_unexpected(corruption(
            "header page size {} is invalid (must either be 0 or a power of 2)", header.page_size));
    }
    return InitialState {header, !exists};
}

} // namespace Calico