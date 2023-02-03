
#include "database_impl.h"
#include "calico/calico.h"
#include "calico/storage.h"
#include "calico/transaction.h"
#include "pager/pager.h"
#include "recovery.h"
#include "storage/helpers.h"
#include "storage/posix_storage.h"
#include "tree/cursor_internal.h"
#include "tree/header.h"
#include "tree/tree.h"
#include "utils/system.h"
#include "wal/wal.h"
#include "wal/cleanup.h"
#include "wal/writer.h"

namespace Calico {

[[nodiscard]]
static auto sanitize_options(const Options &options) -> Options
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

auto Database::open(const Slice &path, const Options &options, Database **db) -> Status
{
    auto *ptr = new(std::nothrow) DatabaseImpl;
    if (ptr == nullptr) {
        return system_error("cannot allocate database object: out of memory");
    }
    
    auto s = ptr->open(path, options);
    if (!s.is_ok()) {
        delete ptr;
	return s;
    }

    *db = ptr;
    return ok();
}

auto DatabaseImpl::open(const Slice &path, const Options &options) -> Status
{
    auto sanitized = sanitize_options(options);

    m_prefix = path.to_string();
    if (m_prefix.back() != '/') {
        m_prefix += '/';
    }

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
    return do_open(sanitized);
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

    m_storage = sanitized.storage;
    if (m_storage == nullptr) {
        m_storage = new PosixStorage;
        m_owns_storage = true;
    }

    const auto initial = setup(m_prefix, *m_storage, sanitized);
    if (!initial.has_value()) {
        return initial.error();
    }
    auto [state, is_new] = *initial;
    if (!is_new) {
        sanitized.page_size = state.page_size;
    }

    maximum_key_size = compute_max_local(sanitized.page_size);

    {
        const auto scratch_size = wal_scratch_size(sanitized.page_size);
        const auto buffer_count = sanitized.wal_buffer_size / scratch_size;

        m_scratch = std::make_unique<LogScratchManager>(
            scratch_size, buffer_count);

        // The WAL segments may be stored elsewhere.
        auto wal_prefix = sanitized.wal_prefix.is_empty()
            ? m_prefix : sanitized.wal_prefix.to_string();
        if (wal_prefix.back() != '/') {
            wal_prefix += '/';
        }

        auto r = WriteAheadLog::open({
            wal_prefix,
            m_storage,
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
            m_storage,
            m_scratch.get(),
            wal.get(),
            system.get(),
            &m_status,
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
        CALICO_TRY_S(wal->start_workers());
        auto xact = start();

        auto root = tree->setup();
        if (!root.has_value()) {
            return root.error();
        }
        CALICO_EXPECT_EQ(pager->page_count(), 1);

        state.page_count = 1;
        state.write(root->page);
        state.header_crc = state.compute_crc();
        state.write(root->page);
        pager->release(std::move(*root).take());

        CALICO_TRY_S(xact.commit());
        CALICO_TRY_S(wal->flush());
        CALICO_TRY_S(pager->flush({}));

    } else {
        m_log->info("ensuring consistency of an existing database");
        // This should be a no-op if the database closed normally last time.
        CALICO_TRY_S(ensure_consistency_on_startup());
        CALICO_TRY_S(wal->start_workers());
    }
    m_log->info("pager recovery lsn is {}", pager->recovery_lsn().value);
    m_log->info("wal flushed lsn is {}", wal->flushed_lsn().value);
    m_log->info("commit lsn is {}", system->commit_lsn.value);
    return status();
}

auto DatabaseImpl::~DatabaseImpl()
{
    (void)close();
}

auto DatabaseImpl::destroy(const std::string &path, const Options &options) -> Status
{
    bool owns_storage {};
    Storage *storage;

    if (options.storage) {
        storage = options.storage;
    } else {
        storage = new PosixStorage;
        owns_storage = true;
    }

    std::vector<std::string> children;
    if (auto s = storage->get_children(path, children); s.is_ok()) {
        for (const auto &name: children) {
            s = storage->remove_file(name);
            if (!s.is_ok()) {

            }
        }
    } else {

    }
    if (!options.wal_prefix.is_empty()) {
        children.clear();

        if (auto s = storage->get_children(options.wal_prefix.to_string(), children); s.is_ok()) {
            for (const auto &name: children) {
                if (name.find("wal-") != std::string::npos) {
                    s = storage->remove_file(name);
                    if (!s.is_ok()) {

                    }
                }
            }
        } else {

        }
    }
    auto s = storage->remove_directory(path);
    if (!s.is_ok()) {

    }
    if (owns_storage) {
        delete storage;
    }
    return s;
}

auto DatabaseImpl::status() const -> Status
{
    if (auto s = wal->status(); !s.is_ok()) {
        m_status = std::move(s);
    }
    return m_status;
}

auto DatabaseImpl::check_key(const Slice &key, const char *message) const -> Status
{
    if (key.is_empty()) {
        auto s = invalid_argument("{}: key is empty (use a nonempty key)", message);
        CALICO_WARN(s);
        return s;
    }
    if (key.size() > maximum_key_size) {
        auto s = invalid_argument("{}: key of length {} is too long", message, key.size(), maximum_key_size);
        CALICO_WARN(s);
        return s;
    }
    return ok();
}

auto DatabaseImpl::get(const Slice &key, std::string &value) const -> Status
{
    if (auto slot = tree->search(key)) {
        auto [node, index, exact] = std::move(*slot);

        if (!exact) {
            pager->release(std::move(node.page));
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

auto DatabaseImpl::new_cursor() const -> Cursor *
{
    auto *cursor = CursorInternal::make_cursor(*tree);
    if (cursor && system->has_error()) {
        CursorInternal::invalidate(cursor, status());
    }
    return cursor;
}

auto DatabaseImpl::put(const Slice &key, const Slice &value) -> Status
{
    CALICO_TRY_S(status());
    CALICO_TRY_S(check_key(key, "insert"));
    bytes_written += key.size() + value.size();
    if (system->has_xact) {
        if (const auto inserted = tree->insert(key, value)) {
            record_count += *inserted;
            return ok();
        } else {
            return inserted.error();
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
        if (const auto erased = tree->erase(key)) {
            record_count--;
            return ok();
        } else {
            return erased.error();
        }
    } else {
        return atomic_erase(key);
    }
}

auto DatabaseImpl::atomic_insert(const Slice &key, const Slice &value) -> Status
{
    CALICO_TRY_S(check_key(key, "insert"));
    auto xact = start();

    if (const auto inserted = tree->insert(key, value)) {
        record_count += *inserted;
        return xact.commit();
    } else {
        CALICO_ERROR_IF(xact.abort());
        return inserted.error();
    }
}

auto DatabaseImpl::atomic_erase(const Slice &key) -> Status
{
    CALICO_TRY_S(check_key(key, "erase"));
    auto xact = start();
    if (const auto erased = tree->erase(key)) {
        record_count--;
        return xact.commit();
    } else {
        if (!erased.error().is_not_found()) {
            CALICO_ERROR_IF(xact.abort());
        }
        return erased.error();
    }
}

auto DatabaseImpl::commit(Size id) -> Status
{
    if (!system->has_xact) {
        return logic_error("transaction has not been started");
    }
    if (id != m_txn_number) {
        return already_completed_error("commit");
    }
    if (auto s = do_commit(); !s.is_ok()) {
        m_status = std::move(s);
    }
    return status();
}

auto DatabaseImpl::do_commit() -> Status
{
    CALICO_EXPECT_TRUE(system->has_xact);
    m_log->info("commit requested at lsn {}", wal->current_lsn().value + 1);

    m_txn_number++;
    system->has_xact = false;
    CALICO_TRY_S(status());
    CALICO_TRY_S(save_state());

    const auto lsn = wal->current_lsn();
    wal->log(encode_commit_payload(lsn, *m_scratch->get()));
    CALICO_TRY_S(wal->advance());

    CALICO_TRY_S(pager->flush(system->commit_lsn));
    wal->cleanup(pager->recovery_lsn());

    m_log->info("commit successful");
    system->commit_lsn = lsn;
    return ok();
}

auto DatabaseImpl::abort(Size id) -> Status
{
    if (!system->has_xact) {
        return logic_error("transaction has not been started");
    }
    if (id != m_txn_number) {
        return already_completed_error("abort");
    }
    if (auto s = do_abort(); !s.is_ok()) {
        m_status = std::move(s);
    }
    return status();
}

auto DatabaseImpl::do_abort() -> Status
{
    CALICO_EXPECT_TRUE(system->has_xact);
    m_log->info("abort requested (last commit was {})", system->commit_lsn.value);

    m_txn_number++;
    system->has_xact = false;
    CALICO_TRY_S(status());
    CALICO_TRY_S(wal->advance());

    CALICO_TRY_S(m_recovery->start_abort());
    CALICO_TRY_S(load_state());
    CALICO_TRY_S(m_recovery->finish_abort());
    m_log->info("abort successful");
    return ok();
}

auto DatabaseImpl::close() -> Status
{
    if (!m_recovery) {
        // We failed during open().
        return m_status;
    }

    CALICO_TRY_S(wal->close());
    CALICO_TRY_S(pager->flush({}));

    if (m_owns_storage) {
        m_owns_storage = false;
        delete m_storage;
    }
    return m_status;
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
    return Transaction {m_txn_number, *this};
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
    header.record_count = record_count;
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

    record_count = header.record_count;
    pager->load_state(header);
    tree->load_state(header);

    pager->release(std::move(*root));
    if (pager->page_count() < before_count) {
        const auto after_size = pager->page_count() * pager->page_size();
        return m_storage->resize_file(m_prefix + "data", after_size);
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

        header = FileHeader {
            Page {Id::root(), span, false}
        };

        if (!s.is_ok()) {
            return tl::make_unexpected(s);
        }

        if (header.page_size == 0) {
            return tl::make_unexpected(corruption("header indicates a page size of 0"));
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
            "header page size {} is too small (must be greater than or equal to {})", header.page_size, MINIMUM_PAGE_SIZE));
    }
    if (header.page_size > MAXIMUM_PAGE_SIZE) {
        return tl::make_unexpected(corruption(
            "header page size {} is too large (must be less than or equal to {})", header.page_size, MAXIMUM_PAGE_SIZE));
    }
    if (!is_power_of_two(header.page_size)) {
        return tl::make_unexpected(corruption(
            "header page size {} is invalid (must be a power of 2)", header.page_size));
    }
    return InitialState {header, !exists};
}

} // namespace Calico
