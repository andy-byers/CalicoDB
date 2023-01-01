
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
#include "utils/crc.h"
#include "utils/header.h"
#include "utils/info_log.h"
#include "utils/layout.h"
#include "wal/basic_wal.h"
#include "wal/disabled_wal.h"

namespace calico {

#define MAYBE_FORWARD_STATUS(message) \
    do { \
        if (!m_status.is_ok()) return forward_status(m_status, message); \
    } while (0)

#define MAYBE_FORWARD(expr, message) \
    do { \
        const auto &calico_s = (expr); \
        if (!calico_s.is_ok()) return forward_status(calico_s, message); \
    } while (0)

#define MAYBE_SAVE_AND_FORWARD(expr, message) \
    do { \
        const auto &calico_s = (expr); \
        if (!calico_s.is_ok()) return save_and_forward_status(calico_s, message); \
    } while (0)

[[nodiscard]]
static auto sanitize_options(const Options &options) -> Options
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

auto initialize_log(Log &logger, const std::string &base)
{
    const auto version_name = fmt::format("v{}.{}.{}", CALICO_VERSION_MAJOR, CALICO_VERSION_MINOR, CALICO_VERSION_PATCH);
    logger.info("starting CalicoDB {} at \"{}\"", version_name, base);
    logger.info("tree is located at \"{}/{}\"", base, DATA_FILENAME);
    logger.info("log is located at \"{}/{}\"", base, LOG_FILENAME);
}

auto Core::open(const std::string &path, const Options &options) -> Status
{
    static constexpr auto MSG = "cannot open database";
    auto sanitized = sanitize_options(options);

    m_prefix = path + (path.back() == '/' ? "" :  "/");
    m_state = std::make_unique<System>(m_prefix, sanitized.log_level, sanitized.log_target);
    m_logger = m_state->create_log("core");
    m_logger->info("constructing Core object");

    m_store = sanitized.store;
    if (!m_store) {
        m_store = new PosixStorage;
        m_owns_store = true;
    }

    auto initial = setup(m_prefix, *m_store, sanitized, *m_logger);
    if (!initial.has_value()) return forward_status(initial.error(), MSG);
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
            m_state.get(),
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
            &m_status,
            &m_has_xact,
            &m_commit_lsn,
            m_state.get(),
            sanitized.frame_count,
            sanitized.page_size,
        });
        if (!r.has_value())
            return r.error();
        m_pager = std::move(*r);
        m_pager->load_state(state);
    }

    {
        auto r = BPlusTree::open(*m_pager, *m_state, sanitized.page_size);
        if (!r.has_value())
            return r.error();
        m_tree = std::move(*r);
        m_tree->load_state(state);
    }

    m_recovery = std::make_unique<Recovery>(*m_pager, *m_wal);

    auto s = ok();
    if (is_new) {
        // The first call to root() allocates the root page.
        auto root = m_tree->root(true);
        MAYBE_FORWARD(root ? ok() : root.error(), MSG);
        CALICO_EXPECT_EQ(m_pager->page_count(), 1);

        state.page_count = 1;
        state.header_crc = compute_header_crc(state);
        write_header(root->page(), state);
        s = m_pager->release(root->take());
        MAYBE_FORWARD(s, MSG);

        // This is safe right now because the WAL has not been started. If successful, we will have the root page
        // set up and saved to the database file.
        s = m_pager->flush({});

        if (s.is_ok() && m_wal->is_enabled())
            s = m_wal->start_workers();

    } else if (m_wal->is_enabled()) {
        // This should be a no-op if the database closed normally last time.
        s = ensure_consistency_on_startup();
        MAYBE_FORWARD(s, MSG);
    }

    return forward_status(s, MSG);
}

Core::~Core()
{
    m_logger->info("destroying Core object");
    m_wal.reset();

    if (m_owns_store)
        delete m_store;
}

auto Core::destroy() -> Status
{
    auto s = ok();
    m_wal.reset();

    std::vector<std::string> children;
    s = m_store->get_children(m_prefix, children);
    if (s.is_ok()) {
        for (const auto &name: children) {
            s = m_store->remove_file(name);

            if (!s.is_ok())
                forward_status(s, "could not remove file " + name);
        }
    } else {
        forward_status(s, "could get child names");
    }
    return m_store->remove_directory(m_prefix);
}

auto Core::forward_status(Status s, const std::string &message) -> Status
{
    if (!s.is_ok()) {
        m_logger->error("(1/2) {}", message);
        m_logger->error("(2/2) {}", s.what());
    }
    return s;
}

auto Core::save_and_forward_status(Status s, const std::string &message) -> Status
{
    if (!s.is_ok()) {
        m_logger->error("(1/2) {}", message);
        m_logger->error("(2/2) {}", s.what());

        if (m_status.is_ok()) {
            m_status = s;
        } else {
            m_logger->error("(1/2) error status is already set");
            m_logger->error("(2/2) {}", m_status.what());
        }
    }
    return s;
}

auto Core::status() const -> Status
{
    return m_status.is_ok() ? m_wal->worker_status() : m_status;
}

auto Core::path() const -> std::string
{
    return m_prefix;
}

auto Core::info() -> Info
{
    return Info {*this};
}

auto Core::find_exact(BytesView key) -> Cursor
{
    return m_tree->find_exact(key);
}

auto Core::find(BytesView key) -> Cursor
{
    return m_tree->find(key);
}

auto Core::first() -> Cursor
{
    return m_tree->find_minimum();
}

auto Core::last() -> Cursor
{
    return m_tree->find_maximum();
}

auto Core::insert(BytesView key, BytesView value) -> Status
{
    MAYBE_FORWARD_STATUS("could not insert");
    if (m_has_xact) {
        return m_tree->insert(key, value);
    } else {
        return atomic_insert(key, value);
    }
}

auto Core::erase(BytesView key) -> Status
{
    return erase(m_tree->find_exact(key));
}

auto Core::erase(Cursor cursor) -> Status
{
    MAYBE_FORWARD_STATUS("could not erase");
    if (m_has_xact) {
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
        m_status = xact.abort();
        return s;
    }
}
auto Core::atomic_erase(Cursor cursor) -> Status
{
    auto xact = transaction();
    auto s = m_tree->erase(cursor);
    if (s.is_ok()) {
        return xact.commit();
    } else if (!s.is_not_found()) {
        m_status = xact.abort();
    }
    return s;
}

auto Core::commit() -> Status
{
    CALICO_EXPECT_TRUE(m_has_xact);
    MAYBE_FORWARD_STATUS("could not commit");
    m_logger->info("received commit request");
    static constexpr auto MSG = "could not commit";

    if (!m_status.is_ok())
        return m_status;

    // Write database state to the root page file header.
    auto s = save_state();
    MAYBE_SAVE_AND_FORWARD(s, MSG);

    // Write a commit record to the WAL.
    const auto lsn = m_wal->current_lsn();
    WalPayloadIn payload {lsn, m_scratch->get()};
    const auto size = encode_commit_payload(payload.data());
    payload.shrink_to_fit(size);
    m_logger->info("commit LSN is {}", lsn.value);

    s = m_wal->log(payload);
    MAYBE_SAVE_AND_FORWARD(s, MSG);

    s = m_wal->advance();
    MAYBE_SAVE_AND_FORWARD(s, MSG);

    // Make sure every dirty page that hasn't been written back since the last commit is on disk.
    s = m_pager->flush(lsn); // TODO: m_commit_lsn);
    MAYBE_SAVE_AND_FORWARD(s, MSG);

    // Clean up obsolete WAL segments.
    s = m_wal->remove_before(m_commit_lsn);
    MAYBE_SAVE_AND_FORWARD(s, MSG);

    m_images.clear();
    m_commit_lsn = lsn;
    m_has_xact = false;
    m_logger->info("commit succeeded");
    return s;
}

auto Core::abort() -> Status
{
    m_logger->info("received abort request");
    static constexpr auto MSG = "could not abort";

    if (!m_status.is_ok())
        return m_status;

    if (!m_has_xact) {
        LogMessage message {*m_logger};
        message.set_primary(MSG);
        message.set_detail("a transaction is not active");
        message.set_hint("start a transaction and try again");
        return message.logic_error();
    }

    m_has_xact = false;
    MAYBE_SAVE_AND_FORWARD(m_recovery->start_abort(m_commit_lsn), MSG);
    MAYBE_SAVE_AND_FORWARD(load_state(), MSG);
    MAYBE_SAVE_AND_FORWARD(m_recovery->finish_abort(m_commit_lsn), MSG);
    return Status::ok();
}

auto Core::close() -> Status
{
    if (m_has_xact && status().is_ok()) {
        LogMessage message {*m_logger};
        message.set_primary("could not close");
        message.set_detail("a transaction is active");
        message.set_hint("finish the transaction and try again");
        return message.logic_error();
    }
    if (m_wal->is_working()) {
        auto s = m_wal->stop_workers();
        if (!s.is_ok()) forward_status(s, "could not stop WAL workers");
    }
    // We already waited on the WAL to be done writing so this should happen immediately.
    auto s = m_pager->flush({});
    MAYBE_SAVE_AND_FORWARD(s, "cannot flush pages before stop");
    return m_status;
}

auto Core::ensure_consistency_on_modify() -> Status
{
    static constexpr auto MSG = "cannot ensure consistent database state after modify";
    MAYBE_SAVE_AND_FORWARD(m_recovery->start_recovery(m_commit_lsn), MSG);
    MAYBE_SAVE_AND_FORWARD(load_state(), MSG);
    MAYBE_SAVE_AND_FORWARD(m_recovery->finish_recovery(m_commit_lsn), MSG);
    return Status::ok();
}

auto Core::ensure_consistency_on_startup() -> Status
{
    static constexpr auto MSG = "cannot ensure consistent database state on startup";
    MAYBE_SAVE_AND_FORWARD(m_recovery->start_recovery(m_commit_lsn), MSG);
    MAYBE_SAVE_AND_FORWARD(load_state(), MSG);
    MAYBE_SAVE_AND_FORWARD(m_recovery->finish_recovery(m_commit_lsn), MSG);
    return Status::ok();
}

auto Core::transaction() -> Transaction
{
    CALICO_EXPECT_TRUE(m_status.is_ok());
    m_has_xact = true;
    return Transaction {*this};
}

auto Core::save_state() -> Status
{
    m_logger->info("saving state to file header");

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
    m_logger->info("loading state from file header");

    auto root = m_pager->acquire(Id::root(), false);
    if (!root.has_value()) return root.error();

    auto state = read_header(*root);
    if (state.header_crc != compute_header_crc(state)) {
        LogMessage message {*m_logger};
        message.set_primary("cannot load database state");
        message.set_detail("file header is corrupted");
        message.set_hint("header CRC is {} but should be {}", state.header_crc, compute_header_crc(state));
        return message.corruption();
    }
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

auto setup(const std::string &prefix, Storage &store, const Options &options, Log &logger) -> tl::expected<InitialState, Status>
{
    const auto MSG = fmt::format("cannot initialize database at \"{}\"", prefix);
    LogMessage message {logger};
    message.set_primary(MSG);

    FileHeader header {};
    Bytes bytes {reinterpret_cast<Byte*>(&header), sizeof(FileHeader)};

    if (options.frame_count < MINIMUM_FRAME_COUNT) {
        message.set_detail("frame count is too small");
        message.set_hint("minimum frame count is {}", MINIMUM_FRAME_COUNT);
        return tl::make_unexpected(message.invalid_argument());
    }

    if (options.frame_count > MAXIMUM_FRAME_COUNT) {
        message.set_detail("frame count is too large");
        message.set_hint("maximum frame count is {}", MAXIMUM_FRAME_COUNT);
        return tl::make_unexpected(message.invalid_argument());
    }

    if (options.page_size < MINIMUM_PAGE_SIZE) {
        message.set_detail("page size {} is too small", options.page_size);
        message.set_hint("must be greater than or equal to {}", MINIMUM_PAGE_SIZE);
        return tl::make_unexpected(message.invalid_argument());
    }

    if (options.page_size > MAXIMUM_PAGE_SIZE) {
        message.set_detail("page size {} is too large", options.page_size);
        message.set_hint("must be less than or equal to {}", MAXIMUM_PAGE_SIZE);
        return tl::make_unexpected(message.invalid_argument());
    }

    if (!is_power_of_two(options.page_size)) {
        message.set_detail("page size {} is invalid", options.page_size);
        message.set_hint("must be a power of 2");
        return tl::make_unexpected(message.invalid_argument());
    }

    if (options.wal_limit != DISABLE_WAL && options.wal_limit < MINIMUM_WAL_LIMIT) {
        message.set_detail("WAL segment limit {} is too small", options.wal_limit);
        message.set_hint("must be greater than or equal to {} blocks", MINIMUM_WAL_LIMIT);
        return tl::make_unexpected(message.invalid_argument());
    }

    if (options.wal_limit > MAXIMUM_WAL_LIMIT) {
        message.set_detail("WAL segment limit {} is too large", options.wal_limit);
        message.set_hint("must be less than or equal to {} blocks", MAXIMUM_WAL_LIMIT);
        return tl::make_unexpected(message.invalid_argument());
    }

    {   // TODO: Already getting created by the log sink...
        auto s = store.create_directory(prefix);
        if (!s.is_ok() && !s.is_logic_error()) {
            logger.error("cannot create database directory");
            logger.error("(reason) {}", s.what());
            return tl::make_unexpected(s);
        }
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

        if (file_size < sizeof(FileHeader)) {
            message.set_detail("database is too small to read the file header");
            message.set_hint("file header is {} bytes", sizeof(FileHeader));
            return tl::make_unexpected(message.corruption());
        }
        s = read_exact(*reader, bytes, 0);
        if (!s.is_ok()) return tl::make_unexpected(s);

        if (file_size % decode_page_size(header.page_size)) {
            message.set_detail("database has an invalid size");
            message.set_hint("database must contain an integral number of pages");
            return tl::make_unexpected(message.corruption());
        }
        if (header.magic_code != MAGIC_CODE) {
            message.set_detail("path does not point to a Calico DB database");
            message.set_hint("magic code is {} but should be {}", header.magic_code, MAGIC_CODE);
            return tl::make_unexpected(message.invalid_argument());
        }
        if (header.header_crc != compute_header_crc(header)) {
            message.set_detail("header has an inconsistent CRC");
            message.set_hint("CRC is {} but should be {}", header.header_crc, compute_header_crc(header));
            return tl::make_unexpected(message.corruption());
        }
        exists = true;

    } else if (s.is_not_found()) {
        header.magic_code = MAGIC_CODE;
        header.page_size = encode_page_size(options.page_size);
        header.flushed_lsn = Id::root().value;
        header.header_crc = compute_header_crc(header);

    } else {
        return tl::make_unexpected(s);
    }

    const auto page_size = decode_page_size(header.page_size);

    if (page_size < MINIMUM_PAGE_SIZE) {
        message.set_detail("header page size {} is too small", page_size);
        message.set_hint("must be greater than or equal to {}", MINIMUM_PAGE_SIZE);
        return tl::make_unexpected(message.corruption());
    }

    if (!is_power_of_two(page_size)) {
        message.set_detail("header page size {} is invalid", page_size);
        message.set_hint("must either be 0 or a power of 2");
        return tl::make_unexpected(message.corruption());
    }

    return InitialState {header, !exists};
}

#undef MAYBE_FORWARD
#undef MAYBE_FORWARD_STATUS
#undef MAYBE_SAVE_AND_FORWARD

} // namespace calico
