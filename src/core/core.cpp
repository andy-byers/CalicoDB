
#include "core.h"
#include "calico/calico.h"
#include "calico/store.h"
#include "calico/transaction.h"
#include "header.h"
#include "pager/basic_pager.h"
#include "pager/pager.h"
#include "store/disk.h"
#include "tree/bplus_tree.h"
#include "utils/crc.h"
#include "utils/layout.h"
#include "utils/logging.h"
#include "wal/basic_wal.h"
#include "wal/disabled_wal.h"
#include <filesystem>

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
static auto compute_header_crc(const FileHeader &state)
{
    BytesView bytes {reinterpret_cast<const Byte*>(&state), sizeof(state)};
    return crc_32(bytes.range(CRC_OFFSET));
}

[[nodiscard]]
static auto sanitize_options(const Options &options) -> Options
{
    auto sanitized = options;
    if (sanitized.log_level > MAXIMUM_LOG_LEVEL)
        sanitized.log_level = DEFAULT_LOG_LEVEL;
    return sanitized;
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

auto initialize_log(spdlog::logger &logger, const std::string &base)
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
    m_sink = sanitized.log_level ? create_sink(path, sanitized.log_level) : create_sink();
    m_logger = create_logger(m_sink, "core");
    initialize_log(*m_logger, path);
    m_logger->info("constructing Core object");

    m_store = sanitized.store;
    if (!m_store) {
        m_store = new DiskStorage;
        m_owns_store = true;
    }

    auto initial = setup(m_prefix, *m_store, sanitized, *m_logger);
    if (!initial.has_value()) return forward_status(initial.error(), MSG);
    auto [state, is_new] = *initial;

    // The database will store 0 in the "page_size" header field if the maximum page size is used (1 << 16 cannot be held
    // in a std::uint16_t).
    if (!is_new) sanitized.page_size = decode_page_size(state.page_size);

    if (sanitized.wal_limit != DISABLE_WAL) {
        // The WAL segments may be stored elsewhere.
        const auto wal_prefix = sanitized.wal_path.empty()
            ? m_prefix : std::string {sanitized.wal_path} + "/";
        WriteAheadLog *wal {};
        auto s = BasicWriteAheadLog::open({
            wal_prefix,
            m_store,
            m_sink,
            sanitized.page_size,
            sanitized.wal_limit,
        }, &wal);
        MAYBE_FORWARD(s, MSG);
        m_wal.reset(wal);
        m_wal->load_state(state);
    } else {
        m_wal = std::make_unique<DisabledWriteAheadLog>();
    }

    {
        auto r = BasicPager::open({
            m_prefix,
            *m_store,
            *m_wal,
            m_status,
            m_has_xact,
            m_sink,
            sanitized.frame_count,
            sanitized.page_size,
        });
        MAYBE_FORWARD(r ? Status::ok() : r.error(), MSG);
        m_pager = std::move(*r);
        m_pager->load_state(state);
    }

    {
        auto r = BPlusTree::open(*m_pager, m_sink, sanitized.page_size);
        MAYBE_FORWARD(r ? Status::ok() : r.error(), MSG);
        m_tree = std::move(*r);
        m_tree->load_state(state);
    }

    auto s = Status::ok();
    if (is_new) {
        // The first call to root() allocates the root page.
        auto root = m_tree->root(true);
        MAYBE_FORWARD(root ? Status::ok() : root.error(), MSG);
        CALICO_EXPECT_EQ(m_pager->page_count(), 1);

        state.page_count = 1;
        state.header_crc = compute_header_crc(state);
        write_header(root->page(), state);
        s = m_pager->release(root->take());
        MAYBE_FORWARD(s, MSG);

        // This is safe right now because the WAL has not been started. If successful, we will have the root page set up and saved
        // to the database file.
        s = m_pager->flush();

    } else if (m_wal->is_enabled()) {
        // This should be a no-op if the database closed normally last time.
        s = ensure_consistent_state();
        MAYBE_FORWARD(s, MSG);
    }

    if (s.is_ok() && m_wal->is_enabled())
        s = m_wal->start_writer();

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
    auto s = Status::ok();
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
        m_logger->error(message);
        m_logger->error("(reason) {}", s.what());
    }
    return s;
}

auto Core::save_and_forward_status(Status s, const std::string &message) -> Status
{
    CALICO_EXPECT_TRUE(m_status.is_ok());
    if (!s.is_ok()) {
        m_logger->error(message);
        m_logger->error("(reason) {}", s.what());
        m_status = s;
    }
    return s;
}

auto Core::status() const -> Status
{
    return m_pager->status();
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

    // Write database state to the root page file header.
    auto s = save_state();
    MAYBE_SAVE_AND_FORWARD(s, MSG);

    // Write a commit record to the WAL.
    s = m_wal->log_commit();
    MAYBE_SAVE_AND_FORWARD(s, MSG);
    m_logger->info("commit succeeded");
    m_has_xact = false;
    return s;
}

auto Core::abort() -> Status
{
    CALICO_EXPECT_TRUE(m_has_xact);
    m_logger->info("received abort request");
    static constexpr auto MSG = "could not abort";
    if (!m_wal->is_enabled()) return Status::ok();
    auto s = Status::ok();

    if (m_wal->is_writing()) {
        s = m_wal->stop_writer();
        MAYBE_FORWARD(s, MSG);
    }

    // This should give us the full images of each updated page belonging to the current transaction, before any changes were made,
    // in reverse order.
    s = m_wal->abort_last([this](UndoDescriptor undo) {
        const auto [id, image] = undo;
        auto page = m_pager->acquire(PageId {id}, true);
        if (!page.has_value()) return page.error();

        page->undo(undo);
        return m_pager->release(std::move(*page));
    });
    MAYBE_FORWARD(s, MSG);

    // Load database state from the root page file header. We need the new page count to determine which pages we can flush.
    s = load_state();
    MAYBE_FORWARD(s, MSG);

    s = m_pager->flush();
    MAYBE_FORWARD(s, MSG);

    // Database state is ALMOST restored if we have made it here.
    m_status = Status::ok();

    m_logger->info("abort succeeded");
    s = m_wal->start_writer();
    MAYBE_SAVE_AND_FORWARD(s, MSG);

    m_has_xact = false;
    return Status::ok();
}

auto Core::close() -> Status
{
    if (m_has_xact) {
        LogMessage message {*m_logger};
        message.set_primary("could not close");
        message.set_detail("a transaction is active");
        message.set_hint("finish the transaction and try again");
        return message.logic_error();
    }

    auto s = Status::ok();
    if (m_wal->is_writing()) {
        s = m_wal->stop_writer();
        if (!s.is_ok()) s = forward_status(s, "cannot stop WAL");
    }
    // We already waited on the WAL to be done writing so this should happen immediately.
    s = m_pager->flush();

    if (!s.is_ok()) s = forward_status(s, "cannot flush pages before stop");

    return s;
}

auto Core::ensure_consistent_state() -> Status
{
    static constexpr auto MSG = "cannot ensure consistent database state";

    auto s = m_wal->setup_and_recover(
        [this](const auto &info) {
            if (!info.is_commit) {
                auto page = m_pager->acquire(PageId {info.page_id}, true);
                if (!page.has_value()) return page.error();

                page->redo(info);
                return m_pager->release(std::move(*page));
            }
            return Status::ok();
        },
        [this](UndoDescriptor undo) {
            const auto [id, image] = undo;
            auto page = m_pager->acquire(PageId {id}, true);
            if (!page.has_value()) return page.error();

            page->undo(undo);
            return m_pager->release(std::move(*page));
        });
    MAYBE_FORWARD(s, MSG);

    s = load_state();
    MAYBE_FORWARD(s, MSG);
    s = m_pager->flush();
    return forward_status(s, MSG);
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

    auto root = m_pager->acquire(PageId::root(), true);
    if (!root.has_value()) return root.error();

    auto state = read_header(*root);
    m_pager->save_state(state);
    m_tree->save_state(state);
    m_wal->save_state(state);
    state.header_crc = compute_header_crc(state);
    write_header(*root, state);

    return m_pager->release(std::move(*root));
}

auto Core::load_state() -> Status
{
    m_logger->info("loading state from file header");

    auto root = m_pager->acquire(PageId::root(), false);
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
    m_wal->load_state(state);

    auto s = m_pager->release(std::move(*root));
    if (s.is_ok() && m_pager->page_count() < before_count) {
        const auto after_size = m_pager->page_count() * m_pager->page_size();
        return m_store->resize_file(m_prefix + DATA_FILENAME, after_size);
    }
    return s;
}

auto setup(const std::string &prefix, Storage &store, const Options &options, spdlog::logger &logger) -> Result<InitialState>
{
    const auto MSG = fmt::format("cannot initialize database at \"{}\"", prefix);
    LogMessage message {logger};
    message.set_primary(MSG);

    FileHeader header {};
    Bytes bytes {reinterpret_cast<Byte*>(&header), sizeof(FileHeader)};

    if (options.frame_count < MINIMUM_FRAME_COUNT) {
        message.set_detail("frame count is too small");
        message.set_hint("minimum frame count is {}", MINIMUM_FRAME_COUNT);
        return Err {message.invalid_argument()};
    }

    if (options.frame_count > MAXIMUM_FRAME_COUNT) {
        message.set_detail("frame count is too large");
        message.set_hint("maximum frame count is {}", MAXIMUM_FRAME_COUNT);
        return Err {message.invalid_argument()};
    }

    if (options.page_size < MINIMUM_PAGE_SIZE) {
        message.set_detail("page size {} is too small", options.page_size);
        message.set_hint("must be greater than or equal to {}", MINIMUM_PAGE_SIZE);
        return Err {message.invalid_argument()};
    }

    if (options.page_size > MAXIMUM_PAGE_SIZE) {
        message.set_detail("page size {} is too large", options.page_size);
        message.set_hint("must be less than or equal to {}", MAXIMUM_PAGE_SIZE);
        return Err {message.invalid_argument()};
    }

    if (!is_power_of_two(options.page_size)) {
        message.set_detail("page size {} is invalid", options.page_size);
        message.set_hint("must be a power of 2");
        return Err {message.invalid_argument()};
    }

    if (options.wal_limit != DISABLE_WAL && options.wal_limit < MINIMUM_WAL_LIMIT) {
        message.set_detail("WAL segment limit {} is too small", options.wal_limit);
        message.set_hint("must be greater than or equal to {} blocks", MINIMUM_WAL_LIMIT);
        return Err {message.invalid_argument()};
    }

    if (options.wal_limit > MAXIMUM_WAL_LIMIT) {
        message.set_detail("WAL segment limit {} is too large", options.wal_limit);
        message.set_hint("must be less than or equal to {} blocks", MAXIMUM_WAL_LIMIT);
        return Err {message.invalid_argument()};
    }

    {   // TODO: Already getting created by the log sink...
        auto s = store.create_directory(prefix);
        if (!s.is_ok() && !s.is_logic_error()) {
            logger.error("cannot create database directory");
            logger.error("(reason) {}", s.what());
            return Err {s};
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
        if (!s.is_ok()) return Err {s};

        if (file_size < FileLayout::HEADER_SIZE) {
            message.set_detail("database is too small to read the file header");
            message.set_hint("file header is {} bytes", FileLayout::HEADER_SIZE);
            return Err {message.corruption()};
        }
        s = read_exact(*reader, bytes, 0);
        if (!s.is_ok()) return Err {s};

        if (header.page_size && file_size % header.page_size) {
            message.set_detail("database has an invalid size");
            message.set_hint("database must contain an integral number of pages");
            return Err {message.corruption()};
        }
        if (header.magic_code != MAGIC_CODE) {
            message.set_detail("path does not point to a Calico DB database");
            message.set_hint("magic code is {} but should be {}", header.magic_code, MAGIC_CODE);
            return Err {message.invalid_argument()};
        }
        if (header.header_crc != compute_header_crc(header)) {
            message.set_detail("header has an inconsistent CRC");
            message.set_hint("CRC is {} but should be {}", header.header_crc, compute_header_crc(header));
            return Err {message.corruption()};
        }
        exists = true;

    } else if (s.is_not_found()) {
        header.magic_code = MAGIC_CODE;
        header.page_size = encode_page_size(options.page_size);
        header.flushed_lsn = SequenceId::base().value;
        header.header_crc = compute_header_crc(header);

    } else {
        return Err {s};
    }

    if (header.page_size && header.page_size < MINIMUM_PAGE_SIZE) {
        message.set_detail("header page size {} is too small", header.page_size);
        message.set_hint("must be greater than or equal to {}", MINIMUM_PAGE_SIZE);
        return Err {message.corruption()};
    }

    if (header.page_size && !is_power_of_two(header.page_size)) {
        message.set_detail("header page size {} is invalid", header.page_size);
        message.set_hint("must either be 0 or a power of 2");
        return Err {message.corruption()};
    }

    return InitialState {header, !exists};
}

#undef MAYBE_FORWARD
#undef MAYBE_FORWARD_STATUS
#undef MAYBE_SAVE_AND_FORWARD

} // namespace calico
