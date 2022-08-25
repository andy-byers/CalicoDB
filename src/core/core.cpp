
#include "core.h"
#include "calico/calico.h"
#include "calico/store.h"
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

#define MAYBE_FORWARD(expr, message) \
    do { \
        const auto &calico_s = (expr); \
        if (!calico_s.is_ok()) return forward_status(calico_s, message); \
    } while (0)

[[nodiscard]]
static auto not_supported_error(const std::string &primary)
{
    ThreePartMessage message;
    message.set_primary(primary);
    message.set_detail("action is not supported with the given database options");
    return message.logic_error();
}

[[nodiscard]]
static auto compute_header_crc(const FileHeader &state)
{
    BytesView bytes {reinterpret_cast<const Byte*>(&state), sizeof(state)};
    return crc_32(bytes.range(CRC_OFFSET));
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
    return m_core->options().page_size;
}

auto Info::maximum_key_size() const -> Size
{
    return get_max_local(page_size());
}

auto initialize_log(spdlog::logger &logger, const std::string &base)
{
    logger.info("starting CalicoDB v{} at \"{}\"", VERSION_NAME, base);
    logger.info("tree is located at \"{}/{}\"", base, DATA_FILENAME);
    logger.info("log is located at \"{}/{}\"", base, LOG_FILENAME);
}

Core::Core(const std::string &path, const Options &options)
    : m_prefix {path + "/"},
      m_options {options},
      m_sink {create_sink(path, options.log_level)}, // TODO: Don't create a logger if we don't have anything on disk, i.e. the store keeps everything in memory...
      m_logger {create_logger(m_sink, "core")}
{
    m_logger->info("constructing Core object");
}

auto Core::open() -> Status
{
    static constexpr auto MSG = "cannot open database";
    initialize_log(*m_logger, m_prefix);

    auto *store = m_options.store;
    if (!store) {
        store = new DiskStorage;
        m_owns_store = true;
    }
    m_store = store;
    m_options.store = store;

    auto initial = setup(m_prefix, *m_store, m_options, *m_logger);
    if (!initial.has_value()) return forward_status(initial.error(), MSG);
    auto [state, is_new] = *initial;
    m_options.page_size = state.page_size;

    auto *wal = m_options.wal;
    if (!wal) {
        auto s = BasicWriteAheadLog::open({
            m_prefix, // TODO: Could specify a different directory for WAL segments.
            m_store,
            m_sink,
            state.page_size,
        }, &wal);
        MAYBE_FORWARD(s, MSG);
        m_owns_wal = true;
    }
    m_wal = wal;
    m_options.wal = wal;
    m_wal->load_state(state);

    {
        auto r = BasicPager::open({m_prefix, *store, *wal, m_sink, m_options.frame_count, m_options.page_size});
        if (!r.has_value()) return forward_status(r.error(), MSG);
        m_pager = std::move(*r);
        m_pager->load_state(state);
    }

    {
        auto r = BPlusTree::open(*m_pager, m_sink, state.page_size);
        if (!r.has_value()) return forward_status(r.error(), MSG);
        m_tree = std::move(*r);
        m_tree->load_state(state);
    }

    auto s = Status::ok();
    if (is_new) {
        // The first call to root() allocates the root page.
        auto root = m_tree->root(true);
        CALICO_EXPECT_EQ(m_pager->page_count(), 1);
        s = !root.has_value() ? root.error() : s;
        MAYBE_FORWARD(s, MSG);

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

    if (m_owns_wal)
        delete m_wal;

    if (m_owns_store)
        delete m_store;
}

auto Core::destroy() -> Status
{
    auto s = Status::ok();

    if (m_owns_wal) {
        delete m_wal;
        m_owns_wal = false;
    }

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

auto Core::find_minimum() -> Cursor
{
    return m_tree->find_minimum();
}

auto Core::find_maximum() -> Cursor
{
    return m_tree->find_maximum();
}

auto Core::insert(BytesView key, BytesView value) -> Status
{
    auto s = m_tree->insert(key, value);
    if (!m_has_update) m_has_update = s.is_ok();
    return s;
}

auto Core::erase(BytesView key) -> Status
{
    return erase(m_tree->find_exact(key));
}

auto Core::erase(Cursor cursor) -> Status
{
    auto s = m_tree->erase(cursor);
    if (!m_has_update) m_has_update = s.is_ok();
    return s;
}

auto Core::commit() -> Status
{
    m_logger->info("received commit request");
    static constexpr auto MSG = "could not commit";

    // Committing an empty transaction is a NOOP.
    if (!m_has_update) return Status::ok();

    // Write database state to the root page file header.
    auto s = save_state();
    MAYBE_FORWARD(s, MSG);

    // Write a commit record to the WAL.
    s = m_wal->log_commit();
    MAYBE_FORWARD(s, MSG);
    m_logger->info("commit succeeded");
    return s;
}

auto Core::abort() -> Status
{
    m_logger->info("received abort request");
    static constexpr auto MSG = "could not abort";
    if (!m_wal->is_enabled()) return not_supported_error(MSG);

    // Aborting an empty transaction is a NOOP.
    if (!m_has_update) return Status::ok();

    // Stop the background writer thread.
    auto s = m_wal->stop_writer();
    MAYBE_FORWARD(s, MSG);

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

    s = m_pager->flush();
    MAYBE_FORWARD(s, MSG);

    // Load database state from the root page file header.
    s = load_state();
    MAYBE_FORWARD(s, MSG);

    m_logger->info("abort succeeded");
    return m_wal->start_writer();
}

auto Core::close() -> Status
{
    auto s = Status::ok();
    if (m_has_update) {
        s = commit();
        if (!s.is_ok()) s = forward_status(s, "cannot commit before stop");
    }
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
        const auto after_size = m_pager->page_count() * m_options.page_size;
        return m_store->resize_file(m_prefix + DATA_FILENAME, after_size);
    }
    return s;
}

auto setup(const std::string &path, Storage &store, const Options &options, spdlog::logger &logger) -> Result<InitialState>
{
    const auto MSG = fmt::format("cannot initialize database at \"{}\"", path);
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

    {   // TODO: Already getting created by the log sink...
        auto s = store.create_directory(path);
        if (!s.is_ok() && !s.is_logic_error()) {
            logger.error("cannot create database directory");
            logger.error("(reason) {}", s.what());
            return Err {s};
        }
    }

    const auto filename = fmt::format("{}/{}", path, DATA_FILENAME);
    RandomReader *reader {};
    RandomEditor *editor {};
    bool exists {};

    if (auto s = store.open_random_reader(filename, &reader); s.is_ok()) {
        Size file_size {};
        s = store.file_size(DATA_FILENAME, file_size);
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
        delete reader;

    } else if (s.is_not_found()) {
        s = store.open_random_editor(DATA_FILENAME, &editor);
        if (!s.is_ok()) return Err {s};

        header.magic_code = MAGIC_CODE;
        header.page_size = encode_page_size(options.page_size);
        header.flushed_lsn = SequenceId::base().value;
        header.header_crc = compute_header_crc(header);

        auto root = btos(bytes);
        root.resize(options.page_size);
        s = editor->write(stob(root), 0);
        if (!s.is_ok()) return Err {s};
        delete editor;

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

} // namespace calico

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
