
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
#include <filesystem>

namespace calico {

[[nodiscard]]
static auto empty_transaction_error(const std::string &primary)
{
    ThreePartMessage message;
    message.set_primary(primary);
    message.set_detail("transaction is empty");
    message.set_hint("update the database and try again");
    return message.logic_error();
}

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
    : m_path {path},
      m_options {options},
      m_sink {create_sink(path, options.log_level)}, // TODO: Don't create a logger if we don't have anything on disk, i.e. the store keeps everything in memory...
      m_logger {create_logger(m_sink, "core")}
{}

auto Core::open() -> Status
{
    static constexpr auto MSG = "cannot open database";
    initialize_log(*m_logger, m_path);
    m_logger->trace("opening");

    auto *store = m_options.store;
    if (!store) {
        store = new DiskStorage;
        m_owns_store = true;
    }
    m_store = store;
    m_options.store = store;

    auto initial = setup(m_path, *m_store, m_options, *m_logger);
    if (!initial.has_value()) return forward_status(initial.error(), MSG);
    auto [state, is_new] = *initial;

    auto *wal = m_options.wal;
    if (!wal) {
        wal = new DisabledWriteAheadLog;
//        auto s = BasicWriteAheadLog::open(*store, &wal);
//        if (!s.is_ok()) return forward_status(s, MSG);
        m_owns_wal = true;
    }
    m_wal = wal;
    m_options.wal = wal;
    m_wal->load_state(state);

    {
        auto r = BasicPager::open({m_path, *store, *wal, m_sink, m_options.frame_count, m_options.page_size});
        if (!r.has_value()) return forward_status(r.error(), MSG);
        m_pager = std::move(*r);
        m_pager->load_state(state);
    }

    {
        auto r = BPlusTree::open(*m_pager, m_sink, m_options.page_size);
        if (!r.has_value()) return forward_status(r.error(), MSG);
        m_tree = std::move(*r);
        m_tree->load_state(state);
    }

    auto s = Status::ok();
    if (is_new) {
        // Write out the root page containing the newly-populated file header.
        auto root = m_pager->acquire(PageId::root(), true);
        if (!root.has_value()) return forward_status(root.error(), MSG);
        CALICO_EXPECT_EQ(m_pager->page_count(), 1);
        state.page_count = 1; // Tree constructor should allocate the root page if it doesn't already exist.
        state.header_crc = compute_header_crc(state);
        write_header(*root, state);
        s = m_pager->release(std::move(*root));
        if (!s.is_ok()) return forward_status(s, MSG);

        s = m_pager->flush();

    } else {

        // This should be a no-op if the WAL is empty.
        if (m_wal->is_enabled()) {
            s = recover();
            if (!s.is_ok()) return forward_status(s, MSG);
            s = load_state();
        }
    }
    if (s.is_ok())
        s = m_wal->start();

    m_is_open = s.is_ok();
    return forward_status(s, MSG);
}

Core::~Core()
{
    // The specific error has already been logged in close().
    if (!close().is_ok())
        m_logger->error("failed to close in destructor");

    if (m_owns_store)
        delete m_store;

    if (m_owns_wal)
        delete m_wal;
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
    return m_path;
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

// TODO: Should we block until the commit record is actually on disk?
auto Core::commit() -> Status
{
    m_logger->trace("committing");
    static constexpr auto MSG = "could not commit";
    if (!m_has_update) return empty_transaction_error(MSG);

    auto s = save_state();
    if (!s.is_ok()) return forward_status(s, MSG);

    s = m_wal->log_commit();
    if (!s.is_ok()) return forward_status(s, MSG);
    m_logger->trace("commit succeeded");
    return s;
}

auto Core::abort() -> Status
{
    m_logger->trace("committing");
    static constexpr auto MSG = "could not abort";
    if (!m_wal->is_enabled()) return not_supported_error(MSG);
    if (!m_has_update) return empty_transaction_error(MSG);

    auto s = m_wal->stop();

    // This should give us the full images of each updated page belonging to the current transaction, before any changes were made,
    // in reverse order.
    s = m_wal->undo_last([this](UndoDescriptor undo) {
        const auto [id, image] = undo;
        auto page = m_pager->acquire(PageId {id}, true);
        if (!page.has_value()) return page.error();

        page->undo(undo);
        return m_pager->release(std::move(*page));
    });
    if (!s.is_ok()) return forward_status(s, MSG);

    s = load_state();
    if (!s.is_ok()) return forward_status(s, MSG);

    m_logger->trace("abort succeeded");
    return m_wal->start();
}

auto Core::close() -> Status
{
    // Failed in open().
    if (!m_is_open) return Status::logic_error("not open");

    auto s = commit();
    if (!s.is_ok()) s = forward_status(s, "cannot commit before close");

    s = m_pager->flush();
    if (!s.is_ok() && !s.is_logic_error()) s = forward_status(s, "cannot commit before close");

    return s;
}

auto Core::recover() -> Status
{
    static constexpr auto MSG = "cannot recover";

    bool found_commit {};
    auto s = m_wal->redo_all([&found_commit, this](RedoDescriptor redo) {
        found_commit = redo.is_commit;

        if (!found_commit) {
            auto page = m_pager->acquire(PageId {redo.page_id}, true);
            if (!page.has_value()) return page.error();

            page->redo(redo);
            return m_pager->release(std::move(*page));
        }
        return Status::ok();
    });
    if (!s.is_ok()) return forward_status(s, MSG);

    if (!found_commit) {
        s = m_wal->undo_last([this](UndoDescriptor undo) {
            const auto [id, image] = undo;
            auto page = m_pager->acquire(PageId {id}, true);
            if (!page.has_value()) return page.error();

            page->undo(undo);
            return m_pager->release(std::move(*page));
        });
        if (!s.is_ok()) return forward_status(s, MSG);
    }
    s = load_state();
    if (!s.is_ok()) return forward_status(s, MSG);
    return s;
}

auto Core::save_state() -> Status
{
    m_logger->trace("saving file header");

    auto root = m_pager->acquire(PageId::root(), true);
    if (!root.has_value()) return root.error();

    auto state = read_header(*root);

    m_pager->save_state(state);
    m_tree->save_state(state);
    m_wal->save_state(state);
    state.header_crc = compute_header_crc(state);
    write_header(*root, state);

fmt::print("save crc = {}\n", state.header_crc);

    return m_pager->release(std::move(*root));
}

auto Core::load_state() -> Status
{
    m_logger->trace("loading file header");

    auto root = m_pager->acquire(PageId::root(), true);
    if (!root.has_value()) return root.error();

    auto state = read_header(*root);
    if (state.header_crc != compute_header_crc(state)) {
        LogMessage message {*m_logger};
        message.set_primary("cannot load database state");
        message.set_detail("file header is corrupted");
        message.set_hint("header CRC is {} but should be {}", state.header_crc, compute_header_crc(state));
        return message.corruption();
    }
fmt::print("load crc = {}\n", state.header_crc);

    load_state(state);
    return m_pager->release(std::move(*root));
}

auto Core::load_state(const FileHeader &state) -> void
{
    m_pager->load_state(state);
    m_tree->load_state(state);
    m_wal->load_state(state);
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

    {
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
fmt::print("init crc = {}\n", header.header_crc);
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

} // namespace cco

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
