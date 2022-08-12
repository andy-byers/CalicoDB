
#include "core.h"
#include "calico/calico.h"
#include "calico/storage.h"
#include "header.h"
#include "pager/basic_pager.h"
#include "pager/pager.h"
#include "storage/disk.h"
#include "tree/bplus_tree.h"
#include "utils/crc.h"
#include "utils/layout.h"
#include "utils/logging.h"
#include "wal/basic_wal.h"
#include <filesystem>

namespace cco {

auto Info::record_count() const -> Size
{
    return m_core->record_count();
}

auto Info::page_count() const -> Size
{
    return m_core->page_count();
}

auto Info::page_size() const -> Size
{
    return m_core->page_size();
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
      m_sink {create_sink(path, options.log_level)},
      m_logger {create_logger(m_sink, "core")}
{}

auto Core::open() -> Status
{
    initialize_log(*m_logger, m_path);
    m_logger->trace("opening");

    auto initial = setup(*m_store, m_options, *m_logger);
    if (!initial.has_value()) return initial.error();
    auto [state, options, is_new, owns_store, owns_wal] = *initial;

    auto *store = m_options.store;
    if (!store) {
        auto s = DiskStorage::open(m_path, &store);
        if (!s.is_ok()) return s;
        m_owns_store = true;
    }
    m_store = store;

    auto *wal = m_options.wal;
    if (!wal) {
        auto s = BasicWriteAheadLog::open(*store, &wal);
        if (!s.is_ok()) return s;
        m_owns_wal = true;
    }
    m_wal = wal;

    {
        auto r = BasicPager::open({*store, *wal, m_sink, options.frame_count, options.page_size});
        if (!r.has_value()) return r.error();
        m_pager = std::move(*r);
    }

    {
        auto r = BPlusTree::open({*m_pager, m_sink, options.page_size});
        if (!r.has_value()) return r.error();
        m_tree = std::move(*r);
    }

    if (is_new) {
        auto root = m_pager->acquire(PageId::base(), true);
        if (!root.has_value()) return root.error();
        write_header(*root, state);
        m_pager->release(std::move(*root));

        commit();
        m_pager->flush();
    } else {
        // This is a no-op if the WAL is empty.
//        if (revised.use_xact)
//            CCO_TRY(impl->m_pool->recover());
        load_state();
    }
}

Core::~Core()
{
    // The specific error has already been logged in close().
    if (!close().has_value())
        m_logger->error("failed to close in destructor");

    if (m_owns_store)
        delete m_store;

    if (m_owns_wal)
        delete m_wal;
}

////auto Core::uses_xact() const -> bool
////{
////    return m_pool->uses_xact();
////}
//
//auto Core::can_commit() const -> bool
//{
//    return m_pool->can_commit();
//}

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

auto Core::insert(BytesView key, BytesView value) -> Result<bool>
{
    return m_tree->insert(key, value);
}

auto Core::erase(BytesView key) -> Result<bool>
{
    return erase(m_tree->find_exact(key));
}

auto Core::erase(Cursor cursor) -> Result<bool>
{
    return m_tree->erase(cursor);
}

//auto Core::commit() -> Result<void>
//{
//    static constexpr auto ERROR_PRIMARY = "cannot commit";
//    m_logger->trace("committing");
//
//    if (m_pool->uses_xact() && !m_pool->can_commit()) {
//        LogMessage message {*m_logger};
//        message.set_primary(ERROR_PRIMARY);
//        message.set_detail("transaction is empty");
//        return Err {message.logic_error()};
//    }
//    return save_state()
//        .and_then([this]() -> Result<void> {
//            CCO_TRY(m_pool->commit());
//            m_logger->trace("commit succeeded");
//            return {};
//        })
//        .or_else([this](const Status &status) -> Result<void> {
//            m_logger->error(ERROR_PRIMARY);
//            m_logger->error("(reason) {}", status.what());
//            return Err {status};
//        });
//}
//
//auto Core::abort() -> Result<void>
//{
//    static constexpr auto ERROR_PRIMARY = "cannot abort";
//    m_logger->trace("aborting");
//
//    if (m_pool->uses_xact() && !m_pool->can_commit()) {
//        LogMessage message {*m_logger};
//        message.set_primary(ERROR_PRIMARY);
//        message.set_detail("transaction is empty");
//        return Err {message.logic_error()};
//    }
//    return m_pool->abort()
//        .and_then([this]() -> Result<void> {
//            CCO_TRY(load_state());
//            m_logger->trace("abort succeeded");
//            return {};
//        })
//        .or_else([this](const Status &status) -> Result<void> {
//            m_logger->error(ERROR_PRIMARY);
//            m_logger->error("(reason) {}", status.what());
//            return Err {status};
//        });
//}
//
//auto Core::file_close() -> Result<void>
//{
//    auto cr = commit();
//    if (!cr.has_value()) {
//        m_logger->error("cannot commit before file_close");
//        if (cr.error().is_logic_error()) {
//            cr = Result<void> {};
//            m_logger->error("(reason) transaction is empty");
//        } else {
//            m_logger->error("(reason) {}", cr.error().what());
//        }
//    }
//
//    const auto fr = m_pool->flush();
//    if (!fr.has_value() && !fr.error().is_logic_error()) {
//        m_logger->error("cannot flush buffer pool before file_close");
//        m_logger->error("(reason) {}", fr.error().what());
//    }
//
//    const auto pr = m_pool->file_close();
//    if (!pr.has_value()) {
//        m_logger->error("cannot file_close buffer pool");
//        m_logger->error("(reason) {}", pr.error().what());
//    }
//    if (!m_is_temp) {
//        auto hr = m_home->file_close();
//        if (!hr.has_value()) {
//            m_logger->error("cannot file_close home directory");
//            m_logger->error("(reason) {}", hr.error().what());
//            // If there were errors from the previous methods, they are already logged. We can only return
//            // one of the errors, so we'll just go with this one.
//            return hr;
//        }
//    }
//    return !fr ? fr : (!cr ? cr : pr);
//}
//
//auto Core::save_state() -> Result<void>
//{
//    CCO_TRY_CREATE(root, m_tree->root(true));
//    auto header = get_file_header_writer(root);
//    m_logger->trace("saving file header");
//    m_pool->save_state(header);
//    m_tree->save_state(header);
//    header.update_header_crc();
//    return m_pool->release(root.take());
//}
//
//auto Core::load_state() -> Result<void>
//{
//    CCO_TRY_CREATE(root, m_tree->root(false));
//    auto header = get_file_header_reader(root);
//    m_logger->trace("loading file header");
//    m_pool->load_state(header);
//    m_tree->load_state(header);
//    return m_pool->release(root.take());
//}
//
//auto Core::cache_hit_ratio() const -> double
//{
//    return m_pool->hit_ratio();
//}
//
//auto Core::record_count() const -> Size
//{
//    return m_tree->cell_count();
//}
//
//auto Core::page_count() const -> Size
//{
//    return m_pool->page_count();
//}
//
//auto Core::page_size() const -> Size
//{
//    return m_pool->page_size();
//}
//
//auto Core::is_temp() const -> bool
//{
//    return m_is_temp;
//}

auto setup(Storage &store, const Options &options, spdlog::logger &logger) -> Result<InitialState>
{
    static constexpr auto ERROR_PRIMARY = "cannot open database";
    LogMessage message {logger};
    message.set_primary(ERROR_PRIMARY);

    auto revised = options;
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

    RandomAccessReader *reader {};
    RandomAccessEditor *editor {};

    if (auto s = store.open_random_reader(DATA_FILENAME, &reader); s.is_ok()) {
        Size file_size {};
        s = store.file_size(DATA_FILENAME, file_size);

        if (file_size < FileLayout::HEADER_SIZE) {
            message.set_detail("database is too small to read the file header");
            message.set_hint("file header is {} B", FileLayout::HEADER_SIZE);
            return Err {message.corruption()};
        }
        s = read_exact(*reader, bytes, 0);
        if (!s.is_ok()) return Err {s};

        // NOTE: This check is omitted if the page size is 0 to avoid getting a floating-point exception. If this is
        //       the case, we'll find out below when we make sure the page size is valid.
        if (header.page_size && file_size % header.page_size) {
            message.set_detail("database has an invalid size");
            message.set_hint("database must contain an integral number of pages");
            return Err {message.corruption()};
        }
        if (header.magic_code != MAGIC_CODE) {
            message.set_detail("path does not point to a Calico DB database");
            message.set_hint("magic code is {}, but should be {}", header.magic_code, MAGIC_CODE);
            return Err {message.invalid_argument()};
        }
        if (header.header_crc != crc_32(bytes)) {
            message.set_detail("header has an inconsistent CRC");
            message.set_hint("CRC is {}", header.header_crc);
            return Err {message.corruption()};
        }
        delete reader;

    } else if (s.is_not_found()) {
        s = store.open_random_editor(DATA_FILENAME, &editor);
        if (!s.is_ok()) return Err {s};

        header.magic_code = MAGIC_CODE;
        header.page_size = static_cast<std::uint16_t>(options.page_size);
        header.flushed_lsn = SequenceNumber::base().value;
        header.header_crc = crc_32(bytes);
        auto root = btos(bytes);
        root.resize(options.page_size);
        s = editor->write(stob(root), 0);
        if (!s.is_ok()) return Err {s};
        delete editor;

    } else {

    }

    const auto choose_error = [exists, &message] {
        return Err {exists ? message.corruption() : message.invalid_argument()};
    };

    if (reader.page_size() < MINIMUM_PAGE_SIZE) {
        message.set_detail("page size {} is too small", reader.page_size());
        message.set_hint("must be greater than or equal to {}", MINIMUM_PAGE_SIZE);
        return choose_error();
    }
    if (reader.page_size() > MAXIMUM_PAGE_SIZE) {
        message.set_detail("page size {} is too large", reader.page_size());
        message.set_hint("must be less than or equal to {}", MAXIMUM_PAGE_SIZE);
        return choose_error();
    }
    if (!is_power_of_two(reader.page_size())) {
        message.set_detail("page size {} is invalid", reader.page_size());
        message.set_hint("must be a power of 2");
        return choose_error();
    }

    revised.page_size = reader.page_size();
    return InitialState {std::move(backing), revised, !exists};
}










//
//auto BufferPool::commit() -> Result<void>
//{
//    CCO_EXPECT_TRUE(can_commit());
//    if (!m_status.is_ok())
//        return Err {m_status};
//
//    // All we need to do is write a commit record and start a new WAL segment.
//    //    if (m_uses_xact)
//    //        POOL_TRY(m_wal->commit());
//
//    return {};
//}
//
//auto BufferPool::abort() -> Result<void>
//{
//    CCO_EXPECT_TRUE(can_commit());
//
//    if (!m_uses_xact) {
//        ThreePartMessage message;
//        message.set_primary("cannot abort");
//        message.set_detail("not supported");
//        message.set_hint("transactions are disabled");
//        return Err {message.logic_error()};
//    }
//
//    //    CCO_TRY(m_wal->abort());
//    CCO_TRY(flush());
//    clear_error();
//    return {};
//}
//
//auto BufferPool::recover() -> Result<void>
//{
//    return {};//m_wal->recover();
//}

} // namespace cco

auto operator<(const cco::Record &lhs, const cco::Record &rhs) -> bool
{
    return cco::stob(lhs.key) < cco::stob(rhs.key);
}

auto operator>(const cco::Record &lhs, const cco::Record &rhs) -> bool
{
    return cco::stob(lhs.key) > cco::stob(rhs.key);
}

auto operator<=(const cco::Record &lhs, const cco::Record &rhs) -> bool
{
    return cco::stob(lhs.key) <= cco::stob(rhs.key);
}

auto operator>=(const cco::Record &lhs, const cco::Record &rhs) -> bool
{
    return cco::stob(lhs.key) >= cco::stob(rhs.key);
}

auto operator==(const cco::Record &lhs, const cco::Record &rhs) -> bool
{
    return cco::stob(lhs.key) == cco::stob(rhs.key);
}

auto operator!=(const cco::Record &lhs, const cco::Record &rhs) -> bool
{
    return cco::stob(lhs.key) != cco::stob(rhs.key);
}
