//
//#include "database_impl.h"
//#include "calico/calico.h"
//#include "calico/storage.h"
//#include "page/file_header.h"
//#include "pool/buffer_pool.h"
//#include "tree/bplus_tree.h"
//#include "utils/layout.h"
//#include "utils/logging.h"
//#include <filesystem>
//
//namespace cco {
//
//namespace fs = std::filesystem;
//
//auto Info::cache_hit_ratio() const -> double
//{
//    return m_impl->cache_hit_ratio();
//}
//
//auto Info::record_count() const -> Size
//{
//    return m_impl->record_count();
//}
//
//auto Info::page_count() const -> Size
//{
//    return m_impl->page_count();
//}
//
//auto Info::page_size() const -> Size
//{
//    return m_impl->page_size();
//}
//
//auto Info::maximum_key_size() const -> Size
//{
//    return get_max_local(page_size());
//}
//
//auto Info::is_temp() const -> bool
//{
//    return m_impl->is_temp();
//}
//
//auto Info::uses_xact() const -> bool
//{
//    return m_impl->uses_xact();
//}
//
//auto initialize_log(spdlog::logger &logger, const std::string &base)
//{
//    logger.info("starting CalicoDB v{} at \"{}\"", VERSION_NAME, base);
//    logger.info("tree is located at \"{}/{}\"", base, DATA_NAME);
//    logger.info("log is located at \"{}/{}\"", base, LOG_NAME);
//}
//
//auto Database::Impl::open(Parameters param, std::unique_ptr<Storage> home) -> Result<std::unique_ptr<Impl>>
//{
//    const auto path = param.options.path;
//    auto sink = create_sink(path, param.options.log_level);
//    auto logger = create_logger(sink, "db");
//    initialize_log(*logger, path);
//
//    CCO_TRY_CREATE(initial_state, setup(*home, param.options, *logger));
//    logger->trace("opening");
//
//    auto impl = std::make_unique<Impl>();
//    auto [backing, revised, is_new] = std::move(initial_state);
//    FileHeaderReader state {stob(backing)};
//
//    CCO_TRY_STORE(impl->m_pool, BufferPool::open({
//        *home,
//        sink,
//        state.flushed_lsn(),
//        revised.frame_count,
//        state.page_count(),
//        state.page_size(),
//    }));
//
//    CCO_TRY_STORE(impl->m_tree, Tree::open({
//        impl->m_pool.get(),
//        sink,
//        state.free_start(),
//        state.record_count(),
//    }));
//
//    impl->m_sink = std::move(sink);
//    impl->m_logger = logger;
//    impl->m_home = std::move(home);
//
//    if (is_new) {
//        CCO_TRY_CREATE(root, impl->m_tree->allocate_root());
//        auto header = get_file_header_writer(root);
//        header.update_magic_code();
//        header.set_page_size(state.page_size());
//        CCO_TRY(impl->m_pool->release(root.take()));
//        CCO_TRY(impl->commit());
//        CCO_TRY(impl->m_pool->flush());
//    } else {
//        // This is a no-op if the WAL is empty.
////        if (revised.use_xact)
////            CCO_TRY(impl->m_pool->recover());
//        CCO_TRY(impl->load_header());
//    }
//    return impl;
//}
//
//Database::Impl::~Impl()
//{
//    // The specific error has already been logged in close().
//    if (!close().has_value())
//        m_logger->error("failed to close in destructor");
//}
//
////auto Database::Impl::uses_xact() const -> bool
////{
////    return m_pool->uses_xact();
////}
//
//auto Database::Impl::can_commit() const -> bool
//{
//    return m_pool->can_commit();
//}
//
//auto Database::Impl::status() const -> Status
//{
//    return m_pool->status();
//}
//
////auto Database::Impl::path() const -> std::string
////{
////    return m_is_temp ? std::string {} : m_home->path();
////}
//
//auto Database::Impl::info() -> Info
//{
//    return Info {*this};
//}
//
//auto Database::Impl::find_exact(BytesView key) -> Cursor
//{
//    return m_tree->find_exact(key);
//}
//
//auto Database::Impl::find(BytesView key) -> Cursor
//{
//    return m_tree->find(key);
//}
//
//auto Database::Impl::find_minimum() -> Cursor
//{
//    return m_tree->find_minimum();
//}
//
//auto Database::Impl::find_maximum() -> Cursor
//{
//    return m_tree->find_maximum();
//}
//
//auto Database::Impl::insert(BytesView key, BytesView value) -> Result<bool>
//{
//    return m_tree->insert(key, value);
//}
//
//auto Database::Impl::erase(BytesView key) -> Result<bool>
//{
//    return erase(m_tree->find_exact(key));
//}
//
//auto Database::Impl::erase(Cursor cursor) -> Result<bool>
//{
//    return m_tree->erase(cursor);
//}
//
////auto Database::Impl::commit() -> Result<void>
////{
////    static constexpr auto ERROR_PRIMARY = "cannot commit";
////    m_logger->trace("committing");
////
////    if (m_pool->uses_xact() && !m_pool->can_commit()) {
////        LogMessage message {*m_logger};
////        message.set_primary(ERROR_PRIMARY);
////        message.set_detail("transaction is empty");
////        return Err {message.logic_error()};
////    }
////    return save_state()
////        .and_then([this]() -> Result<void> {
////            CCO_TRY(m_pool->commit());
////            m_logger->trace("commit succeeded");
////            return {};
////        })
////        .or_else([this](const Status &status) -> Result<void> {
////            m_logger->error(ERROR_PRIMARY);
////            m_logger->error("(reason) {}", status.what());
////            return Err {status};
////        });
////}
////
////auto Database::Impl::abort() -> Result<void>
////{
////    static constexpr auto ERROR_PRIMARY = "cannot abort";
////    m_logger->trace("aborting");
////
////    if (m_pool->uses_xact() && !m_pool->can_commit()) {
////        LogMessage message {*m_logger};
////        message.set_primary(ERROR_PRIMARY);
////        message.set_detail("transaction is empty");
////        return Err {message.logic_error()};
////    }
////    return m_pool->abort()
////        .and_then([this]() -> Result<void> {
////            CCO_TRY(load_state());
////            m_logger->trace("abort succeeded");
////            return {};
////        })
////        .or_else([this](const Status &status) -> Result<void> {
////            m_logger->error(ERROR_PRIMARY);
////            m_logger->error("(reason) {}", status.what());
////            return Err {status};
////        });
////}
////
////auto Database::Impl::close() -> Result<void>
////{
////    auto cr = commit();
////    if (!cr.has_value()) {
////        m_logger->error("cannot commit before close");
////        if (cr.error().is_logic_error()) {
////            cr = Result<void> {};
////            m_logger->error("(reason) transaction is empty");
////        } else {
////            m_logger->error("(reason) {}", cr.error().what());
////        }
////    }
////
////    const auto fr = m_pool->flush();
////    if (!fr.has_value() && !fr.error().is_logic_error()) {
////        m_logger->error("cannot flush buffer pool before close");
////        m_logger->error("(reason) {}", fr.error().what());
////    }
////
////    const auto pr = m_pool->close();
////    if (!pr.has_value()) {
////        m_logger->error("cannot close buffer pool");
////        m_logger->error("(reason) {}", pr.error().what());
////    }
////    if (!m_is_temp) {
////        auto hr = m_home->close();
////        if (!hr.has_value()) {
////            m_logger->error("cannot close home directory");
////            m_logger->error("(reason) {}", hr.error().what());
////            // If there were errors from the previous methods, they are already logged. We can only return
////            // one of the errors, so we'll just go with this one.
////            return hr;
////        }
////    }
////    return !fr ? fr : (!cr ? cr : pr);
////}
////
////auto Database::Impl::save_state() -> Result<void>
////{
////    CCO_TRY_CREATE(root, m_tree->root(true));
////    auto header = get_file_header_writer(root);
////    m_logger->trace("saving file header");
////    m_pool->save_state(header);
////    m_tree->save_state(header);
////    header.update_header_crc();
////    return m_pool->release(root.take());
////}
////
////auto Database::Impl::load_state() -> Result<void>
////{
////    CCO_TRY_CREATE(root, m_tree->root(false));
////    auto header = get_file_header_reader(root);
////    m_logger->trace("loading file header");
////    m_pool->load_state(header);
////    m_tree->load_state(header);
////    return m_pool->release(root.take());
////}
////
////auto Database::Impl::cache_hit_ratio() const -> double
////{
////    return m_pool->hit_ratio();
////}
////
////auto Database::Impl::record_count() const -> Size
////{
////    return m_tree->cell_count();
////}
////
////auto Database::Impl::page_count() const -> Size
////{
////    return m_pool->page_count();
////}
////
////auto Database::Impl::page_size() const -> Size
////{
////    return m_pool->page_size();
////}
////
////auto Database::Impl::is_temp() const -> bool
////{
////    return m_is_temp;
////}
////
////auto setup(Storage &directory, const Options &options, spdlog::logger &logger) -> Result<InitialState>
////{
////    static constexpr auto ERROR_PRIMARY = "cannot open database";
////    LogMessage message {logger};
////    message.set_primary(ERROR_PRIMARY);
////
////    const auto perm = options.permissions;
////    auto revised = options;
////    std::string backing(FileLayout::HEADER_SIZE, '\x00');
////    FileHeaderReader reader {stob(backing)};
////    FileHeaderWriter writer {stob(backing)};
////
////    if (options.frame_count < MINIMUM_FRAME_COUNT) {
////        message.set_detail("frame count is too small");
////        message.set_hint("minimum frame count is {}", MINIMUM_FRAME_COUNT);
////        return Err {message.invalid_argument()};
////    }
////
////    if (options.frame_count > MAXIMUM_FRAME_COUNT) {
////        message.set_detail("frame count is too large");
////        message.set_hint("maximum frame count is {}", MAXIMUM_FRAME_COUNT);
////        return Err {message.invalid_argument()};
////    }
////
////    CCO_TRY_CREATE(exists, directory.exists(DATA_NAME));
////
////    if (exists) {
////        CCO_TRY_CREATE(file, directory.open_file(DATA_NAME, Mode::READ_ONLY, perm));
////        CCO_TRY_CREATE(file_size, file->size());
////
////        if (file_size < FileLayout::HEADER_SIZE) {
////            message.set_detail("database is too small to read the file header");
////            message.set_hint("file header is {} B", FileLayout::HEADER_SIZE);
////            return Err {message.corruption()};
////        }
////        if (!read_exact(*file, stob(backing))) {
////            message.set_detail("cannot read file header");
////            return Err {message.corruption()};
////        }
////        // NOTE: This check is omitted if the page size is 0 to avoid getting a floating-point exception. If this is
////        //       the case, we'll find out below when we make sure the page size is valid.
////        if (reader.page_size() && file_size % reader.page_size()) {
////            message.set_detail("database has an invalid size");
////            message.set_hint("database must contain an integral number of pages");
////            return Err {message.corruption()};
////        }
////        if (!reader.is_magic_code_consistent()) {
////            message.set_detail("path does not point to a Calico DB database");
////            message.set_hint("magic code is {}, but should be {}", reader.magic_code(), MAGIC_CODE);
////            return Err {message.invalid_argument()};
////        }
////        if (!reader.is_header_crc_consistent()) {
////            message.set_detail("header has an inconsistent CRC");
////            message.set_hint("CRC is {}", reader.header_crc());
////            return Err {message.corruption()};
////        }
////        revised.use_xact = !reader.flushed_lsn().is_null();
////        CCO_TRY(file->close());
////    } else {
////        writer.update_magic_code();
////        writer.set_page_size(options.page_size);
////        writer.set_flushed_lsn(SequenceNumber::base());
////        writer.update_header_crc();
////    }
////
////    const auto choose_error = [exists, &message] {
////        return Err {exists ? message.corruption() : message.invalid_argument()};
////    };
////
////    if (reader.page_size() < MINIMUM_PAGE_SIZE) {
////        message.set_detail("page size {} is too small", reader.page_size());
////        message.set_hint("must be greater than or equal to {}", MINIMUM_PAGE_SIZE);
////        return choose_error();
////    }
////    if (reader.page_size() > MAXIMUM_PAGE_SIZE) {
////        message.set_detail("page size {} is too large", reader.page_size());
////        message.set_hint("must be less than or equal to {}", MAXIMUM_PAGE_SIZE);
////        return choose_error();
////    }
////    if (!is_power_of_two(reader.page_size())) {
////        message.set_detail("page size {} is invalid", reader.page_size());
////        message.set_hint("must be a power of 2");
////        return choose_error();
////    }
////
////    revised.page_size = reader.page_size();
////    return InitialState {std::move(backing), revised, !exists};
////}
//
//
//
//
//
//
//
//
//
//
////
////auto BufferPool::commit() -> Result<void>
////{
////    CCO_EXPECT_TRUE(can_commit());
////    if (!m_status.is_ok())
////        return Err {m_status};
////
////    // All we need to do is write a commit record and start a new WAL segment.
////    //    if (m_uses_xact)
////    //        POOL_TRY(m_wal->commit());
////
////    return {};
////}
////
////auto BufferPool::abort() -> Result<void>
////{
////    CCO_EXPECT_TRUE(can_commit());
////
////    if (!m_uses_xact) {
////        ThreePartMessage message;
////        message.set_primary("cannot abort");
////        message.set_detail("not supported");
////        message.set_hint("transactions are disabled");
////        return Err {message.logic_error()};
////    }
////
////    //    CCO_TRY(m_wal->abort());
////    CCO_TRY(flush());
////    clear_error();
////    return {};
////}
////
////auto BufferPool::recover() -> Result<void>
////{
////    return {};//m_wal->recover();
////}
//
//} // namespace cco
//
//auto operator<(const cco::Record &lhs, const cco::Record &rhs) -> bool
//{
//    return cco::stob(lhs.key) < cco::stob(rhs.key);
//}
//
//auto operator>(const cco::Record &lhs, const cco::Record &rhs) -> bool
//{
//    return cco::stob(lhs.key) > cco::stob(rhs.key);
//}
//
//auto operator<=(const cco::Record &lhs, const cco::Record &rhs) -> bool
//{
//    return cco::stob(lhs.key) <= cco::stob(rhs.key);
//}
//
//auto operator>=(const cco::Record &lhs, const cco::Record &rhs) -> bool
//{
//    return cco::stob(lhs.key) >= cco::stob(rhs.key);
//}
//
//auto operator==(const cco::Record &lhs, const cco::Record &rhs) -> bool
//{
//    return cco::stob(lhs.key) == cco::stob(rhs.key);
//}
//
//auto operator!=(const cco::Record &lhs, const cco::Record &rhs) -> bool
//{
//    return cco::stob(lhs.key) != cco::stob(rhs.key);
//}
