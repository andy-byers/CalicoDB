#include "basic_wal.h"
#include "utils/logging.h"

namespace calico {

auto BasicWriteAheadLog::open(const Parameters &param, WriteAheadLog **out) -> Status
{

}

auto BasicWriteAheadLog::flushed_lsn() const -> std::uint64_t
{

}

auto BasicWriteAheadLog::current_lsn() const -> std::uint64_t
{

}

auto BasicWriteAheadLog::log_image(std::uint64_t page_id, BytesView image) -> Status
{

}

auto BasicWriteAheadLog::log_deltas(std::uint64_t page_id, BytesView image, const std::vector<PageDelta> &deltas) -> Status
{

}

auto BasicWriteAheadLog::log_commit() -> Status
{

}

auto BasicWriteAheadLog::stop() -> Status
{

}

auto BasicWriteAheadLog::start() -> Status
{

}

auto BasicWriteAheadLog::redo_all(const RedoCallback &callback) -> Status
{

}

auto BasicWriteAheadLog::undo_last(const UndoCallback &callback) -> Status
{

}

auto BasicWriteAheadLog::save_state(FileHeader &) -> void
{

}

auto BasicWriteAheadLog::load_state(const FileHeader &) -> void
{

}


//namespace fs = std::filesystem;
//
//auto WALManager::open(const WALParameters &param) -> Result<std::unique_ptr<IWALManager>>
//{
//    CALICO_EXPECT_GE(param.page_size, MINIMUM_PAGE_SIZE);
//    CALICO_EXPECT_LE(param.page_size, MAXIMUM_PAGE_SIZE);
//    CALICO_EXPECT_TRUE(is_power_of_two(param.page_size));
//
//    CALICO_TRY_CREATE(writer, WALWriter::create(param));
//    CALICO_TRY_CREATE(reader, WALReader::create(param));
//    auto manager = std::unique_ptr<WALManager> {new (std::nothrow) WALManager {param}};
//    if (!manager) {
//        ThreePartMessage message;
//        message.set_primary("cannot open WAL manager");
//        message.set_detail("out of memory");
//        return Err {message.system_error()};
//    }
//    manager->m_home = &param.store;
//    manager->m_writer = std::move(writer);
//    manager->m_reader = std::move(reader);
//
//    //    // Get a sorted list of WAL segments.
//    //    CALICO_TRY_CREATE(children, param.directory.children());
//    //    std::vector<WALSegment> segments;
//    //
//    //    for (const auto &child: children) {
//    //        const auto filename = fs::path {child}.filename().string();
//    //        auto name = stob(filename);
//    //        if (name.starts_with(stob(WAL_PREFIX))) {
//    //            const auto id = name_to_id(name);
//    //            if (id.is_null()) {
//    //                ThreePartMessage message;
//    //                message.set_primary("cannot open WAL manager");
//    //                message.set_detail("segment name is corrupted");
//    //                message.set_hint("invalid name is \"{}\"", child);
//    //                return Err {message.corruption()};
//    //            }
//    //
//    //            WALSegment segment;
//    //            segment.id = id;
//    //            segments.emplace_back(segment);
//    //        }
//    //    }
//    //    std::sort(begin(segments), end(segments), [](const auto &lhs, const auto &rhs) {
//    //        return lhs.id <= rhs.id;
//    //    });
//    //    for (auto &segment: segments) {
//    //        WALRecordPosition first;
//    //        CALICO_TRY(manager->open_reader_segment(segment.id));
//    //        CALICO_TRY_CREATE(is_empty, manager->m_reader->is_empty());
//    //        if (!is_empty) {
//    //            CALICO_TRY_CREATE(record, manager->m_reader->read(first));
//    //            CALICO_TRY_STORE(segment.has_commit, manager->roll_forward(segment.positions));
//    //            segment.start = record.lsn();
//    //            manager->m_completed_segments.emplace_back(std::move(segment));
//    //        }
//    //    }
//    //
//    //    WALSegment current;
//    //    current.id = SegmentId::base();
//    //    current.start = SeqNum::base();
//    //    if (!manager->m_completed_segments.empty()) {
//    //        current.id = manager->m_completed_segments.back().id;
//    //        current.start = manager->m_completed_segments.back().start;
//    //        current.id++;
//    //        current.start++;
//    //    }
//    //
//    //
//    //
//    //
//    //    CALICO_TRY(manager->open_writer_segment(current.id));
//    //    CALICO_EXPECT_TRUE(manager->m_writer->is_open());
//    //    manager->m_current_segment = std::move(current);
//    //    return manager;
//
//    CALICO_TRY(manager->setup(param));
//    return manager;
//}
//
//WALManager::WALManager(const WALParameters &param)
//    : m_tracker {param.page_size},
//      m_logger {create_logger(param.log_sink, "wal")},
//      m_pool {param.pool},
//      m_record_scratch(param.page_size * IWALManager::SCRATCH_SCALE, '\x00')
//{}
//
//WALManager::~WALManager()
//{
//
//}
//
//auto WALManager::teardown() -> void
//{
//    {
//        std::lock_guard lock {m_queue_mutex};
//        m_queue.enqueue(ExitEvent {});
//        m_queue_cond.notify_one();
//    }
//    m_writer_task->join();
//}
//
//auto WALManager::setup(const WALParameters &param) -> Result<void>
//{
//    // Get a sorted list of WAL segments.
//    CALICO_TRY_CREATE(children, param.store.get_blob_names());
//    std::vector<WALSegment> segments;
//
//    for (const auto &child: children) {
//        const auto filename = fs::path {child}.filename().string();
//        auto name = stob(filename);
//        if (name.starts_with(stob(WAL_PREFIX))) {
//            const auto id = name_to_id(name);
//            if (id.is_null()) {
//                ThreePartMessage message;
//                message.set_primary("cannot setup WAL manager");
//                message.set_detail("segment name is corrupted");
//                message.set_hint("invalid name is \"{}\"", child);
//                return Err {message.corruption()};
//            }
//
//            WALSegment segment;
//            segment.id = id;
//            segments.emplace_back(segment);
//        }
//    }
//    std::sort(begin(segments), end(segments), [](const auto &lhs, const auto &rhs) {
//        return lhs.id <= rhs.id;
//    });
//    std::vector<WALSegment> filtered;
//    filtered.reserve(segments.size());
//    m_next_lsn = param.flushed_lsn;
//    m_next_lsn++;
//
//    for (auto &segment: segments) {
//        WALRecordPosition first;
//        CALICO_TRY(open_reader_segment(segment.id));
//        CALICO_TRY_CREATE(is_empty, m_reader->is_empty());
//        if (!is_empty) {
//            CALICO_TRY_CREATE(record, m_reader->read(first));
//            CALICO_TRY_STORE(segment.has_commit, roll_forward(segment.positions));
//            segment.start = record.page_lsn();
//            filtered.emplace_back(std::move(segment));
//        }
//    }
//
//    WALSegment current;
//    current.id = SegmentId::base();
//    current.start = SeqNum::base();
//    if (!filtered.empty()) {
//        current.id = filtered.back().id;
//        current.start = filtered.back().start;
//        current.id++;
//        current.start++;
//    }
//    m_completed_segments = std::move(filtered);
//    m_current_segment = std::move(current);
//
//    return spawn_writer();
//}
//
//#define WRITER_TRY(expr) \x
//    do { \x
//        if (auto writer_try_result = (expr); !writer_try_result.has_value()) { \x
//            m_writer_status = writer_try_result.error(); \x
//            return nullptr; \x
//        } \x
//    } while (0)
//
//// "name" must be a valid, unused, identifier.
//#define WRITER_TRY_CREATE(name, expr) \x
//    auto writer_try_##name = (expr); \x
//    if (!writer_try_##name.has_value()) { \x
//        m_writer_status = writer_try_##name.error(); \x
//        return nullptr; \x
//    } \x
//    auto name = *writer_try_##name;
//
//auto WALManager::spawn_writer() -> Result<void>
//{
//    if (!m_writer->is_open())
//        CALICO_TRY(open_writer_segment(m_current_segment.id));
//
//    m_writer_task = std::thread {[this] {
//        for (; ; ) {
//            std::unique_lock lock {m_queue_mutex};
//            writer_wait_on_event(lock);
//            auto event = m_queue.dequeue();
//            m_is_busy = true;
//            lock.unlock();
//
//            fmt::print(stderr, "Selecting event type...\n");
//
//            if (std::holds_alternative<AppendEvent>(event)) {
//
//                fmt::print(stderr, "AppendEvent\n");
//
//                auto update = std::get<0>(std::get<AppendEvent>(event).data);
//                WALRecord record {update, stob(m_record_scratch)};
//
//                WRITER_TRY_CREATE(position, m_writer->append(record))
//
//                m_current_segment.positions.emplace_back(position);
//                m_tracker.cleanup(update.page_id); // Internally synchronized.
//                m_has_pending = true;
//
//                if (m_writer->needs_segmentation())
//                    WRITER_TRY(advance_writer(++update.page_lsn, false));
//
//            } else if (std::holds_alternative<CommitEvent>(event)) {
//
//                fmt::print(stderr, "CommitEvent\n");
//
//                auto commit_lsn = std::get<0>(std::get<CommitEvent>(event).data);
//
//                WRITER_TRY(m_writer->append(WALRecord::commit(commit_lsn, stob(m_record_scratch))));
//                WRITER_TRY(m_writer->flush());
//
//                // Only advance if we're not already in a new segment.
//                if (m_writer->has_committed()) {
//                    m_current_segment.has_commit = true;
//                    WRITER_TRY(advance_writer(++commit_lsn, true));
//                } else {
//                    m_completed_segments.back().has_commit = true; // TODO: This needs synchronization probably...
//                }
//                WRITER_TRY(cleanup());
//
//            } else if (std::holds_alternative<AbortEvent>(event)) {
//
//                fmt::print(stderr, "AbortEvent\n");
//
//                WRITER_TRY(m_writer->flush());
//                WRITER_TRY(advance_writer(m_next_lsn, false));
//            }
//
////            writer_signal_manager();
//
//            if (std::holds_alternative<ExitEvent>(event)) {
//                WRITER_TRY(m_writer->flush());
//                WRITER_TRY(cleanup());
//                fmt::print(stderr, "ExitEvent\n");
//                break;
//            } else {
//                std::lock_guard guard {m_busy_mutex};
//                if (std::exchange(m_is_busy, false))
//                    m_busy_cond.notify_one();
//            }
//        }
//        return nullptr;
//    }};
//    return {};
//}
//
//#undef WRITER_TRY
//#undef WRITER_TRY_CREATE
//
//auto WALManager::writer_wait_on_event(std::unique_lock<std::mutex> &lock) -> void
//{
//    m_queue_cond.wait(lock, [this] {
//        return !m_queue.is_empty();
//    });
//}
//
//auto WALManager::manager_wait_on_writer(std::unique_lock<std::mutex> &lock) -> void
//{
//    m_busy_cond.wait(lock, [this] {
//        return !m_is_busy;
//    });
//}
//
//auto WALManager::writer_signal_manager() -> void
//{
//    std::lock_guard lock {m_queue_mutex};
//    m_is_busy = false;
//    m_busy_cond.notify_one();
//}
//
//auto WALManager::manager_signal_writer(PageUpdate update) -> void
//{
//    std::lock_guard lock {m_queue_mutex};
//    m_queue.enqueue(AppendEvent {std::move(update)});
//    m_queue_cond.notify_one();
//}
//
//auto WALManager::cleanup() -> Result<void>
//{
//    if (m_completed_segments.empty())
//        return {};
//
//    auto limit = std::find_if(begin(m_completed_segments), end(m_completed_segments), [](const auto &segment) {
//        return segment.has_commit;
//    });
//    for (auto itr = begin(m_completed_segments); itr != limit; ++itr) // TODO: Design for reentrancy...
//        CALICO_TRY(m_home->remove_file(id_to_name(itr->id)));
//    m_completed_segments.erase(begin(m_completed_segments), limit);
//    return {};
//}
//
//auto WALManager::close() -> Result<void>
//{
//    Result<void> er, wr;
//    CALICO_EXPECT_FALSE(m_reader->is_open());
//
//    if (m_writer->is_open()) {
//        wr = m_writer->close();
//        if (!wr.has_value()) {
//            m_logger->error("cannot close WAL writer");
//            m_logger->error("(reason) {}", wr.error().what());
//        }
//    }
//
//    auto exists = m_home->exists(id_to_name(m_current_segment.id));
//    if (exists.has_value() && exists.value() && m_current_segment.positions.empty())
//        er = m_home->remove_file(id_to_name(m_current_segment.id));
//
//    // If both close() calls produced an error, we'll lose one of them. It'll end up in the
//    // log though.
//    return !er ? er : wr;
//}
//
//auto WALManager::has_pending() const -> bool
//{
//    return m_has_pending;
//}
//
//auto WALManager::flushed_lsn() const -> SeqNum
//{
//    return m_writer->flushed_lsn();
//}
//
//auto WALManager::track(Page &page) -> void
//{
//    m_tracker.track(page);
//}
//
//auto WALManager::discard(Page &page) -> void
//{
//    m_tracker.discard(page);
//}
//
//auto WALManager::append(Page &page) -> Result<void>
//{
//    auto update = m_tracker.collect(page, m_next_lsn++);
//
//    if (!update.changes.empty()) {
//        m_has_pending = true; // Allows commit() to be called.
//        std::lock_guard lock {m_queue_mutex};
//        m_queue.enqueue(AppendEvent {std::move(update)});
//        m_queue_cond.notify_one();
//    }
//
//    return {}; // TODO: void?
//}
//
//auto WALManager::truncate(SegmentId id) -> Result<void>
//{
//    (void)id;
//    return {};//system::unlink(id_to_name(id));
//
////    m_tracker.reset();
////    CALICO_TRY(m_writer->truncate());
////    m_positions.clear();
//    return {};
//}
//
//auto WALManager::flush() -> Result<void>
//{
//    return m_writer->flush();
//}
//
//auto WALManager::recover() -> Result<void>
//{
//    if (m_completed_segments.empty())
//        return {};
//    {
//        std::lock_guard lock {m_queue_mutex};
//        m_is_busy = true;
//        m_queue.enqueue(ExitEvent {});
//    }
//
//    {
//        std::unique_lock lock {m_busy_mutex};
//        manager_wait_on_writer(lock);
//    }
//
//    auto segment = crbegin(m_completed_segments);
//    if (!segment->has_commit) {
//        for (; segment != crend(m_completed_segments) && !segment->has_commit; ++segment)
//            CALICO_TRY(undo_segment(*segment));
//
//        CALICO_TRY(m_pool->flush());
//        for (auto itr = crbegin(m_completed_segments); itr != segment; ++itr)
//            CALICO_TRY(m_home->remove_file(id_to_name(itr->id)));
//        m_completed_segments.erase(segment.base(), end(m_completed_segments));
//    }
//    return spawn_writer();
//}
//
//auto WALManager::undo_segment(const WALSegment &segment) -> Result<void>
//{
//    if (segment.positions.empty()) {
//        LogMessage message {*m_logger};
//        message.set_primary("cannot undo segment");
//        message.set_detail("segment is empty");
//        message.set_hint("segment ID is {}", segment.id.value);
//        return Err {message.corruption()};
//    }
//    CALICO_TRY(open_reader_segment(segment.id));
//    CALICO_TRY(roll_backward(segment.positions));
//    return m_reader->close();
//}
//
//// TODO: Fixes our state if we fail trying to open or file_close a new segment file.
//auto WALManager::ensure_initialized() -> Result<void>
//{
////    if (!m_writer->is_open())
////        return open_writer_segment(m_current_segment.id);
//    return {};
//}
//
//auto WALManager::abort() -> Result<void>
//{
//    CALICO_EXPECT_TRUE(m_has_pending);
//
//
//    {
//        std::lock_guard lock {m_queue_mutex};
//        m_is_busy = true;
//        m_queue.enqueue(AbortEvent {});
//    }
//
//    {
//        std::unique_lock lock {m_busy_mutex};
//        manager_wait_on_writer(lock);
//    }
//
//    m_has_pending = false;
//    CALICO_TRY(m_writer->flush());
//
//    if (!m_current_segment.positions.empty())
//        CALICO_TRY(undo_segment(m_current_segment));
//
//    auto segment = crbegin(m_completed_segments);
//    for (; segment != crend(m_completed_segments) && !segment->has_commit; ++segment)
//        CALICO_TRY(undo_segment(*segment));
//
//    // TODO: Erase segments from vector and directory at the same time so we don't get mismatched in case of failure?
//    for (auto itr = crbegin(m_completed_segments); itr != segment; ++itr)
//        CALICO_TRY(m_home->remove_file(id_to_name(itr->id)));
//    m_completed_segments.erase(segment.base(), end(m_completed_segments));
//    m_has_pending = false;
//    return spawn_writer();
//}
//
//auto WALManager::commit() -> Result<void>
//{
//    // Skip the LSN that will be used for the file header updates.
//    SeqNum commit_lsn {m_next_lsn.value + 2}; // TODO: Maybe "+ 1" now.
//    CALICO_TRY_CREATE(root, m_pool->acquire(PageId::base(), true));
//    auto header = get_file_header_writer(root);
//    header.set_flushed_lsn(commit_lsn);
//    header.update_header_crc();
//    CALICO_TRY(m_pool->release(std::move(root)));
//
//    std::lock_guard lock {m_queue_mutex};
//    m_has_pending = false;
//    m_queue.enqueue(CommitEvent {commit_lsn});
//    m_queue_cond.notify_one();
//    return {};
//}
//
//auto WALManager::advance_writer(SeqNum next_start, bool has_commit) -> Result<void>
//{
//    CALICO_TRY(m_writer->flush());
//    std::lock_guard lock {m_queue_mutex};
//
//    m_current_segment.has_commit = has_commit;
//    m_completed_segments.emplace_back(m_current_segment);
//
//    m_current_segment.positions.clear();
//    m_current_segment.id++;
//    m_current_segment.start = next_start;
//    m_current_segment.has_commit = false;
//    return open_writer_segment(m_current_segment.id);
//}
//
//auto WALManager::open_reader_segment(SegmentId id) -> Result<void>
//{
//    if (m_reader->is_open())
//        CALICO_TRY(m_reader->close());
//    CALICO_TRY_CREATE(file, m_home->open_file(id_to_name(id), Mode::READ_ONLY, DEFAULT_PERMISSIONS));
//    return m_reader->open(std::move(file));
//}
//
//auto WALManager::open_writer_segment(SegmentId id) -> Result<void>
//{
//    if (m_writer->is_open())
//        CALICO_TRY(m_writer->close());
//    CALICO_TRY_CREATE(file, m_home->open_file(id_to_name(id), Mode::CREATE | Mode::WRITE_ONLY | Mode::APPEND, DEFAULT_PERMISSIONS));
//    return m_writer->open(std::move(file));
//}
//
//
//// cleanup obsolete WAL segments
//// start a new segment on commit or segment overflow
//// roll back the currently running transaction
//// roll back an incomplete xact on startup
//// roll forward a complete xact on startup
//
//auto WALManager::roll_forward(std::vector<WALRecordPosition> &positions) -> Result<bool>
//{
//    WALExplorer explorer {*m_reader};
//    m_reader->reset();
//
//    for (; ; ) {
//        auto record = read_next(explorer, positions);
//        if (!record.has_value()) {
//            // We hit EOF but didn't find a commit record.
//            if (record.error().is_not_found())
//                break;
//            return Err {record.error()};
//        }
//
//        if (m_writer->flushed_lsn() < record->page_lsn()) {
//            m_writer->set_flushed_lsn(record->page_lsn());
//            m_next_lsn = ++record->page_lsn();
//        }
//
//        // Stop at the commit record. TODO: This should always be the last record in a given segment.
//        if (record->is_commit())
//            return true;
//
//        const auto update = record->decode();
//        CALICO_TRY_CREATE(page, m_pool->fetch(update.page_id, true));
//        CALICO_EXPECT_FALSE(page.has_manager());
//
//        if (page.page_lsn() < record->page_lsn())
//            page.redo(record->page_lsn(), update.changes);
//
//        CALICO_TRY(m_pool->release(std::move(page)));
//    }
//    return false;
//}
//
//auto WALManager::roll_backward(const std::vector<WALRecordPosition> &positions) -> Result<void>
//{
//    auto itr = crbegin(positions);
//    CALICO_EXPECT_NE(itr, crend(positions));
//    m_reader->reset();
//
//    for (; itr != crend(positions); ++itr) {
//        auto position = *itr;
//        CALICO_TRY_CREATE(record, m_reader->read(position));
//
//        if (record.is_commit()) {
//            if (itr != crbegin(positions)) { // TODO: Only valid at the end of the most recent segment?
//                LogMessage message {*m_logger};
//                message.set_primary("cannot roll backward");
//                message.set_detail("encountered a misplaced commit record");
//                message.set_hint("LSN is {}", record.page_lsn().value);
//                return Err {message.corruption()};
//            }
//            continue;
//        }
//
//        const auto update = record.decode();
//        CALICO_TRY_CREATE(page, m_pool->fetch(update.page_id, true));
//        CALICO_EXPECT_FALSE(page.has_manager());
//        CALICO_EXPECT_EQ(record.page_lsn(), update.page_lsn);
//
//        if (page.page_lsn() >= record.page_lsn())
//            page.undo(update.last_lsn, update.changes);
//
//        CALICO_TRY(m_pool->release(std::move(page)));
//    }
//    return {};
//}
//
//auto WALManager::save_header(FileHeaderWriter &) -> void
//{
//
//}
//
//auto WALManager::load_header(const FileHeaderReader &) -> void
//{
//
//}
//
//auto WALManager::read_next(WALExplorer &explorer, std::vector<WALRecordPosition> &positions) -> Result<WALRecord>
//{
//    static constexpr auto ERROR_PRIMARY = "cannot read record";
//
//    auto discovery = explorer.read_next();
//    if (discovery.has_value()) {
//        positions.emplace_back(discovery->position);
//        return discovery->record;
//    }
//    auto status = discovery.error();
//    CALICO_EXPECT_FALSE(status.is_ok());
//    if (!status.is_not_found()) {
//        m_logger->error(ERROR_PRIMARY);
//        m_logger->error("(reason) {}", status.what());
//    }
//    return Err {status};
//}

} // namespace calico