#include "wal_manager.h"
#include "page/file_header.h"
#include "page/node.h"
#include "pool/interface.h"
#include "storage/interface.h"
#include "storage/system.h"
#include "utils/logging.h"
#include "wal_reader.h"
#include "wal_record.h"
#include "wal_writer.h"
#include <optional>

namespace cco {

namespace fs = std::filesystem;

auto WALManager::open(const WALParameters &param) -> Result<std::unique_ptr<IWALManager>>
{
    CCO_EXPECT_GE(param.page_size, MINIMUM_PAGE_SIZE);
    CCO_EXPECT_LE(param.page_size, MAXIMUM_PAGE_SIZE);
    CCO_EXPECT_TRUE(is_power_of_two(param.page_size));

    CCO_TRY_CREATE(writer, WALWriter::create(param));
    CCO_TRY_CREATE(reader, WALReader::create(param));
    auto manager = std::unique_ptr<WALManager> {new (std::nothrow) WALManager {param}};
    if (!manager) {
        ThreePartMessage message;
        message.set_primary("cannot open WAL manager");
        message.set_detail("out of memory");
        return Err {message.system_error()};
    }
    manager->m_home = &param.directory;
    manager->m_writer = std::move(writer);
    manager->m_reader = std::move(reader);

    // Get a sorted list of WAL segments.
    CCO_TRY_CREATE(children, param.directory.children());
    std::vector<WALSegment> segments;

    for (const auto &child: children) {
        const auto filename = fs::path {child}.filename().string();
        auto name = stob(filename);
        if (name.starts_with(stob(WAL_PREFIX))) {
            const auto id = name_to_id(name);
            if (id.is_null()) {
                ThreePartMessage message;
                message.set_primary("cannot open WAL manager");
                message.set_detail("segment name is corrupted");
                message.set_hint("invalid name is \"{}\"", child);
                return Err {message.corruption()};
            }

            WALSegment segment;
            segment.id = id;
            segments.emplace_back(segment);
        }
    }
    std::sort(begin(segments), end(segments), [](const auto &lhs, const auto &rhs) {
        return lhs.id <= rhs.id;
    });
    for (auto &segment: segments) {
        WALRecordPosition first;
        CCO_TRY(manager->open_reader_segment(segment.id));
        CCO_TRY_CREATE(record, manager->m_reader->read(first));
        CCO_TRY_STORE(segment.has_commit, manager->roll_forward(segment.positions));
        segment.start = record.lsn();
    }

    WALSegment current;
    current.id = SegmentId::base();
    current.start = SequenceNumber::base();
    if (!segments.empty()) {
        current.id = segments.back().id;
        current.start = segments.back().start;
        current.id++;
        current.start++;
    }
    CCO_TRY(manager->open_writer_segment(current.id));
    CCO_EXPECT_TRUE(manager->m_writer->is_open());
    manager->m_current = std::move(current);
    manager->m_segments = std::move(segments);
    return manager;
}

WALManager::WALManager(const WALParameters &param)
    : m_tracker {param.page_size},
      m_logger {create_logger(param.log_sink, "wal")},
      m_scratch(param.page_size * 4, '\x00'), // TODO: Find a sensible value for this. Really shouldn't need much more than a page at max, if even.
      m_pool {param.pool}
{}

auto WALManager::cleanup() -> Result<void>
{
    if (m_segments.empty())
        return {};

    auto limit = std::find_if(begin(m_segments), end(m_segments), [](const auto &segment) {
        return segment.has_commit;
    });
    for (auto itr = begin(m_segments); itr != limit; ++itr) // TODO: Design for reentrancy...
        CCO_TRY(m_home->remove_file(id_to_name(itr->id)));
    m_segments.erase(begin(m_segments), limit);
    return {};
}

auto WALManager::close() -> Result<void>
{
    Result<void> er, rr, wr;
    if (m_reader->is_open()) {
        rr = m_reader->close();
        if (!rr.has_value()) {
            m_logger->error("cannot close WAL reader");
            m_logger->error("(reason) {}", rr.error().what());
        }
    }

    if (m_writer->is_open()) {
        wr = m_writer->close();
        if (!wr.has_value()) {
            m_logger->error("cannot close WAL writer");
            m_logger->error("(reason) {}", wr.error().what());
        }
    }

    auto exists = m_home->exists(id_to_name(m_current.id));
    if (exists.has_value() && exists.value() && m_current.positions.empty())
        er = m_home->remove_file(id_to_name(m_current.id));

    // If both close() calls produced an error, we'll lose one of them. It'll end up in the
    // log though.
    return !rr ? rr : (!er ? er : wr);
}

auto WALManager::has_pending() const -> bool
{
    return m_has_pending;
}

auto WALManager::flushed_lsn() const -> SequenceNumber
{
    return m_writer->flushed_lsn();
}

auto WALManager::track(Page &page) -> void
{
    m_tracker.track(page);
}

auto WALManager::discard(Page &page) -> void
{
    m_tracker.discard(page);
}

auto WALManager::append(Page &page) -> Result<void>
{
    auto update = m_tracker.collect(page, ++m_writer->last_lsn());
    if (!update.changes.empty()) {
        CCO_TRY_CREATE(position, m_writer->append(WALRecord {update, stob(m_scratch)}));
        m_current.positions.emplace_back(position);
        m_has_pending = true;

        if (m_writer->needs_segmentation())
            return advance_writer(++update.lsn, false);
    }
    return {};
}

auto WALManager::truncate(SegmentId id) -> Result<void>
{
    (void)id;
    return {};//system::unlink(id_to_name(id));

//    m_tracker.reset();
//    CCO_TRY(m_writer->truncate());
//    m_positions.clear();
    return {};
}

auto WALManager::flush() -> Result<void>
{
    m_tracker.reset();
    return m_writer->flush();
}

auto WALManager::recover() -> Result<void>
{
    if (m_segments.empty())
        return {};

    auto segment = crbegin(m_segments);
    if (!segment->has_commit) {
        for (; segment != crend(m_segments) && !segment->has_commit; ++segment)
            CCO_TRY(undo_segment(*segment));

        CCO_TRY(m_pool->flush());
        for (auto itr = crbegin(m_segments); itr != segment; ++itr)
            CCO_TRY(m_home->remove_file(id_to_name(itr->id)));
        m_segments.erase(segment.base(), end(m_segments));
    }
    return {};
}

auto WALManager::undo_segment(const WALSegment &segment) -> Result<void>
{
    if (segment.positions.empty()) {
        LogMessage message {*m_logger};
        message.set_primary("cannot undo segment");
        message.set_detail("segment is empty");
        message.set_hint("segment ID is {}", segment.id.value);
        return Err {message.corruption()};
    }
    CCO_TRY(open_reader_segment(segment.id));
    CCO_TRY(roll_backward(segment.positions));
    return m_reader->close();
}

// TODO: Fixes our state if we fail trying to open or close a new segment file.
auto WALManager::ensure_initialized() -> Result<void>
{
    if (!m_writer->is_open())
        return open_writer_segment(m_current.id);
    return {};
}

auto WALManager::abort() -> Result<void>
{
    CCO_EXPECT_TRUE(m_has_pending);

    CCO_TRY(flush());

    if (!m_current.positions.empty())
        CCO_TRY(undo_segment(m_current));

    auto segment = crbegin(m_segments);
    for (; segment != crend(m_segments) && !segment->has_commit; ++segment)
        CCO_TRY(undo_segment(*segment));

    // TODO: Erase segments from vector and directory at the same time so we don't get mismatched in case of failure?
    for (auto itr = crbegin(m_segments); itr != segment; ++itr)
        CCO_TRY(m_home->remove_file(id_to_name(itr->id)));
    m_segments.erase(segment.base(), end(m_segments));
    m_has_pending = false;
    return {};
}

auto WALManager::commit() -> Result<void>
{
    // Skip the LSN that will be used for the file header updates.
    SequenceNumber commit_lsn {m_writer->last_lsn().value + 2};
    CCO_TRY_CREATE(root, m_pool->acquire(PageId::base(), true));
    auto header = get_file_header_writer(root);
    header.set_flushed_lsn(commit_lsn);
    header.update_header_crc();
    CCO_TRY(m_pool->release(std::move(root)));
    CCO_TRY(m_writer->append(WALRecord::commit(commit_lsn, stob(m_scratch))));
    CCO_TRY(flush());

    m_has_pending = false;

    // Only advance if we're not already in a new segment.
    if (m_writer->has_committed()) {
        m_current.has_commit = true;
        CCO_TRY(advance_writer(++commit_lsn, true));
    } else {
        m_segments.back().has_commit = true;
    }
    CCO_TRY(cleanup()); // TODO: Could go elsewhere, like in a background thread. We would likely need a bit of synchronization though.
    return {};
}

auto WALManager::advance_writer(SequenceNumber next_start, bool has_commit) -> Result<void>
{
    CCO_TRY(m_writer->flush());

    m_current.has_commit = has_commit;
    m_segments.emplace_back(m_current);

    m_current.positions.clear();
    m_current.id++;
    m_current.start = next_start;
    m_current.has_commit = false;
    return open_writer_segment(m_current.id);
}

auto WALManager::open_reader_segment(SegmentId id) -> Result<void>
{
    if (m_reader->is_open())
        CCO_TRY(m_reader->close());
    CCO_TRY_CREATE(file, m_home->open_file(id_to_name(id), Mode::READ_ONLY, DEFAULT_PERMISSIONS));
    return m_reader->open(std::move(file));
}

auto WALManager::open_writer_segment(SegmentId id) -> Result<void>
{
    if (m_writer->is_open())
        CCO_TRY(m_writer->close());
    CCO_TRY_CREATE(file, m_home->open_file(id_to_name(id), Mode::CREATE | Mode::WRITE_ONLY | Mode::APPEND, DEFAULT_PERMISSIONS));
    return m_writer->open(std::move(file));
}


// cleanup obsolete WAL segments
// start a new segment on commit or segment overflow
// roll back the currently running transaction
// roll back an incomplete xact on startup
// roll forward a complete xact on startup

auto WALManager::roll_forward(std::vector<WALRecordPosition> &positions) -> Result<bool>
{
    WALExplorer explorer {*m_reader};
    m_reader->reset();

    for (; ; ) {
        auto record = read_next(explorer, positions);
        if (!record.has_value()) {
            // We hit EOF but didn't find a commit record.
            if (record.error().is_not_found())
                break;
            return Err {record.error()};
        }

        if (m_writer->flushed_lsn() < record->lsn())
            m_writer->set_flushed_lsn(record->lsn());

        // Stop at the commit record. TODO: This should always be the last record in a given segment.
        if (record->is_commit())
            return true;

        const auto update = record->decode();
        CCO_TRY_CREATE(page, m_pool->fetch(update.page_id, true));
        CCO_EXPECT_FALSE(page.has_manager());

        if (page.lsn() < record->lsn())
            page.redo(record->lsn(), update.changes);

        CCO_TRY(m_pool->release(std::move(page)));
    }
    return false;
}

auto WALManager::roll_backward(const std::vector<WALRecordPosition> &positions) -> Result<void>
{
    auto itr = crbegin(positions);
    CCO_EXPECT_NE(itr, crend(positions));
    m_reader->reset();

    for (; itr != crend(positions); ++itr) {
        auto position = *itr;
        CCO_TRY_CREATE(record, m_reader->read(position));

        if (record.is_commit()) {
            if (itr != crbegin(positions)) { // TODO: Only valid at the end of the most recent segment?
                LogMessage message {*m_logger};
                message.set_primary("cannot roll backward");
                message.set_detail("encountered a misplaced commit record");
                message.set_hint("LSN is {}", record.lsn().value);
                return Err {message.corruption()};
            }
            continue;
        }

        const auto update = record.decode();
        CCO_TRY_CREATE(page, m_pool->fetch(update.page_id, true));
        CCO_EXPECT_FALSE(page.has_manager());
        CCO_EXPECT_EQ(record.lsn(), update.lsn);

        if (page.lsn() >= record.lsn())
            page.undo(update.previous_lsn, update.changes);

        CCO_TRY(m_pool->release(std::move(page)));
    }
    return {};
}

auto WALManager::save_header(FileHeaderWriter &) -> void
{

}

auto WALManager::load_header(const FileHeaderReader &) -> void
{

}

auto WALManager::read_next(WALExplorer &explorer, std::vector<WALRecordPosition> &positions) -> Result<WALRecord>
{
    static constexpr auto ERROR_PRIMARY = "cannot read record";

    auto discovery = explorer.read_next();
    if (discovery.has_value()) {
        positions.emplace_back(discovery->position);
        return discovery->record;
    }
    auto status = discovery.error();
    CCO_EXPECT_FALSE(status.is_ok());
    if (!status.is_not_found()) {
        m_logger->error(ERROR_PRIMARY);
        m_logger->error("(reason) {}", status.what());
    }
    return Err {status};
}

} // namespace cco