#include "basic_wal.h"
#include "utils/logging.h"

namespace calico {

[[nodiscard]]
auto not_started_error(spdlog::logger &logger, const std::string &primary)
{
    LogMessage message {logger};
    message.set_primary(primary);
    message.set_detail("background writer is not running");
    message.set_hint("start the background writer and try again");
    return message.logic_error();
}

BasicWriteAheadLog::BasicWriteAheadLog(const Parameters &param)
    : m_logger {create_logger(param.sink, "wal")},
      m_dirname {param.dir},
      m_store {param.store},
      m_reader {
          *m_store,
          param.dir,
          param.page_size
      },
      m_writer {{
          m_store,
          &m_collection,
          &m_flushed_lsn,
          param.dir,
          param.page_size,
      }}
{
    m_logger->info("constructing BasicWriteAheadLog object");
}

[[nodiscard]]
auto find_last_lsn(BasicWalReader &reader, std::vector<SegmentId> &segments, SequenceId &last_lsn)
{
    for (auto itr = crbegin(segments); itr != crend(segments); ++itr) {
        auto s = reader.open(*itr);
        if (s.is_logic_error()) continue;
        if (!s.is_ok()) return s;

        std::vector<RecordPosition> positions;
        s = reader.redo(positions, [&last_lsn](const auto &descriptor) {
            last_lsn.value = descriptor.page_lsn;
            return Status::ok();
        });
        if (positions.empty()) continue;
        if (!s.is_ok()) return s;

        // Get rid of empty segments at the end.
        segments.erase(itr.base(), end(segments));
        break;
    }
    return reader.close();
}

auto BasicWriteAheadLog::open(const Parameters &param, WriteAheadLog **out) -> Status
{
    auto *temp = new (std::nothrow) BasicWriteAheadLog {param};

    if (!temp) {
        ThreePartMessage message;
        message.set_primary("cannot open write-ahead log");
        message.set_detail("out of memory");
        return message.system_error();
    }
    std::vector<std::string> child_names;
    const auto prefix = fmt::format("{}/{}", param.dir, WAL_PREFIX);
    auto s = param.store->get_children(param.dir, child_names);

    std::vector<std::string> segment_names;
    std::copy_if(cbegin(child_names), cend(child_names), back_inserter(segment_names), [prefix](const auto &path) {
        return stob(path).starts_with(prefix) && path.size() - prefix.size() == SegmentId::DIGITS_SIZE;
    });

    std::vector<SegmentId> segment_ids;
    std::transform(cbegin(segment_names), cend(segment_names), back_inserter(segment_ids), [](const auto &name) {
        return SegmentId::from_name(stob(name));
    });

    std::sort(begin(segment_ids), end(segment_ids));
    for (const auto &id: segment_ids) {
        s = temp->m_reader.open(id);
        if (s.is_logic_error()) continue;
        if (!s.is_ok()) return s;

        SequenceId first_lsn;
        s = temp->m_reader.read_first_lsn(first_lsn);
        if (!s.is_ok()) return s;

        bool has_commit {};
        std::vector<RecordPosition> positions;
        s = temp->m_reader.redo(positions, [&](const auto &info) {
            temp->m_flushed_lsn.store(SequenceId {info.page_lsn});
            has_commit = info.is_commit;
            return Status::ok();
        });
        if (!s.is_ok()) return s;

        s = temp->m_reader.close();
        if (!s.is_ok()) return s;

        temp->m_collection.start_segment(id, first_lsn);
        temp->m_collection.finish_segment(has_commit);
    }

    SequenceId last_lsn;
    s = find_last_lsn(temp->m_reader, segment_ids, last_lsn);
    if (!s.is_ok()) return s;
    temp->m_last_lsn = last_lsn;
    temp->m_flushed_lsn.store(last_lsn);

    *out = temp;
    return Status::ok();
}

BasicWriteAheadLog::~BasicWriteAheadLog()
{
    m_logger->info("destroying BasicWriteAheadLog object");

    const auto s = m_writer.stop();
    if (!s.is_ok()) {
        m_logger->error("cannot stop WAL writer");
        m_logger->error("(reason) {}", s.what());
    }
}

auto BasicWriteAheadLog::flushed_lsn() const -> std::uint64_t
{
    return m_flushed_lsn.load().value;
}

auto BasicWriteAheadLog::current_lsn() const -> std::uint64_t
{
    return m_last_lsn.value + 1;
}

auto BasicWriteAheadLog::log_image(std::uint64_t page_id, BytesView image) -> Status
{
    if (!m_writer.is_running()) return not_started_error(*m_logger, "could not log full image");

    // Skip writing this full image if one has already been written for this page during this transaction. If so, we can
    // just use the old one to undo changes made to this page during the entire transaction.
    const auto itr = m_image_ids.find(PageId {page_id});
    if (itr != cend(m_image_ids)) {
        m_logger->info("skipping full image for page {}", page_id);
        return Status::ok();
    }

    m_last_lsn++;
    m_logger->info("logging full image for page {} (LSN = {})", page_id, m_last_lsn.value);
    m_writer.log_full_image(m_last_lsn, PageId {page_id}, image);
    m_image_ids.emplace(PageId {page_id});
    return m_writer.status();
}

auto BasicWriteAheadLog::log_deltas(std::uint64_t page_id, BytesView image, const std::vector<PageDelta> &deltas) -> Status
{
    if (!m_writer.is_running()) return not_started_error(*m_logger, "could not log deltas");

    m_last_lsn++;
    m_logger->info("logging deltas for page {} (LSN = {})", page_id, m_last_lsn.value);
    m_writer.log_deltas(m_last_lsn, PageId {page_id}, image, deltas);
    return m_writer.status();
}

auto BasicWriteAheadLog::log_commit() -> Status
{
    if (!m_writer.is_running()) return not_started_error(*m_logger, "could not log commit");

    m_last_lsn++;
    m_logger->info("logging commit (LSN = {})", m_last_lsn.value);
    m_writer.log_commit(m_last_lsn);
    m_image_ids.clear();
    return m_writer.status();
}

auto BasicWriteAheadLog::stop_writer() -> Status
{
    m_logger->info("received stop request");
    auto s = m_writer.stop();
    if (s.is_ok()) {
        m_logger->info("background writer is stopped");
    } else {
        m_logger->error("could not stop background writer");
        m_logger->error("(reason) {}", s.what());
    }
    m_image_ids.clear();
    return s;
}

auto BasicWriteAheadLog::start_writer() -> Status
{
    m_logger->info("received start request: next segment ID is {}", m_collection.most_recent_id().value);

    auto s = m_writer.start();
    if (s.is_ok()) {
        m_logger->info("background writer is started");
    } else {
        m_logger->error("could not start background writer");
        m_logger->error("(reason) {}", s.what());
    }
    return s;
}

auto BasicWriteAheadLog::redo_all(const RedoCallback &callback) -> Status
{
    m_logger->info("received redo request");
    std::vector<RecordPosition> uncommitted;
    for (auto itr = cbegin(m_collection.map()); itr != cend(m_collection.map()); ++itr) {
        const auto [id, meta] = *itr;
        m_logger->info("rolling segment {} forward", id.value);

        auto s = m_reader.open(id);
        if (!s.is_ok()) {
            // Allow segments to be empty.
            if (s.is_logic_error())
                continue;
            return s;
        }

        bool found_commit {};
        s = m_reader.redo(uncommitted, [&](const auto &info) {
            found_commit = info.is_commit;
            return callback(info);
        });
        if (!s.is_ok()) return s;

        if (found_commit)
            uncommitted.clear();
    }
    m_logger->info("finished redo");
    return Status::ok();
}

auto BasicWriteAheadLog::undo_last(const UndoCallback &callback) -> Status
{
    m_logger->info("received undo request");

    auto s = Status::ok();
    SegmentId obsolete;

    for (auto itr = crbegin(m_collection.map()); itr != crend(m_collection.map()); itr++) {
        const auto [id, meta] = *itr;
        m_logger->info("rolling segment {} backward", id.value);
        if (meta.has_commit) break;

        s = m_reader.open(id);
        if (s.is_logic_error()) continue;
        if (!s.is_ok()) return s;
        std::vector<RecordPosition> positions;

        // TODO: Would be nice to avoid this by saving the positions...
        s = m_reader.redo(positions, [](auto) {return Status::ok();});
        if (!s.is_ok()) return s;

        s = m_reader.undo(crbegin(positions), crend(positions), [&callback](const auto &info) {
            return callback(info);
        });
        if (!s.is_ok()) return s;

        s = m_reader.close();
        if (!s.is_ok()) return s;

        obsolete = id;
    }

    // Remove obsolete WAL segments.
    s = m_collection.remove_segments_from_right(obsolete, [this](auto id, auto meta) {
        CALICO_EXPECT_FALSE(meta.has_commit);
        return m_store->remove_file(m_dirname + id.to_name());
    });
    m_logger->info("finished undo");
    return s;
}

auto BasicWriteAheadLog::save_state(FileHeader &) -> void
{
    // TODO: No state needed yet...
}

auto BasicWriteAheadLog::load_state(const FileHeader &) -> void
{
    // TODO: No state needed yet...
}

} // namespace calico