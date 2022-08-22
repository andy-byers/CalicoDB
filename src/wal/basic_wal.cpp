#include "basic_wal.h"
#include "utils/logging.h"

namespace calico {

#define MAYBE_FORWARD(expr, message) \
    do { \
        const auto &calico_s = (expr); \
        if (!calico_s.is_ok()) return forward_status(calico_s, message); \
    } while (0)

[[nodiscard]]
auto is_writer_ready(const BasicWalWriter &writer)
{
    return writer.is_running() && writer.status().is_ok();
}

[[nodiscard]]
auto writer_error(spdlog::logger &logger, const BasicWalWriter &writer, const std::string &primary)
{
    if (!writer.status().is_ok()) {
        logger.error(primary);
        logger.error("(reason) {}", writer.status().what());
        return writer.status();
    }
    LogMessage message {logger};
    message.set_primary(primary);
    message.set_detail("background writer is not running");
    message.set_hint("start the background writer and try again");
    return message.logic_error();
}

BasicWriteAheadLog::BasicWriteAheadLog(const Parameters &param)
    : m_logger {create_logger(param.sink, "wal")},
      m_prefix {param.prefix},
      m_store {param.store},
      m_reader {
          *m_store,
          param.prefix,
          param.page_size
      },
      m_writer {{
          m_store,
          &m_collection,
          &m_flushed_lsn,
          param.prefix,
          param.page_size,
      }}
{
    m_logger->info("constructing BasicWriteAheadLog object");
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
    if (!is_writer_ready(m_writer)) return writer_error(*m_logger, m_writer, "could not log full image");

    // Skip writing this full image if one has already been written for this page during this transaction. If so, we can
    // just use the old one to undo changes made to this page during the entire transaction.
    const auto itr = m_images.find(PageId {page_id});
    if (itr != cend(m_images)) {
        m_logger->info("skipping full image for page {}", page_id);
        return Status::ok();
    }
    m_last_lsn++;

    m_logger->info("logging full image for page {} (LSN = {})", page_id, m_last_lsn.value);
    m_writer.log_full_image(m_last_lsn, PageId {page_id}, image);
    m_images.emplace(PageId {page_id});
    return m_writer.status();
}

auto BasicWriteAheadLog::log_deltas(std::uint64_t page_id, BytesView image, const std::vector<PageDelta> &deltas) -> Status
{
    if (!is_writer_ready(m_writer)) return writer_error(*m_logger, m_writer, "could not log deltas");

    m_last_lsn++;
    m_logger->info("logging deltas for page {} (LSN = {})", page_id, m_last_lsn.value);
    m_writer.log_deltas(m_last_lsn, PageId {page_id}, image, deltas);
    return m_writer.status();
}

auto BasicWriteAheadLog::log_commit() -> Status
{
    if (!is_writer_ready(m_writer)) return writer_error(*m_logger, m_writer, "could not log commit");

    m_last_lsn++;
    m_logger->info("logging commit (LSN = {})", m_last_lsn.value);
    m_writer.log_commit(m_last_lsn);
    m_images.clear();
    return m_writer.status();
}

auto BasicWriteAheadLog::stop_writer() -> Status
{
    m_logger->info("received stop request");
    auto s = m_writer.stop();
    MAYBE_FORWARD(s, "could not stop background writer");
    m_logger->info("background writer is stopped");
    m_images.clear();
    return s;
}

auto BasicWriteAheadLog::start_writer() -> Status
{
    m_logger->info("received start request: next segment ID is {}", m_collection.most_recent_id().value);

    auto s = m_writer.start();
    MAYBE_FORWARD(s, "could not start background writer");
    m_logger->info("background writer is started");
    return s;
}

auto BasicWriteAheadLog::setup_and_recover(const RedoCallback &redo_cb, const UndoCallback &undo_cb) -> Status
{
    static constexpr auto MSG = "could not recovery";
    m_logger->info("received recovery request");

    std::vector<std::string> child_names;
    const auto path_prefix = m_prefix + WAL_PREFIX;
    auto s = m_store->get_children(m_prefix, child_names);
    MAYBE_FORWARD(s, MSG);

    // TODO: Not a great way to validate paths...
    std::vector<std::string> segment_names;
    std::copy_if(cbegin(child_names), cend(child_names), back_inserter(segment_names), [&path_prefix](const auto &path) {
        return stob(path).starts_with(path_prefix) && path.size() - path_prefix.size() == SegmentId::DIGITS_SIZE;
    });

    std::vector<SegmentId> segment_ids;
    std::transform(cbegin(segment_names), cend(segment_names), back_inserter(segment_ids), [](const auto &name) {
        return SegmentId::from_name(stob(name));
    });
    std::sort(begin(segment_ids), end(segment_ids));

    std::vector<RecordPosition> uncommitted;
    for (const auto id: segment_ids) {
        m_logger->info("rolling segment {} forward", id.value);

        s = m_reader.open(id);
        // Allow segments to be empty.
        if (s.is_logic_error())
            continue;
        MAYBE_FORWARD(s, MSG);

        m_collection.start_segment(id);

        bool has_commit {};
        s = m_reader.redo(uncommitted, [&](const auto &info) {
            m_last_lsn.value = info.page_lsn;
            has_commit = info.is_commit;
            return redo_cb(info);
        });
        if (!s.is_ok()) {
            s = forward_status(s, "could not roll WAL forward");
            m_collection.abort_segment();
            break;
        }

        if (has_commit)
            uncommitted.clear();

        m_collection.finish_segment(has_commit);
    }

    m_flushed_lsn.store(m_last_lsn);

    for (auto itr = crbegin(uncommitted); itr != crend(uncommitted); ) {
        m_logger->info("rolling segment {} backward", itr->id.value);

        const auto end = std::find_if(next(itr), crend(uncommitted), [itr](const auto &position) {
            return position.id != itr->id;
        });
        s = m_reader.open(itr->id);
        if (s.is_logic_error()) continue;
        MAYBE_FORWARD(s, MSG);

        s = m_reader.undo(itr, end, [&undo_cb](const auto &info) {
            return undo_cb(info);
        });
        MAYBE_FORWARD(s, MSG);

        s = m_reader.close();
        MAYBE_FORWARD(s, MSG);
        itr = end;
    }

    if (!uncommitted.empty()) {
        s = m_collection.remove_segments_from_right(uncommitted.front().id, [this](const auto &info) {
            CALICO_EXPECT_FALSE(info.has_commit);
            return m_store->remove_file(m_prefix + info.id.to_name());
        });
    }

    m_logger->info("finished recovery");
    return s;
}

auto BasicWriteAheadLog::abort_last(const UndoCallback &callback) -> Status
{
    static constexpr auto MSG = "could not undo last";
    m_logger->info("received undo request");

    auto s = Status::ok();
    SegmentId obsolete;

    for (auto itr = crbegin(m_collection.set()); itr != crend(m_collection.set()); itr++) {
        const auto [id, has_commit] = *itr;
        m_logger->info("rolling segment {} backward", id.value);

        if (has_commit) {
            LogMessage message {*m_logger};
            message.set_primary("finished rolling backward");
            message.set_detail("found segment containing commit record");
            message.log(spdlog::level::info);
            break;
        }

        s = m_reader.open(id);
        if (s.is_logic_error()) {
            LogMessage message {*m_logger};
            message.set_primary("skipping segment");
            message.set_detail("segment is empty");
            message.log(spdlog::level::info);
            continue;
        }
        MAYBE_FORWARD(s, MSG);
        std::vector<RecordPosition> positions;

        // TODO: Would be nice to avoid this by saving the positions...
        s = m_reader.redo(positions, [](auto) {return Status::ok();});
        MAYBE_FORWARD(s, MSG);

        s = m_reader.undo(crbegin(positions), crend(positions), [&callback](const auto &info) {
            return callback(info);
        });
        MAYBE_FORWARD(s, MSG);

        s = m_reader.close();
        MAYBE_FORWARD(s, MSG);

        obsolete = id;
    }
    if (obsolete.is_null()) return s;

    // Remove obsolete WAL segments.
    s = m_collection.remove_segments_from_right(obsolete, [this](auto info) {
        CALICO_EXPECT_FALSE(info.has_commit);
        return m_store->remove_file(m_prefix + info.id.to_name());
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

#undef MAYBE_FORWARD

} // namespace calico