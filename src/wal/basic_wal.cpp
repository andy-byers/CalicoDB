#include "basic_wal.h"
#include "cleaner.h"
#include "utils/info_log.h"

namespace calico {

#define MAYBE_FORWARD(expr, message) \
    do { \
        const auto &calico_s = (expr); \
        if (!calico_s.is_ok()) return forward_status(calico_s, message); \
    } while (0)

static auto handle_worker_error(spdlog::logger &logger, const WalWriter &writer, const WalCleaner &cleaner)
{
    auto s = writer.status();
    if (!s.is_ok()) {
        logger.error("(1/2) background writer encountered an error");
        logger.error("(2/2) {}", s.what());
    }
    auto t = cleaner.status();
    if (!t.is_ok()) {
        logger.error("(1/2) background cleaner encountered an error");
        logger.error("(2/2) {}", t.what());
    }
    return s.is_ok() ? t : s;
}

static auto handle_not_started_error(spdlog::logger &logger, bool is_working, const std::string &primary)
{
    if (!is_working) {
        LogMessage message {logger};
        message.set_primary(primary);
        message.set_detail("background workers are not running");
        message.set_hint("start the background workers and try again");
        return message.logic_error();
    }
    return Status::ok();
}

BasicWriteAheadLog::BasicWriteAheadLog(const Parameters &param)
    : m_logger {create_logger(param.sink, "wal")},
      m_prefix {param.prefix},
      m_store {param.store},
      m_scratch {param.scratch},
      m_reader_data(wal_scratch_size(param.page_size), '\x00'),
      m_reader_tail(wal_block_size(param.page_size), '\x00'),
      m_writer_tail(wal_block_size(param.page_size), '\x00'),
      m_wal_limit {param.wal_limit}
{
    m_logger->info("constructing BasicWriteAheadLog object");
}

auto BasicWriteAheadLog::open(const Parameters &param, WriteAheadLog **out) -> Status
{
    static constexpr auto MSG = "cannot open write-ahead log";
    auto *temp = new (std::nothrow) BasicWriteAheadLog {param};

    if (!temp) {
        ThreePartMessage message;
        message.set_primary(MSG);
        message.set_detail("out of memory");
        return message.system_error();
    }
    std::vector<std::string> child_names;
    const auto path_prefix = param.prefix + WAL_PREFIX;
    auto s = param.store->get_children(param.prefix, child_names);
    if (!s.is_ok()) return s;

    std::vector<std::string> segment_names;
    std::copy_if(cbegin(child_names), cend(child_names), back_inserter(segment_names), [&path_prefix](const auto &path) {
        return BytesView {path}.starts_with(path_prefix) && path.size() - path_prefix.size() == SegmentId::DIGITS_SIZE;
    });

    std::vector<SegmentId> segment_ids;
    std::transform(cbegin(segment_names), cend(segment_names), back_inserter(segment_ids), [](const auto &name) {
        return SegmentId::from_name(BytesView {name});
    });
    std::sort(begin(segment_ids), end(segment_ids));

    // Keep track of the segment files.
    for (const auto &id: segment_ids)
        temp->m_collection.add_segment(id);

    *out = temp;
    return Status::ok();
}

BasicWriteAheadLog::~BasicWriteAheadLog()
{
    m_logger->info("destroying BasicWriteAheadLog object");

    // TODO: Move this into a close() method. We probably want to know if the writer shut down okay.

    if (m_is_working) {
        handle_worker_error(*m_logger, *m_writer, *m_cleaner);

        auto s = stop_workers_impl();
        forward_status(s, "cannot stop workers");
    }
}

auto BasicWriteAheadLog::worker_status() const -> Status
{
    if (m_is_working) {
        auto s = m_writer->status();
        if (s.is_ok())
            return m_cleaner->status();
        return s;
    }
    return Status::ok();
}

auto BasicWriteAheadLog::flushed_lsn() const -> SequenceId
{
    return m_flushed_lsn.load();
}

auto BasicWriteAheadLog::current_lsn() const -> SequenceId
{
    return ++SequenceId {m_last_lsn};
}

auto BasicWriteAheadLog::remove_before(SequenceId pager_lsn) -> Status
{
    if (m_is_working)
        m_cleaner->remove_before(pager_lsn);
    return m_cleaner->status();
}

#define HANDLE_WORKER_ERRORS \
    do { \
        auto s = handle_not_started_error(*m_logger, m_is_working, MSG); \
        MAYBE_FORWARD(s, MSG); \
        s = handle_worker_error(*m_logger, *m_writer, *m_cleaner); \
        MAYBE_FORWARD(s, MSG); \
    } while (0)


auto BasicWriteAheadLog::log(WalPayloadIn payload) -> Status
{
    static constexpr auto MSG = "could not log payload";
    HANDLE_WORKER_ERRORS;

    m_last_lsn++;

    m_writer->write(payload);
    return m_writer->status();
}

auto BasicWriteAheadLog::flush() -> Status
{
    m_logger->info("flushing tail buffer");
    static constexpr auto MSG = "could not flush";
    HANDLE_WORKER_ERRORS;

    m_writer->flush();
    return m_writer->status();
}

auto BasicWriteAheadLog::advance() -> Status
{
    m_logger->info("advancing to new segment");
    static constexpr auto MSG = "could not advance";
    HANDLE_WORKER_ERRORS;

    m_writer->advance();
    return m_writer->status();
}

#undef HANDLE_WORKER_ERRORS

auto BasicWriteAheadLog::stop_workers() -> Status
{
    return stop_workers_impl();
}

auto BasicWriteAheadLog::stop_workers_impl() -> Status
{
    // Stops the workers no matter what, even if an error is encountered. We should be able to call abort_last() safely after
    // this method returns.
    static constexpr auto MSG = "could not stop background writer";
    m_logger->info("received stop request");
    CALICO_EXPECT_TRUE(m_is_working);

    m_images.clear();
    m_is_working = false;

    auto s = std::move(*m_writer).destroy();
    m_writer.reset();

    auto t = std::move(*m_cleaner).destroy();
    m_cleaner.reset();

    MAYBE_FORWARD(s, MSG);
    MAYBE_FORWARD(t, MSG);

    m_logger->info("workers are stopped");
    return Status::ok();
}

auto BasicWriteAheadLog::start_workers() -> Status
{
    static constexpr auto MSG = "could not start workers";
    m_logger->info("received start request");
    CALICO_EXPECT_FALSE(m_is_working);

    auto s = open_writer();
    auto t = open_cleaner();

    if (s.is_ok() && t.is_ok()) {
        m_is_working = true;
        m_logger->info("workers are started");
    } else {
        m_writer.reset();
        m_cleaner.reset();
        MAYBE_FORWARD(s, MSG);
        MAYBE_FORWARD(t, MSG);
    }
    return s;
}

auto BasicWriteAheadLog::open_reader() -> Status
{
    m_reader.emplace(
        *m_store,
        m_collection,
        m_prefix,
        Bytes {m_reader_tail},
        Bytes {m_reader_data}
    );
    return m_reader->open();
}

auto BasicWriteAheadLog::open_writer() -> Status
{
    m_writer.emplace(
        *m_store,
        m_collection,
        *m_scratch,
        Bytes {m_writer_tail},
        m_flushed_lsn,
        m_prefix,
        m_wal_limit
    );
    return m_writer->open();
}

auto BasicWriteAheadLog::open_cleaner() -> Status
{
    m_cleaner.emplace(
        *m_store,
        m_prefix,
        m_collection
    );
    return Status::ok();
}

auto BasicWriteAheadLog::roll_forward(SequenceId begin_lsn, const Callback &callback) -> Status
{
    static constexpr auto MSG = "cannot roll forward";
    m_logger->info("received roll forward request");

    if (m_is_working) {
        auto s = stop_workers();
        MAYBE_FORWARD(s, MSG);
    }

    m_last_lsn = begin_lsn;
    m_flushed_lsn.store(m_last_lsn);

    if (m_collection.first().is_null())
        return Status::ok();

    // Open the reader on the first (oldest) WAL segment file.
    if (m_reader == std::nullopt) {
        auto s = open_reader();
        MAYBE_FORWARD(s, MSG);
    }

    auto s = Status::ok();
    while (s.is_ok()) {
        SequenceId first_lsn;
        s = m_reader->read_first_lsn(first_lsn);
        if (!s.is_ok()) break;

        s = m_reader->roll([&callback, begin_lsn, this](auto payload) {
            m_last_lsn = payload.lsn();
            if (m_last_lsn >= begin_lsn)
                return callback(payload);
            return Status::ok();
        });
        m_flushed_lsn.store(m_last_lsn);

        // We found an empty segment. This happens when the program aborted before the writer could either
        // write a block or delete the empty file. This is OK if we are on the last segment.
        if (s.is_not_found())
            s = Status::corruption(s.what());

        if (!s.is_ok()) {
            s = forward_status(s, "could not roll WAL forward");
            break;
        }
        s = m_reader->seek_next();
    }
    // Translate the error status if needed. Note that we allow an incomplete record at the end of the
    // most-recently-written segment.
    if (!s.is_ok()) {
        if (s.is_corruption()) {
            if (m_reader->segment_id() != m_collection.last())
                return s;
        } else if (!s.is_not_found()) {
            return s;
        }
        s = Status::ok();
    }
    return s;
}

auto BasicWriteAheadLog::roll_backward(SequenceId end_lsn, const Callback &callback) -> Status
{
    static constexpr auto MSG = "could not roll backward";
    m_logger->info("received roll backward request");

    if (m_is_working) {
        auto s = stop_workers();
        MAYBE_FORWARD(s, MSG);
    }

    if (m_collection.first().is_null())
        return Status::ok();

    if (m_reader == std::nullopt) {
        auto s = open_reader();
        MAYBE_FORWARD(s, MSG);
    }

    // Find the most-recent segment.
    for (; ; ) {
        auto s = m_reader->seek_next();
        if (s.is_not_found()) break;
        if (!s.is_ok()) return s;
    }

    auto s = Status::ok();
    for (Size i {}; s.is_ok(); i++) {
        SequenceId first_lsn;
        s = m_reader->read_first_lsn(first_lsn);

        if (s.is_ok()) {
            // Found the segment containing the most-recent commit.
            if (first_lsn <= end_lsn)
                break;

            // Read all full image records. We can read them forward, since the pages are disjoint
            // within each transaction.
            s = m_reader->roll(callback);
        } else if (s.is_not_found()) {
            // The segment file is empty.
            s = Status::corruption(s.what());
        }

        // Most-recent segment can have an incomplete record at the end.
        if (s.is_corruption() && i == 0)
            s = Status::ok();
        MAYBE_FORWARD(s, MSG);

        s = m_reader->seek_previous();
    }
    return s.is_not_found() ? Status::ok() : s;
}

auto BasicWriteAheadLog::remove_after(SequenceId limit) -> Status
{
    static constexpr auto MSG = "could not records remove after";

    if (m_is_working) {
        auto s = stop_workers();
        MAYBE_FORWARD(s, MSG);
    }

    auto last = m_collection.last();
    auto current = last;
    SegmentId target;

    while (!current.is_null()) {
        SequenceId first_lsn;
        auto s = read_first_lsn(
            *m_store, m_prefix, current, first_lsn);

        if (s.is_ok()) {
            if (first_lsn >= limit)
                break;
        } else if (!s.is_not_found()) {
            return s;
        }
        if (!target.is_null()) {
            CALICO_TRY(m_store->remove_file(m_prefix + target.to_name()));
            m_collection.remove_after(current);
        }
        target = current;
        current = m_collection.id_before(current);
    }
    return Status::ok();
}

#undef MAYBE_FORWARD

} // namespace calico