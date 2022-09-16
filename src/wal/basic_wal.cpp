#include "basic_wal.h"
#include "cleaner.h"
#include "utils/logging.h"

namespace calico {

#define MAYBE_FORWARD(expr, message) \
    do { \
        const auto &calico_s = (expr); \
        if (!calico_s.is_ok()) return forward_status(calico_s, message); \
    } while (0)

[[nodiscard]] static auto handle_worker_error(spdlog::logger &logger, const WalWriter &writer, /*const BasicWalCleaner &,*/ Status &out)
{
    auto s = writer.status();
    if (!s.is_ok()) {
        logger.error("(1/2) background writer encountered an error");
        logger.error("(2/2) {}", s.what());
        out = s;
    }
//    s = cleaner.status();
//    if (!s.is_ok()) {
//        logger.error("(1/2) background cleaner encountered an error");
//        logger.error("(2/2) {}", s.what());
//        if (out.is_ok()) out = s;
//    }
    return s;
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
      m_scratch {wal_scratch_size(param.page_size)},
      m_prefix {param.prefix},
      m_store {param.store},
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
        auto s = handle_worker_error(*m_logger, *m_writer/*, *m_cleaner*/, m_status);
        forward_status(s, "cannot clean up before close");

        s = stop_workers_impl();
        forward_status(s, "cannot stop workers");
    }
}

auto BasicWriteAheadLog::status() const -> Status
{
    if (m_is_working) {
        auto s = m_writer->status();
        if (!s.is_ok()) return s;

//        s = m_cleaner->status();
//        if (!s.is_ok()) return s;
    }
    return m_status;
}

auto BasicWriteAheadLog::flushed_lsn() const -> std::uint64_t
{
    return m_flushed_lsn.load().value;
}

auto BasicWriteAheadLog::current_lsn() const -> std::uint64_t
{
    CALICO_EXPECT_TRUE(m_is_working);
    return m_last_lsn.value + 1;
}

auto BasicWriteAheadLog::allow_cleanup(std::uint64_t pager_lsn) -> void
{
    (void)pager_lsn; // TODO: Cleanup needs help. Write unit tests!
//    if (m_is_working)
//        m_cleaner->dispatch(SequenceId {pager_lsn});
}

#define HANDLE_WORKER_ERRORS \
    do { \
        auto s = handle_not_started_error(*m_logger, m_is_working, MSG); \
        MAYBE_FORWARD(s, MSG); \
        s = handle_worker_error(*m_logger, *m_writer/*, *m_cleaner*/, m_status); \
        MAYBE_FORWARD(s, MSG); \
    } while (0)

auto BasicWriteAheadLog::log(std::uint64_t page_id, BytesView image) -> Status
{
    static constexpr auto MSG = "could not log full image";
    HANDLE_WORKER_ERRORS;

    // Skip writing this full image if one has already been written for this page during this transaction. If so, we can
    // just use the old one to undo changes made to this page during the entire transaction. We also need to make sure the
    // full images form a disjoint set that covers all changed pages. This lets us read a segment forward to undo changes.
    const auto itr = m_images.find(PageId {page_id});
    if (itr != cend(m_images)) {
        m_logger->info("skipping full image for page {}", page_id);
        return Status::ok();
    }

    auto payload = m_scratch.get();
    const auto size = encode_full_image_payload(++m_last_lsn, PageId {page_id}, image, *payload);
    payload->truncate(size);

    m_writer->write(m_last_lsn, payload);
    m_images.emplace(PageId {page_id});
    return m_writer->status();
}

auto BasicWriteAheadLog::log(std::uint64_t page_id, BytesView image, const std::vector<PageDelta> &deltas) -> Status
{
    static constexpr auto MSG = "could not log deltas";
    HANDLE_WORKER_ERRORS;

    auto payload = m_scratch.get();
    const auto size = encode_deltas_payload(++m_last_lsn, PageId {page_id}, image, deltas, *payload);
    payload->truncate(size);

    m_writer->write(m_last_lsn, payload);
    return m_writer->status();
}

auto BasicWriteAheadLog::commit() -> Status
{
    m_logger->info("logging commit");

    static constexpr auto MSG = "could not log commit";
    HANDLE_WORKER_ERRORS;

    auto payload = m_scratch.get();
    const auto size = encode_commit_payload(++m_last_lsn, *payload);
    payload->truncate(size);

    m_writer->write(m_last_lsn, payload);
    m_writer->advance();
    m_images.clear();

    // This reflects an updated status, since advance() blocks until the background worker is finished.
    auto s = m_writer->status();
    if (s.is_ok())
        m_commit_id = m_collection.last();
    return s;
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
    //    auto t = std::move(*m_cleaner).destroy();
//    m_cleaner.reset();

    if (m_status.is_ok())
        m_status = s;//s.is_ok() ? t : s;

    MAYBE_FORWARD(s, MSG);
//    MAYBE_FORWARD(t, MSG);

    m_logger->info("background writer is stopped");
    return s;
}

auto BasicWriteAheadLog::start_workers() -> Status
{
    static constexpr auto MSG = "could not start workers";
    m_logger->info("received start request: next segment ID is {}", m_collection.last().value);
    CALICO_EXPECT_FALSE(m_is_working);

    m_writer.emplace(
        *m_store,
        m_collection,
        m_scratch,
        Bytes {m_writer_tail},
        m_flushed_lsn,
        m_prefix,
        m_wal_limit
    );

//    m_cleaner.emplace(
//        *m_store,
//        m_prefix,
//        m_collection,
//        m_reader
//    );

    auto s = m_writer->open();
    auto t = Status::ok(); // m_cleaner->open();
    if (s.is_ok() && t.is_ok()) {
        m_is_working = true;
        m_logger->info("workers are started");
    } else {
        m_writer.reset();
//        m_cleaner.reset();
        MAYBE_FORWARD(s, MSG);
        MAYBE_FORWARD(t, MSG);
    }
    return s;
}

/*
 * First recovery phase. Here, we roll the WAL forward to apply any missing updates. Then, if we are missing a commit record for the
 * most-recent transaction, we roll that transaction back. In this case, we should keep the aborted segments around until all dirty
 * data pages have been flushed to disk.
 */
auto BasicWriteAheadLog::start_recovery(const GetDeltas &delta_cb, const GetFullImage &image_cb) -> Status
{
    static constexpr auto MSG = "cannot recover";
    m_logger->info("received recovery request");
    CALICO_EXPECT_FALSE(m_is_working);

    if (m_collection.first().is_null())
        return Status::ok();

    m_reader.emplace(
        *m_store,
        m_collection,
        m_prefix,
        Bytes {m_reader_tail},
        Bytes {m_reader_data}
    );

    // Open the reader on the first (oldest) WAL segment file.
    auto s = m_reader->open();
    MAYBE_FORWARD(s, MSG);

    while (s.is_ok()) {
        bool has_commit {};
        SequenceId last_lsn;

        s = m_reader->roll([&](const PayloadDescriptor &info) {
            if (std::holds_alternative<DeltasDescriptor>(info)) {
                const auto deltas = std::get<DeltasDescriptor>(info);
                if (deltas.lsn > m_pager_lsn.load(std::memory_order_relaxed)) {
                    last_lsn = deltas.lsn;
                    return delta_cb(deltas);
                }
            } else if (std::holds_alternative<CommitDescriptor>(info)) {
                const auto commit = std::get<CommitDescriptor>(info);
                last_lsn = commit.lsn;
                m_commit_id = m_reader->segment_id();
                has_commit = true;
            }
            return Status::ok();
        });
        if (!s.is_ok()) {
            s = forward_status(s, "could not roll WAL forward");
            break;
        }
        m_flushed_lsn.store(last_lsn);
        s = m_reader->seek_next();
    }
    if (m_commit_id != m_collection.last())
        return start_abort(image_cb);
    return s;
}

/*
 * Final recovery phase. Here, we remove WAL segments belonging to the aborted transaction, if present. This method must not be run
 * unless the pager is able to flush all dirty pages to disk.
 */
auto BasicWriteAheadLog::finish_recovery() -> Status
{
    // Most-recent transaction has been committed.
    if (m_commit_id == m_collection.last()) {
        m_reader.reset();
        return Status::ok();
    }

    return finish_abort();
}

auto BasicWriteAheadLog::start_abort(const GetFullImage &image_cb) -> Status
{
    static constexpr auto MSG = "could not abort last transaction";
    m_logger->info("received abort request");
    CALICO_EXPECT_FALSE(m_is_working);

    // Find the most-recent segment.
    for (; ; ) {
        auto s = m_reader->seek_next();
        if (s.is_not_found()) break;
        if (!s.is_ok()) return s;
    }

    auto s = Status::ok();
    for (Size i {}; ; i++) {
        const auto id = m_reader->segment_id();

        SequenceId first_lsn;
        s = m_reader->read_first_lsn(first_lsn);
        MAYBE_FORWARD(s, MSG);

        if (id < m_commit_id)
            break;

        s = m_reader->roll([&image_cb](const auto &info) {
            if (std::holds_alternative<FullImageDescriptor>(info)) {
                const auto image = std::get<FullImageDescriptor>(info);
                return image_cb(image);
            }
            return Status::ok();
        });

        // Most-recent segment can have an incomplete record at the end.
        if (s.is_corruption() && i == 0)
            continue;
        MAYBE_FORWARD(s, MSG);
    }
    return s;
}

auto BasicWriteAheadLog::finish_abort() -> Status
{
    auto s = Status::ok();
    auto id = m_collection.last();
    // Try to keep the WAL collection consistent with the segment files on disk.
    for (; !id.is_null() && id != m_commit_id && s.is_ok(); id = m_collection.id_before(id))
        s = m_store->remove_file(m_prefix + id.to_name());
    m_collection.remove_after(id);
    if (s.is_ok()) m_reader.reset();
    return s;
}

#undef MAYBE_FORWARD

} // namespace calico