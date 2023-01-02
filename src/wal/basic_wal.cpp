#include "basic_wal.h"
#include "cleaner.h"
#include "utils/system.h"

namespace Calico {

static auto propagate_worker_error(System &system, const WriteAheadLog &wal)
{
    auto s = wal.worker_status();

    if (!s.is_ok())
        system.push_error(Error::ERROR, s);

    return ok();
}

#define PROPAGATE_WORKER_ERROR \
    do { \
        CALICO_EXPECT_TRUE(m_is_working); \
        CALICO_TRY_S(propagate_worker_error(*m_system, *this)); \
    } while (0)

BasicWriteAheadLog::BasicWriteAheadLog(const Parameters &param)
    : m_log {param.system->create_log("wal")},
      m_prefix {param.prefix},
      m_store {param.store},
      m_system {param.system},
      m_reader_data(wal_scratch_size(param.page_size), '\x00'),
      m_reader_tail(wal_block_size(param.page_size), '\x00'),
      m_writer_tail(wal_block_size(param.page_size), '\x00'),
      m_wal_limit {param.wal_limit}
{
    m_log->info("opening WAL");
}

auto BasicWriteAheadLog::open(const Parameters &param) -> tl::expected<WriteAheadLog::Ptr, Status>
{
    // Get the name of every file in the database directory.
    std::vector<std::string> child_names;
    const auto path_prefix = param.prefix + WAL_PREFIX;
    auto s = param.store->get_children(param.prefix, child_names);
    if (!s.is_ok()) return tl::make_unexpected(s);

    // Filter out the segment file names.
    std::vector<std::string> segment_names;
    std::copy_if(cbegin(child_names), cend(child_names), back_inserter(segment_names), [&path_prefix](const auto &path) {
        return BytesView {path}.starts_with(path_prefix);
    });

    // Convert to segment IDs.
    std::vector<SegmentId> segment_ids;
    std::transform(cbegin(segment_names), cend(segment_names), back_inserter(segment_ids), [param](const auto &name) {
        return SegmentId::from_name(BytesView {name}.advance(param.prefix.size()));
    });
    std::sort(begin(segment_ids), end(segment_ids));

    std::unique_ptr<BasicWriteAheadLog> wal {new (std::nothrow) BasicWriteAheadLog {param}};
    if (wal == nullptr)
        return tl::make_unexpected(system_error("cannot allocate WAL object: out of memory"));

    // Keep track of the segment files.
    for (const auto &id: segment_ids)
        wal->m_set.add_segment(id);

    return wal;
}

BasicWriteAheadLog::~BasicWriteAheadLog()
{
    m_log->info("closing WAL");

    if (m_is_working) {
        propagate_worker_error(*m_system, *this);
        CALICO_WARN_IF(stop_workers_impl());
    }
}

auto BasicWriteAheadLog::worker_status() const -> Status
{
    if (m_is_working) {
        CALICO_TRY_S(m_writer->status());
        return m_cleaner->status();
    }
    return ok();
}

auto BasicWriteAheadLog::flushed_lsn() const -> Id
{
    return m_flushed_lsn.load();
}

auto BasicWriteAheadLog::current_lsn() const -> Id
{
    return Id {m_last_lsn.value + 1};
}

auto BasicWriteAheadLog::remove_before(Id lsn) -> Status
{
    PROPAGATE_WORKER_ERROR;

    m_cleaner->remove_before(lsn);
    return m_cleaner->status(); // TODO: It's kinda pointless to return the status here, as it's likely not updated yet anyway. We should check it once each "round", whatever that means.
}

auto BasicWriteAheadLog::log(WalPayloadIn payload) -> Status
{
    PROPAGATE_WORKER_ERROR;

    m_last_lsn.value++;
    m_writer->write(payload);
    return m_writer->status();
}

auto BasicWriteAheadLog::flush() -> Status
{
    m_log->info("flushing tail buffer");
    PROPAGATE_WORKER_ERROR;

    // flush() blocks until the background writer is finished.
    m_writer->flush();
    return m_writer->status();
}

auto BasicWriteAheadLog::advance() -> Status
{
    m_log->info("advancing to new segment");
    PROPAGATE_WORKER_ERROR;

    // advance() blocks until the background writer is finished.
    m_writer->advance();
    return m_writer->status();
}

auto BasicWriteAheadLog::stop_workers() -> Status
{
    return stop_workers_impl();
}

auto BasicWriteAheadLog::stop_workers_impl() -> Status
{
    // Stops the workers no matter what, even if an error is encountered. We should be able to call abort_last() safely after
    // this method returns.
    m_log->info("received stop request");
    if (!m_is_working) return ok();
    PROPAGATE_WORKER_ERROR;

    m_is_working = false;

    auto s = std::move(*m_writer).destroy();
    m_writer.reset();

    auto t = std::move(*m_cleaner).destroy();
    m_cleaner.reset();

    CALICO_TRY_S(s);
    CALICO_TRY_S(t);

    m_log->info("workers are stopped");
    return ok();
}

auto BasicWriteAheadLog::start_workers() -> Status
{
    m_log->info("received start request");
    CALICO_EXPECT_FALSE(m_is_working);

    auto s = open_writer();
    auto t = open_cleaner();

    if (s.is_ok() && t.is_ok()) {
        m_log->info("workers are started");
        m_is_working = true;
        return ok();
    }
    m_writer.reset();
    m_cleaner.reset();
    return s.is_ok() ? t : s;
}

auto BasicWriteAheadLog::open_reader() -> Status
{
    m_reader.emplace(
        *m_store,
        m_set,
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
        m_set,
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
        m_set);
    return ok();
}

auto BasicWriteAheadLog::roll_forward(Id begin_lsn, const Callback &callback) -> Status
{
    m_log->info("rolling forward from LSN {}", begin_lsn.value);

    if (m_is_working)
        CALICO_TRY_S(stop_workers());

    if (m_set.first().is_null())
        return ok();

    if (m_last_lsn.is_null()) {
        m_last_lsn = begin_lsn;
        m_flushed_lsn.store(m_last_lsn);
    }
    // Open the reader on the first (oldest) WAL segment file.
    if (m_reader == std::nullopt)
        CALICO_TRY_S(open_reader());

    // Make sure we are on the first segment.
    for (; ; ) {
        auto s = m_reader->seek_previous();
        if (s.is_not_found()) break;
        CALICO_TRY_S(s);
    }

    auto s = ok();
    while (s.is_ok()) {
        Id first_lsn;
        s = m_reader->read_first_lsn(first_lsn);
        if (!s.is_ok()) break;

        s = m_reader->roll([&callback, begin_lsn, this](auto payload) {
            m_last_lsn = payload.lsn();
            if (m_last_lsn >= begin_lsn)
                return callback(payload);
            return ok();
        });
        m_flushed_lsn.store(m_last_lsn);

        // We found an empty segment. This happens when the program aborted before the writer could either
        // write a block or delete the empty file. This is OK if we are on the last segment.
        if (s.is_not_found())
            s = corruption(s.what());

        if (!s.is_ok()) {
            CALICO_WARN(s);
            break;
        }
        s = m_reader->seek_next();
    }
    const auto last_id = m_reader->segment_id();
    m_reader.reset();

    // Translate the error status if needed. Note that we allow an incomplete record at the end of the
    // most-recently-written segment.
    if (!s.is_ok()) {
        if (s.is_corruption()) {
            if (last_id != m_set.last())
                return s;
        } else if (!s.is_not_found()) {
            return s;
        }
        s = ok();
    }
    return s;
}

auto BasicWriteAheadLog::roll_backward(Id end_lsn, const Callback &callback) -> Status
{
    m_log->info("rolling backward to LSN {}", end_lsn.value);
    CALICO_EXPECT_FALSE(m_is_working);

    if (m_set.first().is_null())
        return ok();

    if (m_reader == std::nullopt)
        CALICO_TRY_S(open_reader());

    // Find the most-recent segment.
    for (; ; ) {
        auto s = m_reader->seek_next();
        if (s.is_not_found()) break;
        if (!s.is_ok()) return s;
        CALICO_TRY_S(s);
    }

    auto s = ok();
    for (Size i {}; s.is_ok(); i++) {
        Id first_lsn;
        s = m_reader->read_first_lsn(first_lsn);

        if (s.is_ok()) {
            // Found the segment containing the end_lsn.
            if (first_lsn <= end_lsn)
                break;

            // Read all full image records. We can read them forward, since the pages are disjoint
            // within each transaction.
            s = m_reader->roll(callback);
        } else if (s.is_not_found()) {
            // The segment file is empty.
            s = corruption(s.what());
        }

        // Most-recent segment can have an incomplete record at the end.
        if (s.is_corruption() && i == 0)
            s = ok();
        CALICO_TRY_S(s);

        s = m_reader->seek_previous();
    }
    m_reader.reset();
    return s.is_not_found() ? ok() : s;
}

auto BasicWriteAheadLog::remove_after(Id limit) -> Status
{
    CALICO_EXPECT_FALSE(m_is_working);
    auto last = m_set.last();
    auto current = last;
    SegmentId target;

    while (!current.is_null()) {
        Id first_lsn;
        auto s = read_first_lsn(
            *m_store, m_prefix, current, first_lsn);

        if (s.is_ok()) {
            if (first_lsn >= limit)
                break;
        } else if (!s.is_not_found()) {
            return s;
        }
        if (!target.is_null()) {
            CALICO_TRY_S(m_store->remove_file(m_prefix + target.to_name()));
            m_set.remove_after(current);
        }
        target = current;
        current = m_set.id_before(current);
    }
    return ok();
}

#undef PROPAGATE_WORKER_ERROR

} // namespace Calico