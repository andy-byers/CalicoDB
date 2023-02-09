#include "wal.h"
#include "cleanup.h"
#include "reader.h"
#include "utils/logging.h"
#include "writer.h"

namespace Calico {

WriteAheadLog::WriteAheadLog(const Parameters &param)
    : m_prefix {param.prefix},
      m_storage {param.store},
      m_reader_data(wal_scratch_size(param.page_size), '\x00'),
      m_reader_tail(wal_block_size(param.page_size), '\x00'),
      m_writer_tail(wal_block_size(param.page_size), '\x00'),
      m_segment_cutoff {param.segment_cutoff},
      m_buffer_count {param.writer_capacity}
{
    CALICO_EXPECT_NE(m_storage, nullptr);
    CALICO_EXPECT_NE(m_buffer_count, 0);
    CALICO_EXPECT_NE(m_segment_cutoff, 0);
}

auto WriteAheadLog::open(const Parameters &param) -> tl::expected<WriteAheadLog::Ptr, Status>
{
    auto path = param.prefix;
    if (auto pos = path.rfind('/'); pos != std::string::npos) {
        path.erase(pos + 1);
    }

    std::vector<std::string> child_names;
    if (auto s = param.store->get_children(path, child_names); !s.is_ok()) {
        return tl::make_unexpected(s);
    }

    std::vector<Id> segment_ids;
    for (auto &name: child_names) {
        name.insert(0, path);
        if (Slice {name}.starts_with(param.prefix)) {
            segment_ids.emplace_back(decode_segment_name(param.prefix, name));
        }
    }
    std::sort(begin(segment_ids), end(segment_ids));

    std::unique_ptr<WriteAheadLog> wal {new (std::nothrow) WriteAheadLog {param}};
    if (wal == nullptr) {
        return tl::make_unexpected(Status::system_error("out of memory"));
    }

    // Keep track of the segment files.
    for (const auto &id: segment_ids) {
        wal->m_set.add_segment(id);
    }

    return wal;
}

auto WriteAheadLog::close() -> Status
{
    // ~Worker<T>() will block until the thread is joined.
    m_worker.reset();
    m_cleanup.reset();
    if (m_writer) {
        std::move(*m_writer).destroy();
        m_writer.reset();
    }
    return status();
}

auto WriteAheadLog::start_workers() -> Status
{
    m_writer = std::unique_ptr<WalWriter> {
        new(std::nothrow) WalWriter {{
            m_prefix,
            m_writer_tail,
            m_storage,
            &m_error,
            &m_set,
            &m_flushed_lsn,
            m_segment_cutoff,
        }}};
    if (m_writer == nullptr) {
        return Status::system_error("cannot allocate writer object: out of memory");
    }

    m_cleanup = std::unique_ptr<WalCleanup> {
        new(std::nothrow) WalCleanup {{
            m_prefix,
            &m_recovery_lsn,
            m_storage,
            &m_error,
            &m_set,
        }}};
    if (m_cleanup == nullptr) {
        return Status::system_error("cannot allocate cleanup object: out of memory");
    }

    m_worker = std::unique_ptr<Worker<Event>> {
        new(std::nothrow) Worker<Event> {[this](auto event) {
            run_task(std::move(event));
        },
        m_buffer_count}};
    if (m_worker == nullptr) {
        return Status::system_error("cannot allocate task manager object: out of memory");
    }
    return Status::ok();
}

auto WriteAheadLog::run_task(Event event) -> void
{
    if (std::holds_alternative<WalPayloadIn>(event)) {
        m_writer->write(std::get<WalPayloadIn>(event));
    } else if (std::holds_alternative<FlushToken>(event)) {
        m_writer->flush();
    } else {
        CALICO_EXPECT_TRUE((std::holds_alternative<AdvanceToken>(event)));
        m_writer->advance();
    }

    m_cleanup->cleanup();
}

auto WriteAheadLog::flushed_lsn() const -> Lsn
{
    return m_flushed_lsn.load();
}

auto WriteAheadLog::current_lsn() const -> Lsn
{
    return {m_last_lsn.value + 1};
}

auto WriteAheadLog::log(WalPayloadIn payload) -> void
{
    CALICO_EXPECT_NE(m_writer, nullptr);

    m_last_lsn.value++;
    CALICO_EXPECT_EQ(payload.lsn(), m_last_lsn);

    m_bytes_written += payload.data().size() + sizeof(Lsn);
    m_worker->dispatch(payload);
}

auto WriteAheadLog::flush() -> Status
{
    CALICO_EXPECT_NE(m_writer, nullptr);
    m_worker->dispatch(FlushToken {}, true);
    return m_error.get();
}

auto WriteAheadLog::advance() -> void
{
    CALICO_EXPECT_NE(m_writer, nullptr);
    m_worker->dispatch(AdvanceToken {}, true);
}

auto WriteAheadLog::open_reader() -> tl::expected<WalReader, Status>
{
    if (auto s = status(); !s.is_ok()) {
        return tl::make_unexpected(s);
    }
    auto reader = WalReader {
        *m_storage,
        m_set,
        m_prefix,
        m_reader_tail,
        m_reader_data};
    if (auto s = reader.open(); !s.is_ok()) {
        return tl::make_unexpected(s);
    }
    return reader;
}

auto WriteAheadLog::roll_forward(Lsn begin_lsn, const Callback &callback) -> Status
{
    if (m_set.first().is_null()) {
        return Status::corruption("wal is empty");
    }

    // Open the reader on the first (oldest) WAL segment file.
    auto reader = open_reader();
    if (!reader.has_value()) {
        return reader.error();
    }

    // We should be on the first segment.
    CALICO_EXPECT_TRUE(reader->seek_previous().is_not_found());

    // Find the segment containing the first update that hasn't been applied yet.
    auto s = Status::ok();
    while (s.is_ok()) {
        Lsn first_lsn;
        s = reader->read_first_lsn(first_lsn);

        // This indicates an empty file. Try to seek back to the last segment.
        if (s.is_not_found()) {
            if (reader->segment_id() != m_set.last()) {
                return Status::corruption("missing segment");
            }
            s = reader->seek_previous();
            break;
        }
        Calico_Try_S(s);

        if (first_lsn >= begin_lsn) {
            if (first_lsn > begin_lsn) {
                s = reader->seek_previous();
            }
            break;
        } else {
            s = reader->seek_next();
        }
    }

    if (s.is_not_found()) {
        s = Status::ok();
    }

    for (auto first = true; s.is_ok(); first = false) {
        s = reader->roll([&callback, &first, this](const auto &payload) {
            if (!first && m_last_lsn.value + 1 != payload.lsn().value) {
                return Status::corruption("missing wal record");
            }
            m_last_lsn = payload.lsn();
            return callback(payload);
        });
        m_flushed_lsn.store(m_last_lsn);

        if (s.is_ok()) {
            s = reader->seek_next();
        }
    }
    const auto last_id = reader->segment_id();

    if (s.is_not_found()) {
        // Allow the last segment to be empty or contain an incomplete record.
        if (last_id == m_set.last()) {
            s = Status::ok();
        }
    }
    return s;
}

auto WriteAheadLog::roll_backward(Lsn end_lsn, const Callback &callback) -> Status
{
    if (m_set.first().is_null()) {
        return Status::corruption("wal is empty");
    }

    auto reader = open_reader();
    if (!reader.has_value()) {
        return reader.error();
    }

    // Find the most-recent segment and cache the first LSNs.
    for (; ; ) {
        auto s = reader->seek_next();
        if (s.is_not_found()) {
            break;
        }
        Calico_Try_S(s);
    }

    auto s = Status::ok();
    for (auto first = true; s.is_ok(); first = false) {
        Lsn first_lsn;
        s = reader->read_first_lsn(first_lsn);

        if (s.is_ok()) {
            if (first_lsn <= end_lsn) {
                break;
            }

            // Read all full image records. We can read them forward, since the page IDs are disjoint
            // within each transaction.
            s = reader->roll(callback);
        } else if (s.is_not_found()) {
            // The segment file is empty.
            s = Status::corruption(s.what().data());
        }

        if (s.is_ok()) {
            m_last_lsn.value = first_lsn.value - 1;
            m_flushed_lsn.store(m_last_lsn);
        } else if (s.is_corruption() && first) {
            // Most-recent segment can be empty or corrupted.
            s = Status::ok();
        }
        Calico_Try_S(s);

        s = reader->seek_previous();
    }
    // Indicates that we have hit the beginning of the WAL.
    if (s.is_not_found()) {
        s = Status::corruption(s.what().data());
    }

    return s;
}

auto WriteAheadLog::cleanup(Lsn recovery_lsn) -> void
{
    m_recovery_lsn.store(recovery_lsn);
}

auto WriteAheadLog::truncate(Lsn lsn) -> Status
{
    auto current = m_set.last();

    while (!current.is_null()) {
        auto first_lsn = read_first_lsn(
            *m_storage, m_prefix, current, m_set);

        if (first_lsn) {
            if (*first_lsn <= lsn) {
                break;
            }
        } else if (auto s = first_lsn.error(); !s.is_not_found()) {
            return s;
        }
        const auto name = encode_segment_name(m_prefix, current);
        Calico_Try_S(m_storage->remove_file(name));
        m_set.remove_after(Id {current.value - 1});
        current = m_set.id_before(current);
    }
    m_last_lsn = lsn;
    m_flushed_lsn.store(lsn);
    return Status::ok();
}

} // namespace Calico