#include "wal.h"
#include "cleanup.h"
#include "reader.h"
#include "writer.h"
#include "utils/system.h"

namespace Calico {

WriteAheadLog::WriteAheadLog(const Parameters &param)
    : system {param.system},
      m_log {param.system->create_log("wal")},
      m_prefix {param.prefix},
      m_storage {param.store},
      m_reader_data(wal_scratch_size(param.page_size), '\x00'),
      m_reader_tail(wal_block_size(param.page_size), '\x00'),
      m_writer_tail(wal_block_size(param.page_size), '\x00'),
      m_segment_cutoff {param.segment_cutoff},
      m_buffer_count {param.writer_capacity}
{
    Calico_Info("initializing, write buffer size is {}", param.writer_capacity * m_reader_data.size());

    CALICO_EXPECT_NE(system, nullptr);
    CALICO_EXPECT_NE(m_storage, nullptr);
    CALICO_EXPECT_NE(m_buffer_count, 0);
    CALICO_EXPECT_NE(m_segment_cutoff, 0);
}

auto WriteAheadLog::open(const Parameters &param) -> tl::expected<WriteAheadLog::Ptr, Status>
{
    // Get the name of every file in the database directory.
    std::vector<std::string> child_names;
    const auto path_prefix = param.prefix + WAL_PREFIX;
    if (auto s = param.store->get_children(param.prefix, child_names); !s.is_ok()) {
        return tl::make_unexpected(s);
    }

    // Filter out the segment file names.
    std::vector<std::string> segment_names;
    std::copy_if(cbegin(child_names), cend(child_names), back_inserter(segment_names), [&path_prefix](const auto &path) {
        return Slice {path}.starts_with(path_prefix);
    });

    // Convert to segment IDs.
    std::vector<Id> segment_ids;
    std::transform(cbegin(segment_names), cend(segment_names), back_inserter(segment_ids), [param](const auto &name) {
        return decode_segment_name(Slice {name}.advance(param.prefix.size()));
    });
    std::sort(begin(segment_ids), end(segment_ids));

    std::unique_ptr<WriteAheadLog> wal {new (std::nothrow) WriteAheadLog {param}};
    if (wal == nullptr) {
        return tl::make_unexpected(system_error("cannot allocate WAL object: out of memory"));
    }

    // Keep track of the segment files.
    for (const auto &id: segment_ids) {
        wal->m_set.add_segment(id);
    }

    return wal;
}

auto WriteAheadLog::close() -> Status
{
    if (m_writer) {
        std::move(*m_writer).destroy();
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
        return system_error("cannot allocate writer object: out of memory");
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
        return system_error("cannot allocate cleanup object: out of memory");
    }

    m_tasks = std::unique_ptr<Worker<Event>> {
        new(std::nothrow) Worker<Event> {[this](auto event) {
            run_task(std::move(event));
        },
        m_buffer_count}};
    if (m_tasks == nullptr) {
        return system_error("cannot allocate task manager object: out of memory");
    }
    return ok();
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
    m_bytes_written += payload.data().size() + sizeof(Lsn);
    m_tasks->dispatch(payload);
}

auto WriteAheadLog::flush() -> Status
{
    CALICO_EXPECT_NE(m_writer, nullptr);
    m_tasks->dispatch(FlushToken {}, true);
    return m_error.get();
}

auto WriteAheadLog::advance() -> Status
{
    CALICO_EXPECT_NE(m_writer, nullptr);
    m_tasks->dispatch(AdvanceToken {}, true);
    return m_error.get();
}

auto WriteAheadLog::open_reader() -> tl::expected<WalReader, Status>
{
    auto reader = WalReader {
        *m_storage,
        m_set,
        m_prefix,
        Span {m_reader_tail},
        Span {m_reader_data}};
    if (auto s = reader.open(); !s.is_ok()) {
        return tl::make_unexpected(s);
    }
    return reader;
}

auto WriteAheadLog::roll_forward(Lsn begin_lsn, const Callback &callback) -> Status
{
    if (m_set.first().is_null()) {
        return ok();
    }

    if (m_last_lsn.is_null()) {
        m_last_lsn = begin_lsn;
        m_flushed_lsn.store(m_last_lsn);
    }
    // Open the reader on the first (oldest) WAL segment file.
    auto reader = open_reader();
    if (!reader.has_value()) {
        return reader.error();
    }

    // We should be on the first segment.
    CALICO_EXPECT_TRUE(reader->seek_previous().is_not_found());

    // Find the segment containing the first update that hasn't been applied yet.
    auto s = ok();
    while (s.is_ok()) {
        Lsn first_lsn;
        s = reader->read_first_lsn(first_lsn);

        // This indicates an empty file. Try to seek back to the last segment.
        if (s.is_not_found()) {
            if (reader->segment_id() != m_set.last()) {
                return corruption("missing WAL data in segment {}", reader->segment_id().value);
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

    if (s.is_not_found())
        s = ok();

    while (s.is_ok()) {
        Calico_Info("rolling segment {} forward", reader->segment_id().value);

        s = reader->roll([&callback, begin_lsn, this](auto payload) {
            m_last_lsn = payload.lsn();
            if (m_last_lsn >= begin_lsn) {
                return callback(payload);
            }
            return ok();
        });
        m_flushed_lsn.store(m_last_lsn);

        // We found an empty segment. This happens when the program aborted before the writer could either
        // write a block or delete the empty file. This is OK if we are on the last segment.
        if (s.is_not_found()) {
            s = corruption("encountered an empty segment file {}", encode_segment_name(reader->segment_id()));
        }

        if (s.is_ok()) {
            s = reader->seek_next();
        }
    }
    const auto last_id = reader->segment_id();

    if (!s.is_ok()) {
        if (s.is_corruption()) {
            if (last_id != m_set.last()) {
                return s;
            }
        } else if (!s.is_not_found()) {
            return s;
        }
        s = ok();
    }
    return s;
}

auto WriteAheadLog::roll_backward(Lsn end_lsn, const Callback &callback) -> Status
{
    if (m_set.first().is_null()) {
        return ok();
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

    auto s = ok();
    for (Size i {}; s.is_ok(); i++) {
        Lsn first_lsn;
        s = reader->read_first_lsn(first_lsn);

        if (s.is_ok()) {
            if (first_lsn <= end_lsn) {
                break;
            }

            Calico_Info("rolling segment {} backward", reader->segment_id().value);

            // Read all full image records. We can read them forward, since the pages are disjoint
            // within each transaction.
            s = reader->roll(callback);
        } else if (s.is_not_found()) {
            // The segment file is empty.
            s = corruption(s.what().data());
        }

        // Most-recent segment can have an incomplete record at the end.
        if (s.is_corruption() && i == 0) {
            s = ok();
        }
        Calico_Try_S(s);

        s = reader->seek_previous();
    }
    return s.is_not_found() ? ok() : s;
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
        if (!current.is_null()) {
            const auto name = m_prefix + encode_segment_name(current);
            Calico_Try_S(m_storage->remove_file(name));
            m_set.remove_after(Id {current.value - 1});
            Calico_Info("removed segment {} with first lsn {}", name, first_lsn->value);
        }
        current = m_set.id_before(current);
    }
    return ok();
}

} // namespace Calico