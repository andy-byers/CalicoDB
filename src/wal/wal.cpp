#include "wal.h"
#include "utils/logging.h"
#include "writer.h"

namespace calicodb
{

WriteAheadLog::WriteAheadLog(const Parameters &param)
    : m_prefix {param.prefix},
      m_storage {param.store},
      m_tail(wal_block_size(param.page_size), '\x00'),
      m_segment_cutoff {param.segment_cutoff}
{
    CALICO_EXPECT_NE(m_storage, nullptr);
    CALICO_EXPECT_NE(m_segment_cutoff, 0);
}

WriteAheadLog::~WriteAheadLog()
{
    delete m_writer;
    delete m_file;
}

auto WriteAheadLog::open(const Parameters &param, WriteAheadLog **out) -> Status
{
    auto path = param.prefix;
    if (auto pos = path.rfind('/'); pos != std::string::npos) {
        path.erase(pos + 1);
    }

    std::vector<std::string> child_names;
    CALICO_TRY(param.store->get_children(path, child_names));

    std::vector<Id> segment_ids;
    for (auto &name : child_names) {
        name.insert(0, path);
        if (Slice {name}.starts_with(param.prefix)) {
            segment_ids.emplace_back(decode_segment_name(param.prefix, name));
        }
    }
    std::sort(begin(segment_ids), end(segment_ids));

    auto *wal = new WriteAheadLog {param};

    // Keep track of the segment files.
    for (const auto &id : segment_ids) {
        wal->m_set.add_segment(id);
    }
    *out = wal;
    return Status::ok();
}

auto WriteAheadLog::close() -> Status
{
    CALICO_EXPECT_NE(m_writer, nullptr);
    return close_writer();
}

auto WriteAheadLog::start_writing() -> Status
{
    CALICO_EXPECT_EQ(m_writer, nullptr);
    return open_writer();
}

auto WriteAheadLog::flushed_lsn() const -> Lsn
{
    if (m_writer) {
        const auto lsn = m_writer->flushed_lsn();
        if (!lsn.is_null()) {
            m_flushed_lsn = lsn;
        }
    }
    return m_flushed_lsn;
}

auto WriteAheadLog::current_lsn() const -> Lsn
{
    return {m_last_lsn.value + 1};
}

auto WriteAheadLog::log(WalPayloadIn payload) -> Status
{
    if (m_writer == nullptr) {
        return Status::logic_error("segment file is not open");
    }
    ++m_last_lsn.value;
    m_bytes_written += sizeof(Lsn) + payload.data().size();
    CALICO_EXPECT_EQ(payload.lsn(), m_last_lsn);

    CALICO_TRY(m_writer->write(payload));
    if (m_writer->block_count() >= m_segment_cutoff) {
        CALICO_TRY(close_writer());
        return open_writer();
    }
    return Status::ok();
}

auto WriteAheadLog::flush() -> Status
{
    if (m_writer == nullptr) {
        return Status::logic_error("segment file is not open");
    }
    CALICO_TRY(m_writer->flush());
    return m_file->sync();
}

auto WriteAheadLog::cleanup(Lsn recovery_lsn) -> Status
{
    if (m_set.segments().size() <= 1) {
        return Status::ok();
    }
    for (;;) {
        const auto id = m_set.first();
        if (id.is_null()) {
            return Status::ok();
        }
        const auto next_id = m_set.id_after(id);
        if (next_id.is_null()) {
            return Status::ok();
        }

        Lsn lsn;
        auto s = read_first_lsn(*m_storage, m_prefix, next_id, m_set, lsn);
        if (!s.is_ok() && !s.is_not_found()) {
            return s;
        }

        if (lsn > recovery_lsn) {
            return Status::ok();
        }
        CALICO_TRY(m_storage->remove_file(encode_segment_name(m_prefix, id)));
        m_set.remove_before(next_id);
    }
}

auto WriteAheadLog::close_writer() -> Status
{
    CALICO_TRY(m_writer->flush());
    CALICO_TRY(m_file->sync());
    const auto written = m_writer->block_count() != 0;

    delete m_file;
    delete m_writer;
    m_file = nullptr;
    m_writer = nullptr;

    auto id = m_set.last();
    ++id.value;

    if (written) {
        m_set.add_segment(id);
    } else {
        CALICO_TRY(m_storage->remove_file(encode_segment_name(m_prefix, id)));
    }
    return Status::ok();
}

auto WriteAheadLog::open_writer() -> Status
{
    auto id = m_set.last();
    ++id.value;

    CALICO_TRY(m_storage->new_logger(encode_segment_name(m_prefix, id), &m_file));
    m_writer = new WalWriter {*m_file, m_tail};
    return Status::ok();
}

} // namespace calicodb