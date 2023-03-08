#include "wal.h"
#include "env_posix.h"
#include "logging.h"
#include "wal_writer.h"

namespace calicodb
{

WriteAheadLog::WriteAheadLog(const Parameters &param)
    : m_prefix {param.prefix},
      m_env {param.env},
      m_data_buffer(wal_scratch_size(param.page_size), '\x00'),
      m_tail_buffer(wal_block_size(param.page_size), '\x00')
{
    CDB_EXPECT_NE(m_env, nullptr);
}

WriteAheadLog::~WriteAheadLog()
{
    delete m_writer;
    delete m_file;
}

auto WriteAheadLog::open(const Parameters &param, WriteAheadLog **out) -> Status
{
    const auto [dir, base] = split_path(param.prefix);
    std::vector<std::string> possible_segments;
    CDB_TRY(param.env->get_children(dir, &possible_segments));

    std::vector<Id> segments;
    for (auto &name : possible_segments) {
        name = join_paths(dir, name);
        if (Slice {name}.starts_with(param.prefix)) {
            segments.emplace_back(decode_segment_name(param.prefix, name));
        }
    }
    std::sort(begin(segments), end(segments));

    auto *wal = new WriteAheadLog {param};

    // Keep track of the segment files.
    for (const auto &id : segments) {
        wal->m_set.add_segment(id);
    }
    *out = wal;
    return Status::ok();
}

auto WriteAheadLog::close() -> Status
{
    CDB_EXPECT_NE(m_writer, nullptr);
    return close_writer();
}

auto WriteAheadLog::start_writing() -> Status
{
    CDB_EXPECT_EQ(m_writer, nullptr);
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

auto WriteAheadLog::log(const Slice &payload) -> Status
{
    std::fprintf(stderr, "LSN = %llu\n", get_u64(payload.data() + 1));

    if (m_writer == nullptr) {
        return Status::logic_error("segment file is not put");
    }
    m_bytes_written += payload.size();

    CDB_TRY(m_writer->write(m_last_lsn, payload));
    if (m_writer->block_count() >= kSegmentCutoff << m_set.size()) {
        CDB_TRY(close_writer());
        return open_writer();
    }
    return Status::ok();
}

auto WriteAheadLog::log_vacuum(bool is_start, Lsn *out) -> Status
{
    ++m_last_lsn.value;
    if (out != nullptr) {
        *out = m_last_lsn;
    }
    return log(encode_vacuum_payload(
        m_last_lsn, is_start, m_data_buffer.data()));
}

auto WriteAheadLog::log_delta(Id page_id, const Slice &image, const ChangeBuffer &delta, Lsn *out) -> Status
{
    ++m_last_lsn.value;
    if (out != nullptr) {
        *out = m_last_lsn;
    }
    return log(encode_deltas_payload(
        m_last_lsn, page_id, image, delta, m_data_buffer.data()));
}

auto WriteAheadLog::log_image(Id page_id, const Slice &image, Lsn *out) -> Status
{
    ++m_last_lsn.value;
    if (out != nullptr) {
        *out = m_last_lsn;
    }
    return log(encode_image_payload(
        m_last_lsn, page_id, image, m_data_buffer.data()));
}

auto WriteAheadLog::flush() -> Status
{
    if (m_writer == nullptr) {
        return Status::logic_error("segment file is not put");
    }
    CDB_TRY(m_writer->flush());
    return m_file->sync();
}

auto WriteAheadLog::cleanup(Lsn recovery_lsn) -> Status
{
    if (m_set.size() <= 1) {
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
        auto s = read_first_lsn(*m_env, m_prefix, next_id, m_set, lsn);
        if (!s.is_ok() && !s.is_not_found()) {
            return s;
        }

        if (lsn > recovery_lsn) {
            return Status::ok();
        }
        CDB_TRY(m_env->remove_file(encode_segment_name(m_prefix, id)));
        m_set.remove_before(next_id);
    }
}

auto WriteAheadLog::close_writer() -> Status
{
    CDB_TRY(m_writer->flush());
    CDB_TRY(m_file->sync());
    m_flushed_lsn = m_writer->flushed_lsn();
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
        CDB_TRY(m_env->remove_file(encode_segment_name(m_prefix, id)));
    }
    return Status::ok();
}

auto WriteAheadLog::open_writer() -> Status
{
    // Writer is always opened on a new segment file.
    auto id = m_set.last();
    ++id.value;

    CDB_TRY(m_env->new_logger(encode_segment_name(m_prefix, id), &m_file));
    m_writer = new WalWriter {*m_file, m_tail_buffer};
    return Status::ok();
}

} // namespace calicodb