// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

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
    CALICODB_EXPECT_NE(m_env, nullptr);
}

WriteAheadLog::~WriteAheadLog()
{
    delete m_writer;
    delete m_file;
    delete m_dir;
}

auto WriteAheadLog::open(const Parameters &param, WriteAheadLog **out) -> Status
{
    const auto [dir, base] = split_path(param.prefix);
    std::vector<std::string> possible_segments;
    CALICODB_TRY(param.env->get_children(dir, possible_segments));

    std::vector<Id> segments;
    for (auto &name : possible_segments) {
        name = join_paths(dir, name);
        if (const auto id = decode_segment_name(param.prefix, name); !id.is_null()) {
            segments.emplace_back(id);
        }
    }
    std::sort(begin(segments), end(segments));

    auto *wal = new WriteAheadLog {param};
    // Keep track of the segment files.
    for (const auto &id : segments) {
        wal->m_segments.insert({id, Lsn::null()});
    }
    *out = wal;
    return Status::ok();
}

auto WriteAheadLog::close() -> Status
{
    if (m_writer == nullptr) {
        return Status::ok();
    }
    return close_writer();
}

auto WriteAheadLog::start_writing() -> Status
{
    CALICODB_EXPECT_EQ(m_writer, nullptr);
    return open_writer();
}

auto WriteAheadLog::flushed_lsn() const -> Lsn
{
    return m_flushed_lsn;
}

auto WriteAheadLog::written_lsn() const -> Lsn
{
    return m_written_lsn;
}

auto WriteAheadLog::current_lsn() const -> Lsn
{
    return {m_last_lsn.value + 1};
}

auto WriteAheadLog::find_obsolete_lsn(Lsn &out) -> Status
{
    out = Lsn::null();
    if (m_segments.size() > 1) {
        auto segment = --end(m_segments);
        CALICODB_TRY(cache_first_lsn(*m_env, m_prefix, segment));
        CALICODB_EXPECT_FALSE(segment->second.is_null());
        out.value = segment->second.value - 1;
    }
    return Status::ok();
}

auto WriteAheadLog::log(const Slice &payload) -> Status
{
    if (m_writer == nullptr) {
        return Status::ok();
    }
    m_bytes_written += payload.size();
    CALICODB_TRY(m_writer->write(payload));

    if (m_writer->flushed_on_last_write()) {
        m_written_lsn.value = m_last_lsn.value - 1;
    }
    if (m_writer->block_number() >= kSegmentCutoff << m_segments.size()) {
        CALICODB_TRY(close_writer());
        return open_writer();
    }
    return Status::ok();
}

auto WriteAheadLog::advance_lsn(Lsn *out) -> void
{
    ++m_last_lsn.value;
    if (out != nullptr) {
        *out = m_last_lsn;
    }
}

auto WriteAheadLog::log_vacuum(bool is_start, Lsn *out) -> Status
{
    advance_lsn(out);
    return log(encode_vacuum_payload(
        m_last_lsn, is_start, m_data_buffer.data()));
}

auto WriteAheadLog::log_delta(Id page_id, const Slice &image, const std::vector<PageDelta> &delta, Lsn *out) -> Status
{
    advance_lsn(out);
    return log(encode_deltas_payload(
        m_last_lsn, page_id, image, delta, m_data_buffer.data()));
}

auto WriteAheadLog::log_image(Id page_id, const Slice &image, Lsn *out) -> Status
{
    advance_lsn(out);
    return log(encode_image_payload(
        m_last_lsn, page_id, image, m_data_buffer.data()));
}

auto WriteAheadLog::synchronize(bool flush) -> Status
{
    if (m_writer == nullptr) {
        return Status::ok();
    }
    if (flush) {
        CALICODB_TRY(m_writer->flush());
        m_written_lsn = m_last_lsn;
    }
    CALICODB_TRY(m_file->sync());

    // Only update the "flushed LSN" when the file has been synchronized. The
    // writer will need to flush intermittently to make more room in the tail
    // buffer, but no fsync() calls are issued until this method.
    m_flushed_lsn = m_written_lsn;
    return Status::ok();
}

auto WriteAheadLog::cleanup(Lsn recovery_lsn) -> Status
{
    for (auto prev = begin(m_segments);;) {
        if (prev == end(m_segments)) {
            return Status::ok();
        }
        auto next = prev;
        if (++next == end(m_segments)) {
            return Status::ok();
        }
        auto s = cache_first_lsn(*m_env, m_prefix, next);
        if (!s.is_ok() && !s.is_not_found()) {
            return s;
        }

        if (next->second > recovery_lsn) {
            return Status::ok();
        }
        CALICODB_TRY(m_env->remove_file(encode_segment_name(m_prefix, prev->first)));
        prev = m_segments.erase(prev);
    }
}

auto WriteAheadLog::close_writer() -> Status
{
    CALICODB_TRY(synchronize(true));

    const auto id = next_segment_id();
    const auto segment_name = encode_segment_name(m_prefix, id);

    std::size_t file_size;
    CALICODB_TRY(m_env->file_size(segment_name, file_size));

    delete m_file;
    delete m_writer;
    m_file = nullptr;
    m_writer = nullptr;

    if (file_size != 0) {
        m_segments.insert({id, Lsn::null()});
    } else {
        CALICODB_TRY(m_env->remove_file(segment_name));
    }
    return Status::ok();
}

auto WriteAheadLog::open_writer() -> Status
{
    const auto id = next_segment_id();
    CALICODB_TRY(m_env->new_logger(encode_segment_name(m_prefix, id), m_file));
    m_writer = new WalWriter {*m_file, m_tail_buffer};
    return m_env->sync_directory(split_path(m_prefix).first);
}

auto WriteAheadLog::next_segment_id() const -> Id
{
    auto id = Id::null();
    auto itr = rbegin(m_segments);
    if (itr != rend(m_segments)) {
        id = itr->first;
    }
    ++id.value;
    return id;
}

} // namespace calicodb