// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#ifndef CALICODB_WAL_H
#define CALICODB_WAL_H

#include "wal_record.h"

namespace calicodb
{

class WalWriter;

class WriteAheadLog
{
public:
    friend class DBImpl;

    struct Parameters {
        std::string prefix;
        Env *env {};
        std::size_t page_size {};
    };

    WriteAheadLog() = default;
    virtual ~WriteAheadLog();
    [[nodiscard]] static auto open(const Parameters &param, WriteAheadLog **out) -> Status;
    [[nodiscard]] virtual auto close() -> Status;
    [[nodiscard]] virtual auto flushed_lsn() const -> Lsn;
    [[nodiscard]] virtual auto written_lsn() const -> Lsn;
    [[nodiscard]] virtual auto current_lsn() const -> Lsn;
    [[nodiscard]] virtual auto find_obsolete_lsn(Lsn &out) -> Status;
    [[nodiscard]] virtual auto start_writing() -> Status;
    [[nodiscard]] virtual auto synchronize(bool flush) -> Status;
    [[nodiscard]] virtual auto cleanup(Lsn recovery_lsn) -> Status;
    [[nodiscard]] virtual auto log_vacuum(bool is_start, Lsn *out) -> Status;
    [[nodiscard]] virtual auto log_delta(Id page_id, const Slice &image, const std::vector<PageDelta> &delta, Lsn *out) -> Status;
    [[nodiscard]] virtual auto log_image(Id page_id, const Slice &image, Lsn *out) -> Status;

    [[nodiscard]] virtual auto bytes_written() const -> std::size_t
    {
        return m_bytes_written;
    }

private:
    explicit WriteAheadLog(const Parameters &param);
    [[nodiscard]] auto next_segment_id() const -> Id;
    [[nodiscard]] auto finish_current_segment() -> Status;
    [[nodiscard]] auto open_next_segment(Logger *&file) -> Status;
    [[nodiscard]] auto log(const Slice &payload) -> Status;
    auto advance_lsn(Lsn *out) -> void;

    static constexpr std::size_t kSegmentCutoff = 32;

    // Maps each completed segment to the first LSN written to it.
    std::map<Id, Lsn> m_segments;

    // Last LSN written to disk. Always greater than or equal to the "flushed LSN".
    Lsn m_written_lsn;

    // Last LSN that is definitely on disk. The file must be fsync()'d before
    // this value can be increased to match the "written LSN".
    Lsn m_flushed_lsn;

    // Last LSN written to the tail buffer. Always greater than or equal to the
    // "written LSN".
    Lsn m_last_lsn;

    // Prefix used for WAL segments.
    std::string m_prefix;

    // Directory containing the WAL segments (must be a prefix of "m_prefix"). Used to
    // fsync() the directory when a new segment is created.
    std::string m_dirname;

    std::string m_data_buffer;
    std::string m_tail_buffer;
    std::size_t m_bytes_written {};

    Env *m_env {};
    WalWriter *m_writer {};
    Logger *m_file {};
    Reader *m_dir {};
};

} // namespace calicodb

#endif // CALICODB_WAL_H
