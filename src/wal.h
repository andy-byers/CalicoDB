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
    [[nodiscard]] virtual auto current_lsn() const -> Lsn;
    [[nodiscard]] virtual auto start_writing() -> Status;
    [[nodiscard]] virtual auto flush() -> Status;
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
    [[nodiscard]] auto close_writer() -> Status;
    [[nodiscard]] auto open_writer() -> Status;
    [[nodiscard]] auto log(const Slice &payload) -> Status;

    static constexpr std::size_t kSegmentCutoff {32};

    std::map<Id, Lsn> m_segments;

    Lsn m_flushed_lsn;
    Lsn m_last_lsn;
    std::string m_prefix;

    Env *m_env {};
    std::string m_data_buffer;
    std::string m_tail_buffer;
    std::size_t m_bytes_written {};

    WalWriter *m_writer {};
    Logger *m_file {};
};

} // namespace calicodb

#endif // CALICODB_WAL_H
