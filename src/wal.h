#ifndef CALICODB_WAL_H
#define CALICODB_WAL_H

#include "wal_record.h"

namespace calicodb
{

class WalWriter;

class WriteAheadLog
{
public:
    friend class Recovery;

    static constexpr std::size_t kSegmentCutoff {32};

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
    [[nodiscard]] virtual auto log(WalPayloadIn payload) -> Status;

    [[nodiscard]] virtual auto bytes_written() const -> std::size_t
    {
        return m_bytes_written;
    }

private:
    explicit WriteAheadLog(const Parameters &param);
    [[nodiscard]] auto close_writer() -> Status;
    [[nodiscard]] auto open_writer() -> Status;

    mutable Lsn m_flushed_lsn;
    Lsn m_last_lsn;
    WalSet m_set;
    std::string m_prefix;

    Env *m_env {};
    std::string m_tail;
    std::size_t m_bytes_written {};

    WalWriter *m_writer {};
    Logger *m_file {};
};

} // namespace calicodb

#endif // CALICODB_WAL_H
