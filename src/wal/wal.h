#ifndef CALICO_WAL_H
#define CALICO_WAL_H

#include "helpers.h"
#include "record.h"

namespace Calico {

class WalWriter;

class WriteAheadLog {
public:
    friend class Recovery;

    struct Parameters {
        std::string prefix;
        Storage *store {};
        Size page_size {};
        Size segment_cutoff {};
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

    [[nodiscard]]
    virtual auto bytes_written() const -> Size
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

    Storage *m_storage {};
    std::string m_tail;
    Size m_segment_cutoff {};
    Size m_bytes_written {};

    WalWriter *m_writer {};
    Logger *m_file {};
};

} // namespace Calico

#endif // CALICO_WAL_H
