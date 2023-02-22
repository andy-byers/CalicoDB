#ifndef CALICO_WAL_H
#define CALICO_WAL_H

#include <atomic>
#include <functional>
#include <optional>
#include <unordered_set>
#include <variant>
#include <vector>
#include "helpers.h"
#include "record.h"
#include "utils/encoding.h"
#include "utils/scratch.h"
#include "utils/types.h"

namespace Calico {

class WalCleanup;
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
    virtual ~WriteAheadLog() = default;
    [[nodiscard]] static auto open(const Parameters &param, WriteAheadLog **out) -> Status;
    [[nodiscard]] virtual auto close() -> Status;
    [[nodiscard]] virtual auto flushed_lsn() const -> Lsn;
    [[nodiscard]] virtual auto current_lsn() const -> Lsn;
    [[nodiscard]] virtual auto start_writing() -> Status;
    [[nodiscard]] virtual auto flush() -> Status;
    virtual auto cleanup(Lsn recovery_lsn) -> void;
    virtual auto log(WalPayloadIn payload) -> void;

    [[nodiscard]]
    virtual auto status() const -> Status
    {
        return m_error.get();
    }

    [[nodiscard]]
    virtual auto bytes_written() const -> Size
    {
        return m_bytes_written;
    }

private:
    explicit WriteAheadLog(const Parameters &param);

    mutable Lsn m_flushed_lsn;
    ErrorBuffer m_error;
    Lsn m_last_lsn;
    WalSet m_set;
    std::string m_prefix;

    Storage *m_storage {};
    std::string m_tail;
    Size m_segment_cutoff {};
    Size m_bytes_written {};

    std::unique_ptr<WalWriter> m_writer;
    std::unique_ptr<WalCleanup> m_cleanup;
};

} // namespace Calico

#endif // CALICO_WAL_H
