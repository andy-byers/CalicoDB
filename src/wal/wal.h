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
#include "utils/expected.hpp"
#include "utils/scratch.h"
#include "utils/types.h"
#include "utils/worker.h"

namespace Calico {

class WalCleanup;
class WalReader;
class WalWriter;

class WriteAheadLog {
public:
    friend class Recovery;

    struct Parameters {
        std::string prefix;
        Storage *store {};
        System *system {};
        Size page_size {};
        Size segment_cutoff {};
        Size writer_capacity {};
    };

    using Ptr = std::unique_ptr<WriteAheadLog>;
    using Callback = std::function<Status(WalPayloadOut)>;

    System *system {};

    WriteAheadLog() = default;
    virtual ~WriteAheadLog() = default;
    [[nodiscard]] static auto open(const Parameters &param) -> tl::expected<WriteAheadLog::Ptr, Status>;
    [[nodiscard]] virtual auto close() -> Status;
    [[nodiscard]] virtual auto flushed_lsn() const -> Lsn;
    [[nodiscard]] virtual auto current_lsn() const -> Lsn;
    [[nodiscard]] virtual auto roll_forward(Lsn begin_lsn, const Callback &callback) -> Status;
    [[nodiscard]] virtual auto roll_backward(Lsn end_lsn, const Callback &callback) -> Status;
    [[nodiscard]] virtual auto start_workers() -> Status;
    [[nodiscard]] virtual auto truncate(Lsn lsn) -> Status;
    [[nodiscard]] virtual auto flush() -> Status;
    virtual auto cleanup(Lsn recovery_lsn) -> void;
    virtual auto log(WalPayloadIn payload) -> void;
    virtual auto advance() -> void;

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
    [[nodiscard]] auto open_reader() -> tl::expected<WalReader, Status>;

    LogPtr m_log;
    std::atomic<Lsn> m_flushed_lsn {};
    std::atomic<Lsn> m_recovery_lsn {};
    ErrorBuffer m_error;
    Lsn m_last_lsn;
    WalSet m_set;
    std::string m_prefix;

    Storage *m_storage {};
    std::string m_reader_data;
    std::string m_reader_tail;
    std::string m_writer_tail;
    Size m_segment_cutoff {};
    Size m_buffer_count {};
    Size m_bytes_written {};

    struct AdvanceToken {};
    struct FlushToken {};

    using Event = std::variant<WalPayloadIn, AdvanceToken, FlushToken>;

    auto run_task(Event event) -> void;

    std::unique_ptr<WalWriter> m_writer;
    std::unique_ptr<WalCleanup> m_cleanup;
    std::unique_ptr<Worker<Event>> m_worker;
};

} // namespace Calico

#endif // CALICO_WAL_H
