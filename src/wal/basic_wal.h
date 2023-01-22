#ifndef CALICO_WAL_BASIC_WAL_H
#define CALICO_WAL_BASIC_WAL_H

#include "cleanup.h"
#include "helpers.h"
#include "reader.h"
#include "record.h"
#include "utils/types.h"
#include "wal.h"
#include "writer.h"
#include <atomic>
#include <optional>
#include "utils/expected.hpp"
#include <unordered_set>

namespace Calico {

class Storage;

class BasicWriteAheadLog: public WriteAheadLog {
public:
    using Interval = std::chrono::duration<double>;

    struct Parameters {
        std::string prefix;
        Storage *store {};
        System *system {};
        Size page_size {};
        Size segment_cutoff {};
        Size writer_capacity {};
        Interval interval {};
    };

    ~BasicWriteAheadLog() override;
    [[nodiscard]] static auto open(const Parameters &param) -> tl::expected<WriteAheadLog::Ptr, Status>;
    [[nodiscard]] auto flushed_lsn() const -> Lsn override;
    [[nodiscard]] auto current_lsn() const -> Lsn override;
    [[nodiscard]] auto roll_forward(Lsn begin_lsn, const Callback &callback) -> Status override;
    [[nodiscard]] auto roll_backward(Lsn end_lsn, const Callback &callback) -> Status override;
    [[nodiscard]] auto start_workers() -> Status override;
    [[nodiscard]] auto truncate(Lsn lsn) -> Status override;
    auto cleanup(Lsn recovery_lsn) -> void override;
    auto advance() -> void override;
    auto log(WalPayloadIn payload) -> void override;
    auto flush() -> void override;

    [[nodiscard]]
    auto bytes_written() const -> Size override
    {
        return m_bytes_written;
    }

private:
    explicit BasicWriteAheadLog(const Parameters &param);
    [[nodiscard]] auto open_reader() -> tl::expected<WalReader, Status>;

    LogPtr m_log;
    std::atomic<Lsn> m_flushed_lsn {};
    std::atomic<Lsn> m_recovery_lsn {};
    Lsn m_last_lsn;
    WalSet m_set;
    std::string m_prefix;

    Storage *m_store {};
    System *m_system {};
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
    std::unique_ptr<Worker<Event>> m_tasks;
};

} // namespace Calico

#endif // CALICO_WAL_BASIC_WAL_H
