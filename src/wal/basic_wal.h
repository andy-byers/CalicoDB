#ifndef CALICO_WAL_BASIC_WAL_H
#define CALICO_WAL_BASIC_WAL_H

#include "cleaner.h"
#include "helpers.h"
#include "reader.h"
#include "record.h"
#include "wal.h"
#include "writer.h"
#include <tl/expected.hpp>
#include "utils/types.h"
#include <atomic>
#include <optional>
#include <unordered_set>

namespace calico {

class Storage;
class AppendWriter;
class RandomReader;
class WalCleaner;

class BasicWriteAheadLog: public WriteAheadLog {
public:
    struct Parameters {
        std::string prefix;
        Storage *store {};
        System *system {};
        Size page_size {};
        Size wal_limit {};
    };

    [[nodiscard]]
    auto is_enabled() const -> bool override
    {
        return m_wal_limit != DISABLE_WAL;
    }

    [[nodiscard]]
    auto is_working() const -> bool override
    {
        return m_is_working;
    }

    ~BasicWriteAheadLog() override;
    [[nodiscard]] static auto open(const Parameters &param) -> tl::expected<WriteAheadLog::Ptr, Status>;
    [[nodiscard]] auto flushed_lsn() const -> Id override;
    [[nodiscard]] auto current_lsn() const -> Id override;
    [[nodiscard]] auto stop_workers() -> Status override;
    [[nodiscard]] auto start_workers() -> Status override;
    [[nodiscard]] auto worker_status() const -> Status override;
    [[nodiscard]] auto log(WalPayloadIn payload) -> Status override;
    [[nodiscard]] auto advance() -> Status override;
    [[nodiscard]] auto flush() -> Status override;
    [[nodiscard]] auto roll_forward(Id begin_lsn, const Callback &callback) -> Status override;
    [[nodiscard]] auto roll_backward(Id end_lsn, const Callback &callback) -> Status override;
    [[nodiscard]] auto remove_after(Id lsn) -> Status override;
    [[nodiscard]] auto remove_before(Id lsn) -> Status override;

private:
    explicit BasicWriteAheadLog(const Parameters &param);
    [[nodiscard]] auto stop_workers_impl() -> Status;
    [[nodiscard]] auto open_reader() -> Status; // TODO: We don't really have to store the reader as a member...
    [[nodiscard]] auto open_writer() -> Status;
    [[nodiscard]] auto open_cleaner() -> Status;

    auto forward_status(Status s, const std::string &message) -> Status
    {
        if (!s.is_ok()) {
            m_log->error(message);
            m_log->error("(reason) {}", s.what());
        }
        return s;
    }

    LogPtr m_log;
    std::atomic<Id> m_flushed_lsn {};
    Id m_last_lsn;
    WalCollection m_set;
    std::string m_prefix;

    Storage *m_store {};
    System *m_system {};
    std::optional<WalReader> m_reader;
    std::optional<WalWriter> m_writer;
    std::optional<WalCleaner> m_cleaner;
    std::string m_reader_data;
    std::string m_reader_tail;
    std::string m_writer_tail;
    Size m_wal_limit {};

    // If this is true, both m_writer and m_cleaner should exist. Otherwise, neither should exist. The two
    // groups should never overlap.
    bool m_is_working {};
};

} // namespace calico

#endif // CALICO_WAL_BASIC_WAL_H
