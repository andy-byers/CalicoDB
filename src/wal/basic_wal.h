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

namespace Calico {

class Storage;
class AppendWriter;
class RandomReader;
class WalCleaner;

class BasicWriteAheadLog: public WriteAheadLog {
public:
    using Interval = std::chrono::duration<double>;

    struct Parameters {
        std::string prefix;
        Storage *store {};
        System *system {};
        Size page_size {};
        Size wal_limit {};
        Size writer_capacity {};
        Interval interval {};
    };

    [[nodiscard]]
    auto is_enabled() const -> bool override
    {
        return m_wal_limit != DISABLE_WAL;
    }

    ~BasicWriteAheadLog() override;
    [[nodiscard]] static auto open(const Parameters &param) -> tl::expected<WriteAheadLog::Ptr, Status>;
    [[nodiscard]] auto flushed_lsn() const -> Id override;
    [[nodiscard]] auto current_lsn() const -> Id override;
    [[nodiscard]] auto roll_forward(Id begin_lsn, const Callback &callback) -> Status override;
    [[nodiscard]] auto roll_backward(Id end_lsn, const Callback &callback) -> Status override;
    [[nodiscard]] auto remove_after(Id lsn) -> Status override;
    auto remove_before(Id lsn) -> void override;
    auto advance() -> void override;
    auto log(WalPayloadIn payload) -> void override;
    auto flush() -> void override;

private:
    explicit BasicWriteAheadLog(const Parameters &param);
    [[nodiscard]] auto open_reader() -> Status; // TODO: We don't really have to store the reader as a member...

    LogPtr m_log;
    std::atomic<Id> m_flushed_lsn {};
    Id m_last_lsn;
    WalSet m_set;
    std::string m_prefix;

    Storage *m_store {};
    System *m_system {};
    std::optional<WalReader> m_reader;
    std::string m_reader_data;
    std::string m_reader_tail;
    std::string m_writer_tail;
    Size m_wal_limit {};
    Size m_writer_capacity {};

    // If this is true, both m_writer and m_cleaner should exist. Otherwise, neither should exist. The two
    // groups should never overlap.
    bool m_is_working {};

    WalWriterTask m_writer_task;
    WalCleanupTask m_cleanup_task;
    TaskManager m_tasks; // TODO: Could store this in Core and pass it in as a parameter if we need it elsewhere.
};

} // namespace Calico

#endif // CALICO_WAL_BASIC_WAL_H
