#ifndef CALICO_WAL_BASIC_WAL_H
#define CALICO_WAL_BASIC_WAL_H

#include "cleaner.h"
#include "helpers.h"
#include "reader.h"
#include "record.h"
#include "wal.h"
#include "writer.h"
#include "utils/result.h"
#include "utils/types.h"
#include <atomic>
#include <optional>
#include <unordered_set>

namespace calico {

class Storage;
class AppendWriter;
class RandomReader;
class BasicWalCleaner;

class BasicWriteAheadLog: public WriteAheadLog {
public:
    struct Parameters {
        std::string prefix;
        Storage *store {};
        spdlog::sink_ptr sink;
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

    auto allow_cleanup(std::uint64_t pager_lsn) -> void override;

    ~BasicWriteAheadLog() override;
    [[nodiscard]] static auto open(const Parameters&, WriteAheadLog**) -> Status;
    [[nodiscard]] auto status() const -> Status override;
    [[nodiscard]] auto flushed_lsn() const -> std::uint64_t override;
    [[nodiscard]] auto current_lsn() const -> std::uint64_t override;
    [[nodiscard]] auto stop_workers() -> Status override;
    [[nodiscard]] auto start_workers() -> Status override;
    [[nodiscard]] auto log(std::uint64_t page_id, BytesView image) -> Status override;
    [[nodiscard]] auto log(std::uint64_t page_id, BytesView image, const std::vector<PageDelta> &deltas) -> Status override;
    [[nodiscard]] auto commit() -> Status override;
    [[nodiscard]] auto start_recovery(const GetDeltas &delta_cb, const GetFullImage &image_cb) -> Status override;
    [[nodiscard]] auto finish_recovery() -> Status override;
    [[nodiscard]] auto start_abort(const GetFullImage &callback) -> Status override;
    [[nodiscard]] auto finish_abort() -> Status override;

private:
    explicit BasicWriteAheadLog(const Parameters &param);
    [[nodiscard]] auto stop_workers_impl() -> Status;
    [[nodiscard]] auto open_reader() -> Status;

    auto forward_status(Status s, const std::string &message) -> Status
    {
        if (!s.is_ok()) {
            m_logger->error(message);
            m_logger->error("(reason) {}", s.what());
        }
        return s;
    }

    std::unordered_set<PageId, PageId::Hash> m_images;
    std::shared_ptr<spdlog::logger> m_logger;
    std::atomic<SequenceId> m_flushed_lsn {};
    std::atomic<SequenceId> m_pager_lsn {};
    SequenceId m_last_lsn;
    SegmentId m_commit_id;
    LogScratchManager m_scratch;
    WalCollection m_collection;
    std::string m_prefix;

    Storage *m_store {};
    Status m_status {Status::ok()};
    std::optional<WalReader> m_reader;
    std::optional<WalWriter> m_writer;
//    std::optional<BasicWalCleaner> m_cleaner;
    std::string m_reader_data;
    std::string m_reader_tail;
    std::string m_writer_tail;
    Size m_wal_limit {};

    // If this is true, both m_writer and m_cleaner should exist. Otherwise, m_reader should exist. The two
    // groups should never overlap.
    bool m_is_working {};
};

} // namespace calico

#endif // CALICO_WAL_BASIC_WAL_H
