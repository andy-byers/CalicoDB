#ifndef CALICO_WAL_BASIC_WAL_H
#define CALICO_WAL_BASIC_WAL_H

#include "helpers.h"
#include "reader.h"
#include "utils/result.h"
#include "utils/types.h"
#include "wal.h"
#include "wal/record.h"
#include "writer.h"
#include <atomic>
#include <optional>
#include <unordered_set>

namespace calico {

class Storage;
class AppendWriter;
class RandomReader;

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
        return true;
    }

    [[nodiscard]]
    auto is_working() const -> bool override
    {
        return m_writer.is_running();
    }

    auto allow_cleanup(std::uint64_t pager_lsn) -> void override
    {
        // TODO: m_pager_lsn is the largest page LSN written back to the database file. We can delete WAL segments up to this point. We could
        //       do this in a thread separate from the background writer, with some synchronization to make sure we aren't messing with the
        //       segment being written.
        const auto before = m_pager_lsn.load();
        m_pager_lsn.store(SequenceId {std::max(before, SequenceId {pager_lsn})});
    }

    ~BasicWriteAheadLog() override;
    [[nodiscard]] static auto open(const Parameters&, WriteAheadLog**) -> Status;
    [[nodiscard]] auto flushed_lsn() const -> std::uint64_t override;
    [[nodiscard]] auto current_lsn() const -> std::uint64_t override;
    [[nodiscard]] auto log_image(std::uint64_t page_id, BytesView image) -> Status override;
    [[nodiscard]] auto log_deltas(std::uint64_t page_id, BytesView image, const std::vector<PageDelta> &deltas) -> Status override;
    [[nodiscard]] auto log_commit() -> Status override;
    [[nodiscard]] auto stop_workers() -> Status override;
    [[nodiscard]] auto start_workers() -> Status override;
    [[nodiscard]] auto flush_pending() -> Status override;
    [[nodiscard]] auto open_iterator(WalIterator**) -> Status override {return Status::ok();} // TODO: Implement me!


    [[nodiscard]] auto setup_and_recover(const RedoCallback &redo_cb, const UndoCallback &undo_cb) -> Status override;
    [[nodiscard]] auto abort_last(const UndoCallback &callback) -> Status override;

private:
    explicit BasicWriteAheadLog(const Parameters &param);

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
    WalCollection m_collection;
    std::string m_prefix;
    Storage *m_store {};

    BasicWalReader m_reader;
    BasicWalWriter m_writer;
};

} // namespace calico

#endif // CALICO_WAL_BASIC_WAL_H
