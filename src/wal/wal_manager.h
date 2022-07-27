#ifndef CCO_WAL_WAL_MANAGER_H
#define CCO_WAL_WAL_MANAGER_H

#include "interface.h"
#include "page/update.h"
#include "utils/tracker.h"

namespace cco {

class IDirectory;
class WALExplorer;

class WALManager: public IWALManager {
public:
    [[nodiscard]] static auto open(const WALParameters&) -> Result<std::unique_ptr<IWALManager>>;
    [[nodiscard]] auto close() -> Result<void> override;
    [[nodiscard]] auto has_records() const -> bool override;
    [[nodiscard]] auto flushed_lsn() const -> LSN override;
    [[nodiscard]] auto truncate() -> Result<void> override;
    [[nodiscard]] auto flush() -> Result<void> override;
    [[nodiscard]] auto append(Page&) -> Result<void> override;
    [[nodiscard]] auto recover() -> Result<void> override;
    [[nodiscard]] auto abort() -> Result<void> override;
    [[nodiscard]] auto commit() -> Result<void> override;
    auto discard(Page&) -> void override;
    auto track(Page&) -> void override;
    auto save_header(FileHeaderWriter&) -> void override;
    auto load_header(const FileHeaderReader&) -> void override;
private:
    explicit WALManager(const WALParameters&);
    [[nodiscard]] auto roll_forward() -> Result<bool>;
    [[nodiscard]] auto roll_backward() -> Result<void>;
    [[nodiscard]] auto read_next(WALExplorer&) -> Result<WALRecord>;

    Tracker m_tracker;
    std::unique_ptr<IWALReader> m_reader;
    std::unique_ptr<IWALWriter> m_writer;
    std::shared_ptr<spdlog::logger> m_logger;
    std::vector<Position> m_positions;
    IBufferPool *m_pool {};
};

} // cco

#endif // CCO_WAL_WAL_MANAGER_H
