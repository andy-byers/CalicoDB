#ifndef CCO_WAL_WAL_MANAGER_H
#define CCO_WAL_WAL_MANAGER_H

#include "interface.h"
#include "page/update.h"
#include "utils/scratch.h"

namespace cco {

class IDirectory;

class WALManager: public IWALManager {
public:
    [[nodiscard]] static auto open(const WALParameters&) -> Result<std::unique_ptr<IWALManager>>;
    [[nodiscard]] auto has_records() const -> bool override;
    [[nodiscard]] auto flushed_lsn() const -> LSN override;
    [[nodiscard]] auto truncate() -> Result<void> override;
    [[nodiscard]] auto flush() -> Result<void> override;
    [[nodiscard]] auto append(page::Page&) -> Result<void> override;
    [[nodiscard]] auto recover() -> Result<void> override;
    [[nodiscard]] auto abort() -> Result<void> override;
    [[nodiscard]] auto commit() -> Result<void> override;
    auto post(page::Page&) -> void override;

private:
    WALManager(std::unique_ptr<IWALReader> reader, std::unique_ptr<IWALWriter>, const WALParameters&);
    [[nodiscard]] auto roll_forward() -> Result<bool>;
    [[nodiscard]] auto roll_backward() -> Result<void>;

    std::unordered_map<PID, page::UpdateManager, PID::Hasher> m_registry;
    utils::ScratchManager m_scratch;
    std::unique_ptr<IWALReader> m_reader;
    std::unique_ptr<IWALWriter> m_writer;
    IBufferPool *m_pool {};
};

} // cco

#endif // CCO_WAL_WAL_MANAGER_H
