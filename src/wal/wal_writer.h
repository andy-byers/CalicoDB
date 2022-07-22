#ifndef CALICO_WAL_WAL_WRITER_H
#define CALICO_WAL_WAL_WRITER_H

#include <memory>
#include <spdlog/logger.h>
#include "interface.h"
#include "page/update.h"
#include "utils/identifier.h"

namespace cco {

class IDirectory;
class IFile;
class WALRecord;

/**
 * A component that appends records to the WAL file.
 */
class WALWriter: public IWALWriter {
public:
    ~WALWriter() override = default;
    [[nodiscard]] static auto open(const WALParameters&) -> Result<std::unique_ptr<IWALWriter>>;
    [[nodiscard]] auto close() -> Result<void> override;
    [[nodiscard]] auto append(WALRecord) -> Result<Position> override;
    [[nodiscard]] auto truncate() -> Result<void> override;
    [[nodiscard]] auto flush() -> Result<void> override;

    auto set_flushed_lsn(LSN flushed_lsn) -> void override
    {
        m_flushed_lsn = flushed_lsn;
        m_last_lsn = flushed_lsn;
    }

    [[nodiscard]] auto flushed_lsn() const -> LSN override
    {
        return m_flushed_lsn;
    }

    [[nodiscard]] auto last_lsn() const -> LSN override
    {
        return m_last_lsn;
    }

    [[nodiscard]] auto has_pending() const -> bool override
    {
        return m_cursor > 0;
    }

    [[nodiscard]] auto has_committed() const -> bool override
    {
        return m_has_committed;
    }

private:
    WALWriter(std::unique_ptr<IFile>, const WALParameters&);

    std::unique_ptr<IFile> m_file;
    std::string m_block;
    Position m_position {};
    Index m_cursor {};
    LSN m_flushed_lsn;
    LSN m_last_lsn;
    bool m_has_committed {};
};

} // cco

#endif // CALICO_WAL_WAL_WRITER_H