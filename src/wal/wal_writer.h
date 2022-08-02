#ifndef CALICO_WAL_WAL_WRITER_H
#define CALICO_WAL_WAL_WRITER_H

#include "interface.h"
#include "page/update.h"
#include "utils/identifier.h"
#include <memory>
#include <spdlog/logger.h>

namespace cco {

class IDirectory;
class IFile;
class WALRecord;

/**
 * A component that appends records to the WAL file.
 */
class WALWriter : public IWALWriter {
public:
    ~WALWriter() override = default;
    [[nodiscard]] static auto create(const WALParameters &) -> Result<std::unique_ptr<IWALWriter>>;
    [[nodiscard]] auto needs_segmentation() -> bool override;
    [[nodiscard]] auto is_open() -> bool override;
    [[nodiscard]] auto open(std::unique_ptr<IFile>) -> Result<void> override;
    [[nodiscard]] auto close() -> Result<void> override;
    [[nodiscard]] auto append(WALRecord) -> Result<Position> override;
    [[nodiscard]] auto truncate() -> Result<void> override;
    [[nodiscard]] auto flush() -> Result<void> override;

    auto set_flushed_lsn(SequenceNumber flushed_lsn) -> void override
    {
        m_flushed_lsn = flushed_lsn;
        m_last_lsn = flushed_lsn;
    }

    [[nodiscard]] auto flushed_lsn() const -> SequenceNumber override
    {
        return m_flushed_lsn;
    }

    [[nodiscard]] auto last_lsn() const -> SequenceNumber override
    {
        return m_last_lsn;
    }

    [[nodiscard]] auto has_pending() const -> bool override
    {
        return m_position.offset > 0;
    }

    [[nodiscard]] auto has_committed() const -> bool override
    {
        return m_position.block_id > 0;
    }

private:
    explicit WALWriter(const WALParameters &);

    std::unique_ptr<IFile> m_file;
    std::string m_tail;
    Position m_position {};
    SequenceNumber m_flushed_lsn;
    SequenceNumber m_last_lsn;
};

} // namespace cco

#endif // CALICO_WAL_WAL_WRITER_H