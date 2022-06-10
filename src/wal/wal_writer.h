#ifndef CUB_WAL_WAL_WRITER_H
#define CUB_WAL_WAL_WRITER_H

#include <memory>
#include "interface.h"
#include "utils/identifier.h"

namespace cub {

class ILogFile;
class WALRecord;

/**
 * A writer that appends records to the WAL file.
 */
class WALWriter: public IWALWriter {
public:
    WALWriter(std::unique_ptr<ILogFile>, Size);
    ~WALWriter() override = default;
    [[nodiscard]] auto has_committed() const -> bool override;

    /**
     * Determine if there is data waiting to be flushed to disk.
     *
     * @return True if there is data in the tail buffer, false otherwise.
     */
    [[nodiscard]] auto has_pending() const -> bool override
    {
        return m_cursor > 0;
    }

    /**
     * Get the WAL block size.
     *
     * @return The size of a WAL block
     */
    [[nodiscard]] auto block_size() const -> Size override
    {
        return m_block.size();
    }

    auto append(WALRecord record) -> LSN override;
    auto truncate() -> void override;
    auto flush() -> LSN override;

private:
    std::unique_ptr<ILogFile> m_file; ///< Write-only WAL file handle
    std::string m_block;              ///< Tail buffer for holding the current block
    Index m_cursor {};                ///< Position in the tail buffer
    LSN m_last_lsn;
};

} // cub

#endif // CUB_WAL_WAL_WRITER_H
