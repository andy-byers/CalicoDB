#ifndef CALICO_WAL_WAL_WRITER_H
#define CALICO_WAL_WAL_WRITER_H

#include <memory>
#include <spdlog/logger.h>
#include "interface.h"
#include "utils/identifier.h"

namespace calico {

class ILogFile;
class WALRecord;

/**
 * A writer that appends records to the WAL file.
 */
class WALWriter: public IWALWriter {
public:
    struct Parameters {
        std::string wal_path;
        std::unique_ptr<ILogFile> wal_file;
        spdlog::sink_ptr log_sink;
        Size block_size {};
    };
    explicit WALWriter(Parameters);
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
    std::shared_ptr<spdlog::logger> m_logger;
    std::string m_block;              ///< Tail buffer for holding the current block
    Index m_cursor {};                ///< Position in the tail buffer
    LSN m_last_lsn;
};

} // calico

#endif // CALICO_WAL_WAL_WRITER_H
