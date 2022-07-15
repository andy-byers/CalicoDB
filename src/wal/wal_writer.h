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
class IFileWriter;
class WALRecord;

/**
 * A component that appends records to the WAL file.
 */
class WALWriter: public IWALWriter {
public:
    struct Parameters {
        IDirectory &directory;
        spdlog::sink_ptr log_sink;
        Size page_size {};
    };

    ~WALWriter() override = default;
    [[nodiscard]] static auto open(const Parameters&) -> Result<std::unique_ptr<IWALWriter>>;
    [[nodiscard]] auto append(page::Page&) -> Result<void> override;
    [[nodiscard]] auto truncate() -> Result<void> override;
    [[nodiscard]] auto flush() -> Result<void> override;
    auto post(page::Page&) -> void override;
    auto discard(PID) -> void override;

    [[nodiscard]] auto flushed_lsn() const -> LSN override
    {
        return m_flushed_lsn;
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
    WALWriter(std::unique_ptr<IFile>, const Parameters&);

    std::unordered_map<PID, page::UpdateManager, PID::Hasher> m_registry;
    std::unique_ptr<IFile> m_file;
    std::unique_ptr<IFileWriter> m_writer;
    std::shared_ptr<spdlog::logger> m_logger;
    utils::ScratchManager m_scratch;
    std::string m_block;
    Index m_cursor {};
    LSN m_previous_lsn;
    LSN m_flushed_lsn;
    bool m_has_committed {};
};

} // cco

#endif // CALICO_WAL_WAL_WRITER_H