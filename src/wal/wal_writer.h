// Startup Conditions:
//     WAL file not found
//     WAL file found:
//         WAL file empty
//         WAL file not empty:
//             (a) Commit record found
//             (b) Commit record not found
//
// In (a), we have enough information in the WAL to complete the failed transaction. We roll
// the log forward until all updates are saved in the database. In (b), we must have failed
// before some part of the transaction was flushed to the WAL. In this case we must roll the
// log backward until the beginning. This will make the database consistent with its state
// after the last commit.

#ifndef CUB_WAL_WAL_WRITER_H
#define CUB_WAL_WAL_WRITER_H

#include <memory>
#include "interface.h"

namespace cub {

class ILogFile;
class WALRecord;

class WALWriter: public IWALWriter {
public:
    WALWriter(std::unique_ptr<ILogFile>, Size);
    ~WALWriter() override = default;
    [[nodiscard]] auto block_size() const -> Size override;
    [[nodiscard]] auto has_pending() const -> bool override;
    [[nodiscard]] auto has_committed() const -> bool override;
    auto write(WALRecord) -> LSN override;
    auto truncate() -> void override;
    auto flush() -> LSN override;

private:
    std::unique_ptr<ILogFile> m_file; ///< Write-only handle to the WAL file
    std::string m_block;              ///< Log tail buffer
    Index m_cursor{};                 ///< Position in the tail buffer
    LSN m_last_lsn;
};

} // Cub

#endif // CUB_WAL_WAL_WRITER_H
