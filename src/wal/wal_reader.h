/**
*
* References
*   (1) https://github.com/facebook/rocksdb/wiki/Write-Ahead-Log-IFile-Format
*/

#ifndef CCO_WAL_WAL_READER_H
#define CCO_WAL_WAL_READER_H

#include "interface.h"
#include <memory>
#include <spdlog/logger.h>
#include <stack>

namespace cco {

class IDirectory;
class IFile;
struct LSN;

class WALReader : public IWALReader {
public:
    ~WALReader() override = default;
    [[nodiscard]] static auto open(const WALParameters &) -> Result<std::unique_ptr<IWALReader>>;
    [[nodiscard]] auto read(Position &) -> Result<WALRecord> override;
    [[nodiscard]] auto close() -> Result<void> override;
    auto reset() -> void override;

private:
    WALReader(std::unique_ptr<IFile>, const WALParameters &);
    [[nodiscard]] auto read_block(Index) -> Result<bool>;
    [[nodiscard]] auto read_record(Index) -> Result<WALRecord>;

    std::string m_block;
    std::unique_ptr<IFile> m_file;
    Index m_block_id {};
    bool m_has_block {};
};

class WALExplorer final {
public:
    using Position = IWALReader::Position;

    struct Discovery {
        WALRecord record;
        Position position;
    };

    ~WALExplorer() = default;
    explicit WALExplorer(IWALReader &);
    [[nodiscard]] auto read_next() -> Result<Discovery>;
    auto reset() -> void;

private:
    Position m_position;
    IWALReader *m_reader {};
};

} // namespace cco

#endif // CCO_WAL_WAL_READER_H