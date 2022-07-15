/**
*
* References
*   (1) https://github.com/facebook/rocksdb/wiki/Write-Ahead-Log-IFile-Format
*/

#ifndef CCO_WAL_WAL_READER_H
#define CCO_WAL_WAL_READER_H

#include <memory>
#include <stack>
#include <spdlog/logger.h>
#include "interface.h"
#include "wal_record.h"

namespace cco {

class IDirectory;
class IFile;
class IFileReader;
struct LSN;

/**
* A cursor-like object for traversing the WAL storage.
*/
class WALReader: public IWALReader {
public:
   struct Parameters {
       IDirectory &directory;
       spdlog::sink_ptr log_sink;
       Size page_size {};
   };
   ~WALReader() override = default;
    [[nodiscard]] static auto open(const Parameters&) -> Result<std::unique_ptr<IWALReader>>;
    [[nodiscard]] auto record() const -> std::optional<WALRecord> override;
    auto increment() -> Result<bool> override;
    auto decrement() -> Result<bool> override;
    auto reset() -> Result<void> override;

private:
    WALReader(std::unique_ptr<IFile>, Parameters);
    auto read_block() -> Result<bool>;
    auto read_record() -> Result<std::optional<WALRecord>>;
    auto read_record_aux(Index) -> Result<std::optional<WALRecord>>;
    auto read_next() -> Result<std::optional<WALRecord>>;
    auto read_previous() -> Result<std::optional<WALRecord>>;
    auto push_position() -> void;
    auto pop_position_and_seek() -> Result<void>;

    std::vector<Index> m_positions;
    std::string m_block;
    std::unique_ptr<IFile> m_file;
    std::unique_ptr<IFileReader> m_reader;
    std::shared_ptr<spdlog::logger> m_logger;
    std::optional<WALRecord> m_record;
    Index m_block_id {};
    Index m_cursor {};
    bool m_has_block {};
    bool m_incremented {};
};

} // cco

#endif // CCO_WAL_WAL_READER_H