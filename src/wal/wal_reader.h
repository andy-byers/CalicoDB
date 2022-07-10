/**
 *
 * References
 *   (1) https://github.com/facebook/rocksdb/wiki/Write-Ahead-Log-IFile-Format
 */

#ifndef CALICO_WAL_WAL_READER_H
#define CALICO_WAL_WAL_READER_H

#include <memory>
#include <stack>
#include <spdlog/logger.h>
#include "interface.h"
#include "wal_record.h"

namespace calico {

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
        Size block_size {};
    };
    explicit WALReader(Parameters);
    ~WALReader() override = default;
    [[nodiscard]] auto record() const -> std::optional<WALRecord> override;
    auto increment() -> bool override;
    auto decrement() -> bool override;
    auto reset() -> void override;

    [[nodiscard]] auto noex_record() const -> Result<WALRecord> override;
    [[nodiscard]] auto noex_increment() -> Result<bool> override;
    [[nodiscard]] auto noex_decrement() -> Result<bool> override;
    [[nodiscard]] auto noex_reset() -> Result<void> override;

private:
    auto read_block() -> bool;
    auto read_record() -> std::optional<WALRecord>;
    auto read_record_aux(Index) -> std::optional<WALRecord>;
    auto read_next() -> std::optional<WALRecord>;
    auto read_previous() -> std::optional<WALRecord>;
    auto push_position() -> void;
    auto pop_position_and_seek() -> void;

    [[nodiscard]] auto noex_read_next() -> Result<std::optional<WALRecord>>; // TODO: Try to not use optional so much if possible. Seems unnecessary in some cases.
    [[nodiscard]] auto noex_read_previous() -> Result<std::optional<WALRecord>>;
    [[nodiscard]] auto noex_read_record() -> Result<std::optional<WALRecord>>;
    [[nodiscard]] auto noex_read_record_aux(Index) -> Result<std::optional<WALRecord>>;
    [[nodiscard]] auto noex_pop_position_and_seek() -> Result<void>;
    [[nodiscard]] auto noex_try_pop_position_and_seek() -> Result<void>;
    [[nodiscard]] auto noex_read_block() -> Result<bool>;

    std::vector<Index> m_positions;        ///< Stack containing the absolute offset of each record we have read so far
    std::string m_block;                   ///< Tail buffer for caching WAL block contents
    std::unique_ptr<IFile> m_file;
    std::unique_ptr<IFileReader> m_reader;
    std::shared_ptr<spdlog::logger> m_logger;
    std::optional<WALRecord> m_record;     ///< Record that the cursor is currently over
    Index m_block_id {};                   ///< Index of the current block in the WAL storage
    Index m_cursor {};                     ///< Offset of the current record in the tail buffer
    bool m_has_block {};                   ///< True if the tail buffer contains a block, false otherwise
    bool m_incremented {};
};

} // calico

#endif // CALICO_WAL_WAL_READER_H
