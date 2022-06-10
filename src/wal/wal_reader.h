/**
 *
 * References
 *   (1) https://github.com/facebook/rocksdb/wiki/Write-Ahead-Log-IFile-Format
 */

#ifndef CUB_WAL_WAL_READER_H
#define CUB_WAL_WAL_READER_H

#include <memory>
#include <stack>
#include "interface.h"
#include "wal_record.h"

namespace cub {

class IReadOnlyFile;
struct LSN;

/**
 * A cursor-like object for traversing the WAL file.
 */
class WALReader: public IWALReader {
public:
    WALReader(std::unique_ptr<IReadOnlyFile>, Size);
    ~WALReader() override = default;
    [[nodiscard]] auto record() const -> std::optional<WALRecord> override;
    auto increment() -> bool override;
    auto decrement() -> bool override;
    auto reset() -> void override;

private:
    auto read_block() -> bool;
    auto read_record() -> std::optional<WALRecord>;
    auto read_record_aux(Index) -> std::optional<WALRecord>;
    auto read_next() -> std::optional<WALRecord>;
    auto read_previous() -> std::optional<WALRecord>;
    auto push_position() -> void;
    auto pop_position_and_seek() -> void;

    std::vector<Index> m_positions;        ///< Stack containing the absolute offset of each record we have read so far
    std::string m_block;                   ///< Tail buffer for caching WAL block contents
    std::unique_ptr<IReadOnlyFile> m_file; ///< Read-only WAL file handle
    std::optional<WALRecord> m_record;     ///< Record that the cursor is currently over
    Index m_block_id {};                   ///< Index of the current block in the WAL file
    Index m_cursor {};                     ///< Offset of the current record in the tail buffer
    bool m_has_block {};                   ///< True if the tail buffer contains a block, false otherwise
    bool m_incremented {};
};

} // cub

#endif // CUB_WAL_WAL_READER_H
