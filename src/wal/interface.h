#ifndef CCO_WAL_INTERFACE_H
#define CCO_WAL_INTERFACE_H

#include <optional>
#include "wal_record.h"
#include "page/file_header.h"
#include "utils/result.h"

namespace cco {

class IBufferPool;
class IDirectory;

namespace page {
    class Page;
} // page

constexpr auto WAL_NAME = "wal";

struct WALParameters {
    IBufferPool *pool {};
    IDirectory &directory;
    spdlog::sink_ptr log_sink;
    Size page_size {};
    LSN flushed_lsn {};
};

class IWALManager {
public:
    struct Position {
        Index block_id {};
        Index offset {};
    };


    virtual ~IWALManager() = default;
    [[nodiscard]] virtual auto has_records() const -> bool = 0;
    [[nodiscard]] virtual auto flushed_lsn() const -> LSN = 0;
    [[nodiscard]] virtual auto truncate() -> Result<void> = 0;
    [[nodiscard]] virtual auto flush() -> Result<void> = 0;
    [[nodiscard]] virtual auto append(Page&) -> Result<void> = 0;
    [[nodiscard]] virtual auto recover() -> Result<void> = 0;
    [[nodiscard]] virtual auto commit() -> Result<void> = 0;
    [[nodiscard]] virtual auto abort() -> Result<void> = 0;
    [[nodiscard]] virtual auto close() -> Result<void> = 0;
    virtual auto track(Page&) -> void = 0;
    virtual auto discard(Page&) -> void = 0;
    virtual auto save_header(FileHeaderWriter&) -> void = 0;
    virtual auto load_header(const FileHeaderReader&) -> void = 0;
};

class IWALWriter {
public:
    using Position = IWALManager::Position;
    virtual ~IWALWriter() = default;
    [[nodiscard]] virtual auto flushed_lsn() const -> LSN = 0;
    [[nodiscard]] virtual auto last_lsn() const -> LSN = 0;
    [[nodiscard]] virtual auto has_pending() const -> bool = 0;
    [[nodiscard]] virtual auto has_committed() const -> bool = 0;
    [[nodiscard]] virtual auto append(WALRecord) -> Result<Position> = 0;
    [[nodiscard]] virtual auto truncate() -> Result<void> = 0;
    [[nodiscard]] virtual auto flush() -> Result<void> = 0;
    [[nodiscard]] virtual auto close() -> Result<void> = 0;
    virtual auto set_flushed_lsn(LSN) -> void = 0;
};

class IWALReader {
public:
    using Position = IWALManager::Position;
    virtual ~IWALReader() = default;
    [[nodiscard]] virtual auto read(Position&) -> Result<WALRecord> = 0;
    [[nodiscard]] virtual auto close() -> Result<void> = 0;
    virtual auto reset() -> void = 0;
};

} // cco

#endif // CCO_WAL_INTERFACE_H
