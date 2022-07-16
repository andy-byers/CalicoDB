#ifndef CCO_WAL_INTERFACE_H
#define CCO_WAL_INTERFACE_H

#include "calico/status.h"
#include "utils/identifier.h"
#include <optional>

namespace cco {

class IBufferPool;
class IDirectory;
class WALRecord;

namespace page {
    class Page;
} // page

constexpr auto WAL_NAME = "wal";

struct WALParameters {
    IBufferPool *pool {};
    IDirectory &directory;
    Size page_size {};
    LSN flushed_lsn {};
};

class IWALManager {
public:
    virtual ~IWALManager() = default;
    [[nodiscard]] virtual auto has_records() const -> bool = 0;
    [[nodiscard]] virtual auto flushed_lsn() const -> LSN = 0;
    [[nodiscard]] virtual auto truncate() -> Result<void> = 0;
    [[nodiscard]] virtual auto flush() -> Result<void> = 0;
    [[nodiscard]] virtual auto append(page::Page&) -> Result<void> = 0;
    [[nodiscard]] virtual auto recover() -> Result<void> = 0;
    [[nodiscard]] virtual auto commit() -> Result<void> = 0;
    [[nodiscard]] virtual auto abort() -> Result<void> = 0;
    virtual auto post(page::Page&) -> void = 0;
};

class IWALWriter {
public:
    virtual ~IWALWriter() = default;
    [[nodiscard]] virtual auto flushed_lsn() const -> LSN = 0;
    [[nodiscard]] virtual auto last_lsn() const -> LSN = 0;
    [[nodiscard]] virtual auto has_pending() const -> bool = 0;
    [[nodiscard]] virtual auto has_committed() const -> bool = 0;
    [[nodiscard]] virtual auto append(WALRecord) -> Result<void> = 0;
    [[nodiscard]] virtual auto truncate() -> Result<void> = 0;
    [[nodiscard]] virtual auto flush() -> Result<void> = 0;
    virtual auto set_flushed_lsn(LSN) -> void = 0;
};

class IWALReader {
public:
    virtual ~IWALReader() = default;
    [[nodiscard]] virtual auto record() const -> std::optional<WALRecord> = 0;
    [[nodiscard]] virtual auto increment() -> Result<bool> = 0;
    [[nodiscard]] virtual auto decrement() -> Result<bool> = 0;
    [[nodiscard]] virtual auto reset() -> Result<void> = 0;
};

} // cco

#endif // CCO_WAL_INTERFACE_H
