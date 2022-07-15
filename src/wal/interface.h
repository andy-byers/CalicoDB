#ifndef CCO_WAL_INTERFACE_H
#define CCO_WAL_INTERFACE_H

#include <optional>
#include "calico/error.h"

namespace cco {

struct LSN;
struct PID;
class WALRecord;

namespace page {
    class Page;
} // page

constexpr auto WAL_NAME = "wal";

class IWALWriter {
public:
    virtual ~IWALWriter() = default;
    [[nodiscard]] virtual auto flushed_lsn() const -> LSN = 0;
    [[nodiscard]] virtual auto has_pending() const -> bool = 0;
    [[nodiscard]] virtual auto has_committed() const -> bool = 0;
    [[nodiscard]] virtual auto append(page::Page&) -> Result<void> = 0;
    [[nodiscard]] virtual auto truncate() -> Result<void> = 0;
    [[nodiscard]] virtual auto flush() -> Result<void> = 0;
    virtual auto post(page::Page&) -> void = 0;
    virtual auto discard(PID) -> void = 0;
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
