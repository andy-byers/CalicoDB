#ifndef CALICO_WAL_INTERFACE_H
#define CALICO_WAL_INTERFACE_H

#include <optional>
#include "calico/options.h"

namespace calico {

class WALRecord;
struct LSN;

constexpr auto WAL_NAME = "wal";

// TODO: WAL segmentation.
// constexpr auto WAL_PREFIX = "wal-";

class IWALWriter {
public:
    virtual ~IWALWriter() = default;
    [[nodiscard]] virtual auto block_size() const -> Size = 0;
    [[nodiscard]] virtual auto has_pending() const -> bool = 0;
    [[nodiscard]] virtual auto has_committed() const -> bool = 0;
    virtual auto append(WALRecord) -> LSN = 0;
    virtual auto truncate() -> void = 0;
    virtual auto flush() -> LSN = 0;

    [[nodiscard]] virtual auto noex_append(WALRecord) -> Result<LSN> = 0;
    [[nodiscard]] virtual auto noex_truncate() -> Result<void> = 0;
    [[nodiscard]] virtual auto noex_flush() -> Result<LSN> = 0;
};


class IWALReader {
public:
    virtual ~IWALReader() = default;
    [[nodiscard]] virtual auto record() const -> std::optional<WALRecord> = 0;
    virtual auto increment() -> bool = 0;
    virtual auto decrement() -> bool = 0;
    virtual auto reset() -> void = 0;


    [[nodiscard]] virtual auto noex_record() const -> Result<WALRecord> = 0;
    [[nodiscard]] virtual auto noex_increment() -> Result<bool> = 0;
    [[nodiscard]] virtual auto noex_decrement() -> Result<bool> = 0;
    [[nodiscard]] virtual auto noex_reset() -> Result<void> = 0;
};

} // calico

#endif //CALICO_WAL_INTERFACE_H
