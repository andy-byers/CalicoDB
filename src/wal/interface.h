#ifndef CUB_WAL_INTERFACE_H
#define CUB_WAL_INTERFACE_H

#include "common.h"

namespace cub {

class WALRecord;
struct LSN;

class IWALWriter {
public:
    virtual ~IWALWriter() = default;
    [[nodiscard]] virtual auto block_size() const -> Size = 0;
    [[nodiscard]] virtual auto has_pending() const -> bool = 0;
    [[nodiscard]] virtual auto has_committed() const -> bool = 0;
    virtual auto write(WALRecord) -> LSN = 0;
    virtual auto truncate() -> void = 0;
    virtual auto flush() -> LSN = 0;
};


class IWALReader {
public:
    virtual ~IWALReader() = default;
    [[nodiscard]] virtual auto record() const -> std::optional<WALRecord> = 0;
    virtual auto increment() -> bool = 0;
    virtual auto decrement() -> bool = 0;
    virtual auto reset() -> void = 0;
};

} // cub

#endif //CUB_WAL_INTERFACE_H
