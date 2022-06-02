#ifndef CUB_POOL_PAGER_H
#define CUB_POOL_PAGER_H

#include <list>
#include <optional>
#include "common.h"

namespace cub {

class Frame;
class IReadWriteFile;

class Pager {
public:
    struct Parameters {
        std::unique_ptr<IReadWriteFile> database_file;
        Size page_size{};
        Size frame_count{};
    };
    explicit Pager(Parameters);
    ~Pager() = default;
    [[nodiscard]] auto available() const -> Size;
    [[nodiscard]] auto page_size() const -> Size;
    [[nodiscard]] auto pin(PID) -> std::optional<Frame>;
    auto unpin(Frame) -> void;
    auto discard(Frame) -> void;
    auto truncate(Size) -> void;

private:
    [[nodiscard]] auto try_read_page_from_file(PID, Bytes) const -> bool;
    auto write_page_to_file(PID, BytesView) const -> void;

    std::list<Frame> m_available;             ///< List of available frames
    std::unique_ptr<IReadWriteFile> m_file; ///< Read/write database file handle
    Size m_frame_count{};
    Size m_page_size{};
};

} // cub

#endif // CUB_POOL_PAGER_H
