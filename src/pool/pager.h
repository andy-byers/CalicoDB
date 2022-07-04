#ifndef CALICO_POOL_PAGER_H
#define CALICO_POOL_PAGER_H

#include <list>
#include <memory>
#include <optional>
#include "calico/bytes.h"

namespace calico {

class Frame;
class IFileReader;
class IFileWriter;
struct PID;

class Pager final {
public:
    struct Parameters {
        std::unique_ptr<IFileReader> reader;
        std::unique_ptr<IFileWriter> writer;
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
    auto sync() -> void;

private:
    [[nodiscard]] auto try_read_page_from_file(PID, Bytes) const -> bool;
    auto write_page_to_file(PID, BytesView) const -> void;

    std::list<Frame> m_available;           ///< List of available frames
    std::unique_ptr<IFileReader> m_reader;
    std::unique_ptr<IFileWriter> m_writer;
    Size m_frame_count{};
    Size m_page_size{};
};

} // calico

#endif // CALICO_POOL_PAGER_H
