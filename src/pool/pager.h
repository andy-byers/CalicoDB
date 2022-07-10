#ifndef CALICO_POOL_PAGER_H
#define CALICO_POOL_PAGER_H

#include <list>
#include <memory>
#include <optional>
#include <spdlog/spdlog.h>
#include "calico/error.h"

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
        spdlog::sink_ptr log_sink;
        Size page_size{};
        Size frame_count{};
    };

    explicit Pager(Parameters);
    ~Pager();
    [[nodiscard]] auto available() const -> Size;
    [[nodiscard]] auto page_size() const -> Size;
    [[nodiscard]] auto pin(PID) -> std::optional<Frame>;
    auto unpin(Frame) -> void;
    auto discard(Frame) -> void;
    auto truncate(Size) -> void;
    auto sync() -> void;

    [[nodiscard]] auto noex_pin(PID) -> Result<Frame>;
    [[nodiscard]] auto noex_unpin(Frame) -> Result<void>;
    [[nodiscard]] auto noex_discard(Frame) -> Result<void>;
    [[nodiscard]] auto noex_truncate(Size) -> Result<void>;
    [[nodiscard]] auto noex_sync() -> Result<void>;

private:
    [[nodiscard]] auto noex_read_page_from_file(PID, Bytes) const -> Result<bool>;
    [[nodiscard]] auto noex_write_page_to_file(PID, BytesView) const -> Result<void>;
    [[nodiscard]] auto noex_maybe_write_pending() -> Result<void>;

    [[nodiscard]] auto read_page_from_file(PID, Bytes) const -> bool;
    auto write_page_to_file(PID, BytesView) const -> void;
    auto do_write(PID, BytesView) const -> void;
    auto maybe_write_pending() -> void;

    struct AlignedDeleter {

        explicit AlignedDeleter(std::align_val_t alignment)
            : align {alignment} {}

        auto operator()(Byte *ptr) const -> void
        {
            operator delete[](ptr, align);
        }

        std::align_val_t align;
    };

    std::unique_ptr<Byte[], AlignedDeleter> m_buffer;
    std::list<Frame> m_available;           ///< List of available frames
    std::list<Frame> m_pending;           ///< List of available frames
    std::unique_ptr<IFileReader> m_reader;
    std::unique_ptr<IFileWriter> m_writer;
    std::shared_ptr<spdlog::logger> m_logger;
    Size m_frame_count{};
    Size m_page_size{};
};

} // calico

#endif // CALICO_POOL_PAGER_H
