#ifndef CCO_POOL_PAGER_H
#define CCO_POOL_PAGER_H

#include "calico/status.h"
#include <list>
#include <memory>
#include <optional>
#include <spdlog/spdlog.h>

namespace cco {

class Frame;
class IFile;
struct PID;

class Pager final {
public:
    struct Parameters {
        std::unique_ptr<IFile> file;
        Size page_size {};
        Size frame_count {};
    };

    struct AlignedDeleter {

        explicit AlignedDeleter(std::align_val_t alignment)
            : align {alignment} {}

        auto operator()(Byte *ptr) const -> void
        {
            operator delete[](ptr, align);
        }

        std::align_val_t align;
    };

    using AlignedBuffer = std::unique_ptr<Byte[], AlignedDeleter>;

    ~Pager() = default;
    [[nodiscard]] static auto open(Parameters) -> Result<std::unique_ptr<Pager>>;
    [[nodiscard]] auto available() const -> Size;
    [[nodiscard]] auto page_size() const -> Size;
    [[nodiscard]] auto pin(PID) -> Result<Frame>;
    [[nodiscard]] auto unpin(Frame) -> Result<void>;
    [[nodiscard]] auto truncate(Size) -> Result<void>;
    [[nodiscard]] auto sync() -> Result<void>;
    auto discard(Frame) -> void;

    auto operator=(Pager&&) -> Pager& = default;
    Pager(Pager&&) = default;

private:
    Pager(AlignedBuffer, Parameters);
    [[nodiscard]] auto read_page_from_file(PID, Bytes) const -> Result<bool>;
    [[nodiscard]] auto write_page_to_file(PID, BytesView) const -> Result<void>;

    AlignedBuffer m_buffer;
    std::list<Frame> m_available;
    std::unique_ptr<IFile> m_file;
    Size m_frame_count{};
    Size m_page_size{};
};

} // calico

#endif // CCO_POOL_PAGER_H
