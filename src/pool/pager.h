#ifndef CCO_POOL_PAGER_H
#define CCO_POOL_PAGER_H

#include "calico/status.h"
#include "page/file_header.h"
#include "utils/identifier.h"
#include "utils/result.h"
#include "utils/types.h"
#include <list>
#include <memory>
#include <optional>
#include <spdlog/spdlog.h>

namespace cco {

class Frame;
class IFile;

class Pager final {
public:
    struct Parameters {
        std::unique_ptr<IFile> file;
        SequenceNumber flushed_lsn;
        Size page_size {};
        Size frame_count {};
    };

    ~Pager() = default;
    [[nodiscard]] static auto open(Parameters) -> Result<std::unique_ptr<Pager>>;
    [[nodiscard]] auto close() -> Result<void>;
    [[nodiscard]] auto available() const -> Size;
    [[nodiscard]] auto page_size() const -> Size;
    [[nodiscard]] auto pin(PageId) -> Result<Frame>;
    [[nodiscard]] auto unpin(Frame) -> Result<void>;
    [[nodiscard]] auto clean(Frame &) -> Result<void>;
    [[nodiscard]] auto truncate(Size) -> Result<void>;
    [[nodiscard]] auto sync() -> Result<void>;
    auto discard(Frame) -> void;
    auto load_header(const FileHeaderReader&) -> void;
    auto save_header(FileHeaderWriter&) -> void;

    [[nodiscard]] auto flushed_lsn() -> SequenceNumber
    {
        return m_flushed_lsn;
    }

    auto operator=(Pager &&) -> Pager & = default;
    Pager(Pager &&) = default;

private:
    Pager(AlignedBuffer, Parameters);
    [[nodiscard]] auto read_page_from_file(PageId, Bytes) const -> Result<bool>;
    [[nodiscard]] auto write_page_to_file(PageId, BytesView) const -> Result<void>;

    AlignedBuffer m_buffer;
    std::list<Frame> m_available;
    std::unique_ptr<IFile> m_file;
    SequenceNumber m_flushed_lsn;
    Size m_frame_count {};
    Size m_page_size {};
};

} // namespace cco

#endif // CCO_POOL_PAGER_H
