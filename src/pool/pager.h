#ifndef CCO_POOL_PAGER_H
#define CCO_POOL_PAGER_H

#include "calico/status.h"
#include "frame.h"
#include "page/file_header.h"
#include "utils/identifier.h"
#include "utils/result.h"
#include "utils/types.h"
#include <list>
#include <memory>
#include <optional>
#include <spdlog/spdlog.h>

namespace cco {

class FileHeader;
class RandomAccessEditor;

class Pager final {
public:
    ~Pager() = default;
    [[nodiscard]] static auto open(std::unique_ptr<RandomAccessEditor>, Size, Size) -> Result<std::unique_ptr<Pager>>;
    [[nodiscard]] auto pin(PageId) -> Result<FrameId>;
    [[nodiscard]] auto unpin(FrameId) -> Status;
    [[nodiscard]] auto sync() -> Status;
    [[nodiscard]] auto ref(FrameId, IBufferPool*, bool) -> Page;
    auto unref(FrameId, Page&) -> void;
    auto discard(FrameId) -> void;
    auto load_state(const FileHeader&) -> void;
    auto save_state(FileHeader&) -> void;

    [[nodiscard]]
    auto flushed_lsn() -> SequenceNumber
    {
        return m_flushed_lsn;
    }

    [[nodiscard]]
    auto page_count() const -> Size
    {
        return m_page_count;
    }
    
    [[nodiscard]]
    auto available() const -> Size
    {
        return m_available.size();
    }
    
    [[nodiscard]]
    auto page_size() const -> Size
    {
        return m_page_size;
    }

    auto operator=(Pager &&) -> Pager & = default;
    Pager(Pager &&) = default;

private:
    Pager(std::unique_ptr<RandomAccessEditor>, AlignedBuffer, Size, Size);
    [[nodiscard]] auto read_page_from_file(PageId, Bytes) const -> Result<bool>;
    [[nodiscard]] auto write_page_to_file(PageId, BytesView) const -> Status;

    [[nodiscard]] auto frame_at(FrameId id) const -> const Frame&
    {
        CCO_EXPECT_LT(id.as_index(), m_frame_count);
        return m_frames[id.as_index()];
    }

    [[nodiscard]] auto frame_at(FrameId id) -> Frame&
    {
        CCO_EXPECT_LT(id.as_index(), m_frame_count);
        return m_frames[id.as_index()];
    }

    AlignedBuffer m_buffer;
    std::vector<Frame> m_frames;
    std::list<FrameId> m_available;
    std::unique_ptr<RandomAccessEditor> m_file;
    SequenceNumber m_flushed_lsn;
    Size m_frame_count {};
    Size m_page_count {};
    Size m_page_size {};
};

} // namespace cco

#endif // CCO_POOL_PAGER_H
