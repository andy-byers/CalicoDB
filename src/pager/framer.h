#ifndef CCO_POOL_PAGER_H
#define CCO_POOL_PAGER_H

#include "calico/status.h"
#include "utils/identifier.h"
#include "utils/result.h"
#include "utils/types.h"
#include <list>
#include <memory>
#include <optional>
#include <spdlog/spdlog.h>

namespace cco {

struct FileHeader;
class Page;
class Pager;
class RandomAccessEditor;

using FrameId = Identifier<std::uint64_t>;

class Frame final {
public:
    Frame(Byte *, Index, Size);

    [[nodiscard]]
    auto pid() const -> PageId
    {
        return m_page_id;
    }

    [[nodiscard]]
    auto ref_count() const -> Size
    {
        return m_ref_count;
    }

    [[nodiscard]]
    auto data() const -> BytesView
    {
        return m_bytes;
    }

    [[nodiscard]]
    auto data() -> Bytes
    {
        return m_bytes;
    }

    auto reset(PageId id) -> void
    {
        CCO_EXPECT_EQ(m_ref_count, 0);
        m_page_id = id;
    }

    [[nodiscard]] auto lsn() const -> SequenceNumber;
    [[nodiscard]] auto ref(Pager&, bool, bool) -> Page;
    auto unref(Page &page) -> void;

private:
    Bytes m_bytes;
    PageId m_page_id;
    Size m_ref_count {};
    bool m_is_writable {};
};

class Framer final {
public:
    ~Framer() = default;
    [[nodiscard]] static auto open(std::unique_ptr<RandomAccessEditor>, Size, Size) -> Result<std::unique_ptr<Framer>>;
    [[nodiscard]] auto pin(PageId) -> Result<FrameId>;
    [[nodiscard]] auto unpin(FrameId, bool) -> Status;
    [[nodiscard]] auto sync() -> Status;
    [[nodiscard]] auto ref(FrameId, Pager&, bool, bool) -> Page;
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
    auto frame_at(FrameId id) const -> const Frame&
    {
        CCO_EXPECT_LT(id.as_index(), m_frame_count);
        return m_frames[id.as_index()];
    }

    auto operator=(Framer &&) -> Framer & = default;
    Framer(Framer &&) = default;

private:
    Framer(std::unique_ptr<RandomAccessEditor>, AlignedBuffer, Size, Size);
    [[nodiscard]] auto read_page_from_file(PageId, Bytes) const -> Result<bool>;
    [[nodiscard]] auto write_page_to_file(PageId, BytesView) const -> Status;

    [[nodiscard]]
    auto frame_at_impl(FrameId id) -> Frame&
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
