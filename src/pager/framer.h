#ifndef CALICO_POOL_PAGER_H
#define CALICO_POOL_PAGER_H

#include "calico/status.h"
#include "utils/result.h"
#include "utils/types.h"
#include <list>
#include <memory>
#include <optional>
#include <spdlog/spdlog.h>

namespace calico {

struct FileHeader;
class Page;
class Pager;
class RandomEditor;

struct FrameNumber {
    using Hash = IndexHash<FrameNumber>;

    constexpr FrameNumber() noexcept = default;

    template<class U>
    constexpr explicit FrameNumber(U u) noexcept
        : value {u}
    {}

    constexpr operator std::uint64_t() const
    {
        return value;
    }

    std::uint64_t value {};
};

class Frame final {
public:
    Frame(Byte *, Size, Size);

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
        CALICO_EXPECT_EQ(m_ref_count, 0);
        m_page_id = id;
    }

    [[nodiscard]] auto lsn() const -> SequenceId;
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
    [[nodiscard]] static auto open(std::unique_ptr<RandomEditor>, Size, Size) -> Result<std::unique_ptr<Framer>>;
    [[nodiscard]] auto pin(PageId) -> Result<FrameNumber>;
    [[nodiscard]] auto unpin(FrameNumber, bool) -> Status;
    [[nodiscard]] auto sync() -> Status;
    [[nodiscard]] auto ref(FrameNumber, Pager&, bool, bool) -> Page;
    auto unref(FrameNumber, Page&) -> void;
    auto discard(FrameNumber) -> void;
    auto load_state(const FileHeader&) -> void;
    auto save_state(FileHeader&) -> void;

    [[nodiscard]]
    auto flushed_lsn() -> SequenceId
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
    auto frame_at(FrameNumber id) const -> const Frame&
    {
        CALICO_EXPECT_LT(id, m_frame_count);
        return m_frames[id.value];
    }

    auto operator=(Framer &&) -> Framer & = default;
    Framer(Framer &&) = default;

private:
    Framer(std::unique_ptr<RandomEditor>, AlignedBuffer, Size, Size);
    [[nodiscard]] auto read_page_from_file(PageId, Bytes) const -> Result<bool>;
    [[nodiscard]] auto write_page_to_file(PageId, BytesView) const -> Status;

    [[nodiscard]]
    auto frame_at_impl(FrameNumber id) -> Frame&
    {
        CALICO_EXPECT_LT(id, m_frame_count);
        return m_frames[id.value];
    }

    AlignedBuffer m_buffer;
    std::vector<Frame> m_frames;
    std::list<FrameNumber> m_available;
    std::unique_ptr<RandomEditor> m_file;
    SequenceId m_flushed_lsn;
    Size m_frame_count {};
    Size m_page_count {};
    Size m_page_size {};
};

} // namespace cco

#endif // CALICO_POOL_PAGER_H
