#ifndef CALICO_PAGER_FRAMER_H
#define CALICO_PAGER_FRAMER_H

#include "calico/status.h"
#include "utils/expected.hpp"
#include "utils/types.h"
#include <list>
#include <memory>
#include <optional>

namespace Calico {

struct FileHeader;
class Page;
class Pager;
class Editor;
class Storage;

class Frame final {
public:
    Frame(Byte *buffer, Size id, Size size);

    [[nodiscard]]
    auto pid() const -> Id
    {
        return m_page_id;
    }

    [[nodiscard]]
    auto ref_count() const -> Size
    {
        return m_ref_count;
    }

    [[nodiscard]]
    auto data() const -> Slice
    {
        return m_bytes;
    }

    [[nodiscard]]
    auto data() -> Span
    {
        return m_bytes;
    }

    auto reset(Id id) -> void
    {
        CALICO_EXPECT_EQ(m_ref_count, 0);
        m_page_id = id;
    }

    [[nodiscard]] auto lsn() const -> Id;
    [[nodiscard]] auto ref(bool is_writable) -> Page;
    auto upgrade(Page &page) -> void;
    auto unref(Page &page) -> void;

private:
    Span m_bytes;
    Id m_page_id;
    Size m_ref_count {};
    bool m_is_writable {};
};

class FrameManager final {
public:
    friend class DatabaseImpl;
    friend class Pager;

    ~FrameManager() = default;
    [[nodiscard]] static auto open(const std::string &prefix, Storage *storage, Size page_size, Size frame_count) -> tl::expected<FrameManager, Status>;
    [[nodiscard]] auto pin(Id pid) -> tl::expected<Size, Status>;
    [[nodiscard]] auto write_back(Size index) -> Status;
    [[nodiscard]] auto sync() -> Status;
    [[nodiscard]] auto ref(Size index) -> Page;
    auto unpin(Size) -> void;
    auto upgrade(Size index, Page &page) -> void;
    auto unref(Size index, Page page) -> void;
    auto load_state(const FileHeader &header) -> void;
    auto save_state(FileHeader &header) const -> void;

    [[nodiscard]]
    auto page_count() const -> Size
    {
        return m_page_count;
    }

    [[nodiscard]]
    auto page_size() const -> Size
    {
        return m_page_size;
    }
    
    [[nodiscard]]
    auto available() const -> Size
    {
        return m_available.size();
    }

    [[nodiscard]]
    auto ref_sum() const -> Size
    {
        return m_ref_sum;
    }

    [[nodiscard]]
    auto get_frame(Size index) const -> const Frame &
    {
        CALICO_EXPECT_LT(index, m_frames.size());
        return m_frames[index];
    }

    [[nodiscard]]
    auto bytes_written() const -> Size
    {
        return m_bytes_written;
    }

    auto operator=(FrameManager &&) -> FrameManager & = default;
    FrameManager(FrameManager &&) = default;

private:
    FrameManager(std::unique_ptr<Editor> file, AlignedBuffer buffer, Size page_size, Size frame_count);
    [[nodiscard]] auto read_page_from_file(Id, Span) const -> tl::expected<bool, Status>;
    [[nodiscard]] auto write_page_to_file(Id pid, const Slice &page) const -> Status;

    AlignedBuffer m_buffer;
    std::vector<Frame> m_frames;
    std::list<Size> m_available;
    std::unique_ptr<Editor> m_file;
    Size m_page_count {};
    Size m_page_size {};
    Size m_ref_sum {};
    Size m_bytes_written {};
};

} // namespace Calico

#endif // CALICO_PAGER_FRAMER_H
