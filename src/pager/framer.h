#ifndef CALICO_PAGER_FRAMER_H
#define CALICO_PAGER_FRAMER_H

#include "calico/status.h"
#include <tl/expected.hpp>
#include "utils/types.h"
#include <list>
#include <memory>
#include <optional>
#include "spdlog/spdlog.h"

namespace calico {

struct FileHeader;
class Page;
class Pager;
class RandomEditor;
class Storage;
class WriteAheadLog;

class Frame final {
public:
    Frame(Byte *, Size, Size);

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
    auto data() const -> BytesView
    {
        return m_bytes;
    }

    [[nodiscard]]
    auto data() -> Bytes
    {
        return m_bytes;
    }

    auto reset(Id id) -> void
    {
        CALICO_EXPECT_EQ(m_ref_count, 0);
        m_page_id = id;
    }

    [[nodiscard]] auto lsn() const -> Id;
    [[nodiscard]] auto ref(Pager&, bool) -> Page;
    auto unref(Page &page) -> void;

private:
    Bytes m_bytes;
    Id m_page_id;
    Size m_ref_count {};
    bool m_is_writable {};
};

class Framer final {
public:
    ~Framer() = default;
    [[nodiscard]] static auto open(const std::string &prefix, Storage *storage, Size page_size, Size frame_count) -> tl::expected<Framer, Status>;
    [[nodiscard]] auto pin(Id) -> tl::expected<Size, Status>;
    [[nodiscard]] auto write_back(Size) -> Status;
    [[nodiscard]] auto sync() -> Status;
    [[nodiscard]] auto ref(Size, Pager&, bool) -> Page;
    auto unpin(Size) -> void;
    auto unref(Size, Page&) -> void;
    auto discard(Size) -> void;
    auto load_state(const FileHeader&) -> void;
    auto save_state(FileHeader&) const -> void;

    [[nodiscard]]
    auto flushed_lsn() const -> Id
    {
        return m_flushed_lsn;
    }

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
    auto frame_at(Size id) const -> const Frame&
    {
        CALICO_EXPECT_LT(id, m_frames.size());
        return m_frames[id];
    }

    auto operator=(Framer &&) -> Framer & = default;
    Framer(Framer &&) = default;

private:
    Framer(std::unique_ptr<RandomEditor>, AlignedBuffer, Size, Size);
    [[nodiscard]] auto read_page_from_file(Id, Bytes) const -> tl::expected<bool, Status>;
    [[nodiscard]] auto write_page_to_file(Id, BytesView) const -> Status;

    [[nodiscard]]
    auto frame_at_impl(Size id) -> Frame&
    {
        CALICO_EXPECT_LT(id, m_frames.size());
        return m_frames[id];
    }

    AlignedBuffer m_buffer;
    std::vector<Frame> m_frames;
    std::list<Size> m_available;
    std::unique_ptr<RandomEditor> m_file;
    Id m_flushed_lsn;
    Size m_page_count {};
    Size m_page_size {};
    Size m_ref_sum {};
};

} // namespace calico

#endif // CALICO_PAGER_FRAMER_H
