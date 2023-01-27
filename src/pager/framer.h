#ifndef CALICO_PAGER_FRAMER_H
#define CALICO_PAGER_FRAMER_H

#include "calico/status.h"
#include "spdlog/spdlog.h"
#include "temp/page.h"
#include "utils/expected.hpp"
#include "utils/types.h"
#include <list>
#include <memory>
#include <optional>

namespace Calico {

struct FileHeader__;
class Page_;
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
    [[nodiscard]] auto ref(Pager&, bool) -> Page_;
    [[nodiscard]] auto ref_(bool) -> Page;
    auto unref(Page_ &page) -> void;
    auto unref_(Page &page) -> void;

private:
    Span m_bytes;
    Id m_page_id;
    Size m_ref_count {};
    bool m_is_writable {};
};

class Framer final {
public:
    ~Framer() = default;
    [[nodiscard]] static auto open(const std::string &prefix, Storage *storage, Size page_size, Size frame_count) -> tl::expected<Framer, Status>;
    [[nodiscard]] auto pin(Id pid) -> tl::expected<Size, Status>;
    [[nodiscard]] auto write_back(Size index) -> Status;
    [[nodiscard]] auto sync() -> Status;
    [[nodiscard]] auto ref_(Size index) -> Page;
    auto unpin(Size) -> void;
    auto upgrade_(Size index, Page &page) -> void;
    auto unref_(Size index, Page page) -> void;
    auto discard(Size) -> void;
    auto load_state(const FileHeader__ &) -> void;
    auto save_state(FileHeader__ &) const -> void;
    [[nodiscard]]
    auto page_count() const -> Size
    {
        return m_page_count;
    }

    [[nodiscard]] auto ref(Size, Pager&, bool) -> Page_;
    auto unref(Size, Page_ &) -> void;

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

    [[nodiscard]]
    auto bytes_written() const -> Size
    {
        return m_bytes_written;
    }

    auto operator=(Framer &&) -> Framer & = default;
    Framer(Framer &&) = default;

private:
    Framer(std::unique_ptr<RandomEditor>, AlignedBuffer, Size, Size);
    [[nodiscard]] auto read_page_from_file(Id, Span) const -> tl::expected<bool, Status>;
    [[nodiscard]] auto write_page_to_file(Id pid, const Slice &page) const -> Status;

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
    Size m_page_count {};
    Size m_page_size {};
    Size m_ref_sum {};
    Size m_bytes_written {};
};

} // namespace Calico

#endif // CALICO_PAGER_FRAMER_H
