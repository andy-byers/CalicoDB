#include "framer.h"
#include "calico/store.h"
#include "core/header.h"
#include "page/page.h"
#include "utils/encoding.h"
#include "utils/expect.h"
#include "utils/layout.h"
#include "utils/logging.h"

namespace calico {

Frame::Frame(Byte *buffer, Size id, Size size)
    : m_bytes {buffer + id*size, size}
{
    CALICO_EXPECT_TRUE(is_power_of_two(size));
    CALICO_EXPECT_GE(size, MINIMUM_PAGE_SIZE);
    CALICO_EXPECT_LE(size, MAXIMUM_PAGE_SIZE);
}

auto Frame::lsn() const -> SequenceId
{
    const auto offset = PageLayout::header_offset(m_page_id) + PageLayout::LSN_OFFSET;
    return SequenceId {get_u64(m_bytes.range(offset))};
}

auto Frame::ref(Pager &source, bool is_writable, bool is_dirty) -> Page
{
    CALICO_EXPECT_FALSE(m_is_writable);

    if (is_writable) {
        CALICO_EXPECT_EQ(m_ref_count, 0);
        m_is_writable = true;
    }
    m_ref_count++;
    return Page {{m_page_id, data(), &source, is_writable, is_dirty}};
}

auto Frame::unref(Page &page) -> void
{
    CALICO_EXPECT_EQ(m_page_id, page.id());
    CALICO_EXPECT_GT(m_ref_count, 0);

    if (page.is_writable()) {
        CALICO_EXPECT_EQ(m_ref_count, 1);
        m_is_writable = false;
    }
    // Make sure the page doesn't get released twice.
    page.m_source.reset();
    m_ref_count--;
}

auto Framer::open(std::unique_ptr<RandomEditor> file, Size page_size, Size frame_count) -> Result<std::unique_ptr<Framer>>
{
    CALICO_EXPECT_TRUE(is_power_of_two(page_size));
    CALICO_EXPECT_GE(page_size, MINIMUM_PAGE_SIZE);
    CALICO_EXPECT_LE(page_size, MAXIMUM_PAGE_SIZE);
    CALICO_EXPECT_GE(frame_count, MINIMUM_FRAME_COUNT);
    CALICO_EXPECT_LE(frame_count, MAXIMUM_FRAME_COUNT);

    const auto cache_size = page_size * frame_count;
    AlignedBuffer buffer {
        new(static_cast<std::align_val_t>(page_size), std::nothrow) Byte[cache_size],
        AlignedDeleter {static_cast<std::align_val_t>(page_size)}};

    if (!buffer) {
        ThreePartMessage message;
        message.set_primary("cannot open pager");
        message.set_detail("out of memory");
        message.set_hint("tried to allocate {} bytes of cache memory", cache_size);
        return Err {message.system_error()};
    }
    return std::unique_ptr<Framer> {new Framer {std::move(file), std::move(buffer), page_size, frame_count}};
}

Framer::Framer(std::unique_ptr<RandomEditor> file, AlignedBuffer buffer, Size page_size, Size frame_count)
    : m_buffer {std::move(buffer)},
      m_file {std::move(file)},
      m_frame_count {frame_count},
      m_page_size {page_size}
{
    // The buffer should be aligned to the page size.
    CALICO_EXPECT_EQ(reinterpret_cast<std::uintptr_t>(m_buffer.get()) % page_size, 0);
    mem_clear({m_buffer.get(), page_size * frame_count});

    while (m_frames.size() < frame_count)
        m_frames.emplace_back(m_buffer.get(), m_frames.size(), page_size);
    
    while (m_available.size() < m_frames.size())
        m_available.emplace_back(FrameNumber {m_available.size()});
}

auto Framer::ref(FrameNumber id, Pager &source, bool is_writable, bool is_dirty) -> Page
{
    CALICO_EXPECT_LT(id, m_frame_count);
    return m_frames[id].ref(source, is_writable, is_dirty);
}
auto Framer::unref(FrameNumber id, Page &page) -> void
{
    CALICO_EXPECT_LT(id, m_frame_count);
    m_frames[id].unref(page);
}

auto Framer::pin(PageId id) -> Result<FrameNumber>
{
    CALICO_EXPECT_FALSE(id.is_null());
    if (m_available.empty()) {
        ThreePartMessage message;
        message.set_primary("could not pin page");
        message.set_detail("unable to find an available frame");
        message.set_hint("unpin a page and try again");
        return Err {message.not_found()};
    }

    auto &frame = frame_at_impl(m_available.back());

    if (auto r = read_page_from_file(id, frame.data())) {
        if (!*r) {
            // We just tried to read at EOF. This happens when we allocate a new page or roll the WAL forward.
            CALICO_EXPECT_EQ(id.as_index(), m_page_count);
            mem_clear(frame.data());
            m_page_count++;
        }
    } else {
        return Err {r.error()};
    }

    auto fid = m_available.back();
    m_available.pop_back();
    frame.reset(id);
    return fid;
}

auto Framer::discard(FrameNumber id) -> void
{
    CALICO_EXPECT_EQ(frame_at_impl(id).ref_count(), 0);
    frame_at_impl(id).reset(PageId::null());
    m_available.emplace_back(id);
}

auto Framer::unpin(FrameNumber id, bool is_dirty) -> Status
{
    auto &frame = frame_at_impl(id);
    CALICO_EXPECT_LT(frame.pid().as_index(), m_page_count);
    CALICO_EXPECT_EQ(frame.ref_count(), 0);
    auto s = Status::ok();

    // If this fails, the caller (buffer pool) will need to roll back the database state or exit.
    if (is_dirty) {
        s = write_page_to_file(frame.pid(), frame.data());

        if (s.is_ok()) {
            const auto offset = PageLayout::header_offset(frame.pid()) + PageLayout::LSN_OFFSET;
            m_flushed_lsn = std::max(m_flushed_lsn, SequenceId {get_u64(frame.data().range(offset))});
        }
    }

    frame.reset(PageId::null());
    m_available.emplace_back(id);
    return s;
}

auto Framer::sync() -> Status
{
    return m_file->sync();
}

auto Framer::read_page_from_file(PageId id, Bytes out) const -> Result<bool>
{
    CALICO_EXPECT_EQ(m_page_size, out.size());
    const auto file_size = m_page_count * m_page_size;
    const auto offset = FileLayout::page_offset(id, out.size());

    // Don't even try to call read() if the file isn't large enough. The system call version can be pretty slow even if it doesn't read anything.
    if (offset >= file_size)
        return false;

    const auto s = m_file->read(out, offset);

    // System call error.
    if (!s.is_ok())
        return Err {s};

    if (out.size() != m_page_size) {
        ThreePartMessage message;
        message.set_primary("could not read page {}", id.value);
        message.set_detail("incomplete read");
        message.set_hint("read {}/{} bytes", out.size(), m_page_size);
        return Err {message.system_error()};
    }
    return true;
}

auto Framer::write_page_to_file(PageId id, BytesView in) const -> Status
{
    CALICO_EXPECT_EQ(m_page_size, in.size());
    return m_file->write(in, FileLayout::page_offset(id, in.size()));
}

auto Framer::load_state(const FileHeader &header) -> void
{
    m_flushed_lsn.value = header.flushed_lsn;
    m_page_count = header.page_count;
    m_page_size = decode_page_size(header.page_size);
}

auto Framer::save_state(FileHeader &header) -> void
{
    header.flushed_lsn = m_flushed_lsn.value;
    header.page_count = m_page_count;
    header.page_size = encode_page_size(m_page_size);
}

} // namespace cco
