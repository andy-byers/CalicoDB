#include "pager.h"
#include "frame.h"
#include "calico/storage.h"
#include "db/header.h"
#include "utils/encoding.h"
#include "utils/expect.h"
#include "utils/layout.h"
#include "utils/logging.h"

namespace cco {

auto Pager::open(std::unique_ptr<RandomAccessEditor> file, Size page_size, Size frame_count) -> Result<std::unique_ptr<Pager>>
{
    CCO_EXPECT_TRUE(is_power_of_two(page_size));
    CCO_EXPECT_GE(page_size, MINIMUM_PAGE_SIZE);
    CCO_EXPECT_LE(page_size, MAXIMUM_PAGE_SIZE);
    CCO_EXPECT_GE(frame_count, MINIMUM_FRAME_COUNT);
    CCO_EXPECT_LE(frame_count, MAXIMUM_FRAME_COUNT);

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
    return std::unique_ptr<Pager> {new Pager {std::move(file), std::move(buffer), page_size, frame_count}};
}

Pager::Pager(std::unique_ptr<RandomAccessEditor> file, AlignedBuffer buffer, Size page_size, Size frame_count)
    : m_buffer {std::move(buffer)},
      m_file {std::move(file)},
      m_frame_count {frame_count},
      m_page_size {page_size}
{
    // The buffer should be aligned to the page size.
    CCO_EXPECT_EQ(reinterpret_cast<std::uintptr_t>(m_buffer.get()) % page_size, 0);
    mem_clear({m_buffer.get(), page_size * frame_count});

    while (m_frames.size() < frame_count)
        m_frames.emplace_back(m_buffer.get(), m_available.size(), page_size);
    
    for (const auto &frame: m_frames)
        m_available.emplace_back(m_available.size());
}

auto Pager::ref(FrameId id, IBufferPool *src, bool is_writable) -> Page
{
    CCO_EXPECT_LT(id.as_index(), m_frame_count);
    return m_frames[id.as_index()].ref(src, is_writable);
}
auto Pager::unref(FrameId id, Page &page) -> void
{
    CCO_EXPECT_LT(id.as_index(), m_frame_count);
    m_frames[id.as_index()].unref(page);
}

auto Pager::pin(PageId id) -> Result<FrameId>
{
    CCO_EXPECT_FALSE(id.is_null());
    if (m_available.empty()) {
        ThreePartMessage message;
        message.set_primary("could not pin page");
        message.set_detail("unable to find an available frame");
        message.set_hint("unpin a page and try again");
        return Err {message.not_found()};
    }

    auto &frame = frame_at(m_available.back());

    if (auto r = read_page_from_file(id, frame.data())) {
        if (!*r) {
            // We just tried to read at EOF. This happens when we allocate a new page or roll the WAL forward.
            CCO_EXPECT_EQ(id.value, m_page_count);
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

auto Pager::discard(FrameId id) -> void
{
    CCO_EXPECT_EQ(frame_at(id).ref_count(), 0);
    frame_at(id).reset(PageId::null());
    m_available.emplace_back(id);
}

auto Pager::unpin(FrameId id) -> Status
{
    auto &frame = frame_at(id);
    CCO_EXPECT_LT(frame.pid().value, m_page_count);
    CCO_EXPECT_EQ(frame.ref_count(), 0);
    auto s = Status::ok();

    // If this fails, the caller (buffer pool) will need to roll back the database state or exit.
    if (frame.is_dirty()) {
        s = write_page_to_file(frame.pid(), frame.data());

        if (s.is_ok()) {
            const auto offset = PageLayout::header_offset(frame.pid()) + PageLayout::LSN_OFFSET;
            m_flushed_lsn = std::max(m_flushed_lsn, SequenceNumber {get_u64(frame.data().range(offset))});
        }
    }

    frame.reset(PageId::null());
    m_available.emplace_back(id);
    return s;
}

auto Pager::sync() -> Status
{
    return m_file->sync();
}

auto Pager::read_page_from_file(PageId id, Bytes out) const -> Result<bool>
{
    static constexpr auto ERROR_PRIMARY = "could not read page";
    static constexpr auto ERROR_DETAIL = "page ID is {}";
    CCO_EXPECT_EQ(m_page_size, out.size());

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

auto Pager::write_page_to_file(PageId id, BytesView in) const -> Status
{
    CCO_EXPECT_EQ(page_size(), in.size());
    return m_file->write(in, FileLayout::page_offset(id, in.size()));
}

auto Pager::load_state(const FileHeader &header) -> void
{
    m_flushed_lsn.value = header.flushed_lsn;
    m_page_count = header.page_count;
    m_page_size = header.page_size;
}

auto Pager::save_state(FileHeader &header) -> void
{
    header.flushed_lsn = m_flushed_lsn.value;
    header.page_count = m_page_count;
    header.page_size = static_cast<std::uint16_t>(m_page_size);
}

} // namespace cco
