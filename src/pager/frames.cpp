#include "frames.h"
#include "calico/storage.h"
#include "page.h"
#include "tree/header.h"
#include "utils/encoding.h"

namespace Calico {

Frame::Frame(Byte *buffer, Size id, Size size)
    : m_bytes {buffer + id * size, size}
{
    CALICO_EXPECT_TRUE(is_power_of_two(size));
    CALICO_EXPECT_GE(size, MINIMUM_PAGE_SIZE);
    CALICO_EXPECT_LE(size, MAXIMUM_PAGE_SIZE);
}

auto Frame::lsn() const -> Id
{
    return {get_u64(m_bytes.range(m_page_id.is_root() * FileHeader::SIZE))};
}

auto Frame::ref(Page &page) -> void
{
    CALICO_EXPECT_FALSE(m_is_writable);
    page.m_id = m_page_id;
    page.m_span = m_bytes;
    page.m_write = false;
    ++m_ref_count;
}

auto Frame::upgrade(Page &page) -> void
{
    CALICO_EXPECT_FALSE(page.is_writable());
    CALICO_EXPECT_FALSE(m_is_writable);
    m_is_writable = true;
    page.m_write = true;
}

auto Frame::unref(Page &page) -> void
{
    CALICO_EXPECT_EQ(m_page_id, page.id());
    CALICO_EXPECT_GT(m_ref_count, 0);

    if (page.is_writable()) {
        CALICO_EXPECT_TRUE(m_is_writable);
        CALICO_EXPECT_EQ(m_ref_count, 1);
        m_is_writable = false;
        page.m_write = false;
    }
    --m_ref_count;
}

FrameManager::FrameManager(Editor *file, AlignedBuffer buffer, Size page_size, Size frame_count)
    : m_buffer {std::move(buffer)},
      m_file {file},
      m_page_size {page_size}
{
    // The buffer should be aligned to the page size.
    CALICO_EXPECT_EQ(reinterpret_cast<std::uintptr_t>(m_buffer.get()) % page_size, 0);

    while (m_frames.size() < frame_count) {
        m_frames.emplace_back(m_buffer.get(), m_frames.size(), page_size);
    }

    while (m_available.size() < m_frames.size()) {
        m_available.emplace_back(m_available.size());
    }
}

auto FrameManager::ref(Size index, Page &out) -> void
{
    ++m_ref_sum;
    CALICO_EXPECT_LT(index, m_frames.size());
    m_frames[index].ref(out);
}

auto FrameManager::unref(Size index, Page page) -> void
{
    CALICO_EXPECT_LT(index, m_frames.size());
    m_frames[index].unref(page);
    --m_ref_sum;
}

auto FrameManager::upgrade(Size index, Page &page) -> void
{
    CALICO_EXPECT_FALSE(page.is_writable());
    CALICO_EXPECT_LT(index, m_frames.size());
    m_frames[index].upgrade(page);
}

auto FrameManager::pin(Id pid, Size &fid) -> Status
{
    CALICO_EXPECT_FALSE(pid.is_null());

    if (m_available.empty()) {
        return Status::not_found("out of frames");
    }

    fid = m_available.back();
    CALICO_EXPECT_LT(fid, m_frames.size());
    auto &frame = m_frames[fid];
    CALICO_EXPECT_EQ(frame.ref_count(), 0);

    auto s = read_page_from_file(pid, frame.data());
    if (s.is_not_found()) {
        // We just tried to read at or past EOF. This happens when we allocate a new page or roll the WAL forward.
        mem_clear(frame.data());
        ++m_page_count;
    } else if (!s.is_ok()) {
        return s;
    }
    m_available.pop_back();
    frame.reset(pid);
    return Status::ok();
}

auto FrameManager::unpin(Size id) -> void
{
    CALICO_EXPECT_LT(id, m_frames.size());
    auto &frame = m_frames[id];
    CALICO_EXPECT_EQ(frame.ref_count(), 0);
    frame.reset(Id::null());
    m_available.emplace_back(id);
}

auto FrameManager::write_back(Size id) -> Status
{
    auto &frame = get_frame(id);
    CALICO_EXPECT_LE(frame.ref_count(), 1);

    m_bytes_written += m_page_size;
    return write_page_to_file(frame.pid(), frame.data());
}

auto FrameManager::sync() -> Status
{
    return m_file->sync();
}

auto FrameManager::read_page_from_file(Id id, Span out) const -> Status
{
    CALICO_EXPECT_EQ(m_page_size, out.size());
    const auto offset = id.as_index() * out.size();

    // Don't even try to call read() if the file isn't large enough. The system call can be pretty slow even if it doesn't read anything.
    // This happens when we are allocating a page from the end of the file.
    if (offset >= m_page_count * m_page_size) {
        return Status::not_found("end of file");
    }

    auto read_size = out.size();
    Calico_Try(m_file->read(out.data(), read_size, offset));

    // We should always read exactly what we requested, unless we are allocating a page during recovery.
    if (read_size == m_page_size) {
        return Status::ok();
    }

    // In that case, we will hit EOF here.
    if (read_size == 0) {
        return Status::not_found("end of file");
    }

    return Status::system_error("incomplete read");
}

auto FrameManager::write_page_to_file(Id pid, const Slice &page) const -> Status
{
    CALICO_EXPECT_EQ(m_page_size, page.size());
    return m_file->write(page, pid.as_index() * page.size());
}

auto FrameManager::load_state(const FileHeader &header) -> void
{
    // Page size should already be correct.
    m_page_count = header.page_count;
}

auto FrameManager::save_state(FileHeader &header) const -> void
{
    header.page_count = m_page_count;
    header.page_size = static_cast<PageSize>(m_page_size);
}

} // namespace Calico
