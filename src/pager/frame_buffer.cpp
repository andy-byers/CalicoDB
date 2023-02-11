#include "frame_buffer.h"
#include "calico/storage.h"
#include "page.h"
#include "tree/header.h"
#include "utils/encoding.h"

namespace Calico {

Frame::Frame(Byte *buffer, Size id, Size size)
    : m_bytes {buffer + id*size, size}
{
    CALICO_EXPECT_TRUE(is_power_of_two(size));
    CALICO_EXPECT_GE(size, MINIMUM_PAGE_SIZE);
    CALICO_EXPECT_LE(size, MAXIMUM_PAGE_SIZE);
}

auto Frame::lsn() const -> Id
{
    return {get_u64(m_bytes.range(m_page_id.is_root() * FileHeader::SIZE))};
}

auto Frame::ref(bool is_writable) -> Page
{
    CALICO_EXPECT_FALSE(m_is_writable);

    if (is_writable) {
        CALICO_EXPECT_EQ(m_ref_count, 0);
        m_is_writable = true;
    }
    m_ref_count++;
    return Page {m_page_id, data(), is_writable};
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
    m_ref_count--;
}

auto FrameBuffer::open(const std::string &prefix, Storage *storage, Size page_size, Size frame_count) -> tl::expected<FrameBuffer, Status>
{
    CALICO_EXPECT_TRUE(is_power_of_two(page_size));
    CALICO_EXPECT_GE(page_size, MINIMUM_PAGE_SIZE);
    CALICO_EXPECT_LE(page_size, MAXIMUM_PAGE_SIZE);

    Editor *temp_file {};
    auto s = storage->new_editor(prefix + "data", &temp_file);
    std::unique_ptr<Editor> file {temp_file};

    // Allocate the frames, i.e. where pages from disk are stored in memory. Aligned to the page size, so it could
    // potentially be used for direct I/O.
    const auto cache_size = page_size * frame_count;
    AlignedBuffer buffer {cache_size, page_size};
    if (buffer.get() == nullptr) {
        return tl::make_unexpected(Status::system_error("out of memory"));
    }

    return FrameBuffer {std::move(file), std::move(buffer), page_size, frame_count};
}

FrameBuffer::FrameBuffer(std::unique_ptr<Editor> file, AlignedBuffer buffer, Size page_size, Size frame_count)
    : m_buffer {std::move(buffer)},
      m_file {std::move(file)},
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

auto FrameBuffer::ref(Size index) -> Page
{
    m_ref_sum++;
    CALICO_EXPECT_LT(index, m_frames.size());
    return m_frames[index].ref(false);
}

auto FrameBuffer::unref(Size index, Page page) -> void
{
    CALICO_EXPECT_LT(index, m_frames.size());
    m_frames[index].unref(page);
    m_ref_sum--;
}

auto FrameBuffer::upgrade(Size index, Page &page) -> void
{
    CALICO_EXPECT_FALSE(page.is_writable());
    CALICO_EXPECT_LT(index, m_frames.size());
    m_frames[index].upgrade(page);
}

auto FrameBuffer::pin(Id pid) -> tl::expected<Size, Status>
{
    CALICO_EXPECT_FALSE(pid.is_null());
    CALICO_EXPECT_LE(pid.as_index(), m_page_count);

    if (m_available.empty()) {
        return tl::make_unexpected(Status::not_found("out of frames"));
    }

    auto fid = m_available.back();
    CALICO_EXPECT_LT(fid, m_frames.size());
    auto &frame = m_frames[fid];
    CALICO_EXPECT_EQ(frame.ref_count(), 0);

    if (auto r = read_page_from_file(pid, frame.data())) {
        if (!*r) {
            // We just tried to read at or past EOF. This happens when we allocate a new page or roll the WAL forward.
            mem_clear(frame.data());
            m_page_count++;
        }
    } else {
        return tl::make_unexpected(r.error());
    }
    m_available.pop_back();
    frame.reset(pid);
    return fid;
}

auto FrameBuffer::unpin(Size id) -> void
{
    CALICO_EXPECT_LT(id, m_frames.size());
    auto &frame = m_frames[id];
    CALICO_EXPECT_EQ(frame.ref_count(), 0);
    frame.reset(Id::null());
    m_available.emplace_back(id);
}

auto FrameBuffer::write_back(Size id) -> Status
{
    auto &frame = get_frame(id);
    CALICO_EXPECT_LE(frame.ref_count(), 1);

    m_bytes_written += m_page_size;
    return write_page_to_file(frame.pid(), frame.data());
}

auto FrameBuffer::sync() -> Status
{
    return m_file->sync();
}

auto FrameBuffer::read_page_from_file(Id id, Span out) const -> tl::expected<bool, Status>
{
    CALICO_EXPECT_EQ(m_page_size, out.size());
    const auto file_size = m_page_count * m_page_size;
    const auto offset = id.as_index() * out.size();

    // Don't even try to call read() if the file isn't large enough. The system call can be pretty slow even if it doesn't read anything.
    // This happens when we are allocating a page from the end of the file.
    if (offset >= file_size) {
        return false;
    }

    auto read_size = out.size();
    auto s = m_file->read(out.data(), read_size, offset);
    if (!s.is_ok()) {
        return tl::make_unexpected(s);
    }

    // We should always read exactly what we requested, unless we are allocating a page during recovery.
    if (read_size == m_page_size) {
        return true;
    }

    // In that case, we will hit EOF here.
    if (read_size == 0) {
        return false;
    }

    return tl::make_unexpected(Status::system_error("incomplete read"));
}

auto FrameBuffer::write_page_to_file(Id pid, const Slice &page) const -> Status
{
    CALICO_EXPECT_EQ(m_page_size, page.size());
    return m_file->write(page, pid.as_index() * page.size());
}

auto FrameBuffer::load_state(const FileHeader &header) -> void
{
    m_page_count = header.page_count;
}

auto FrameBuffer::save_state(FileHeader &header) const -> void
{
    header.page_count = m_page_count;
}

} // namespace Calico
