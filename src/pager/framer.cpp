#include "framer.h"
#include "pager.h"
#include "calico/storage.h"
#include "page/page.h"
#include "utils/encoding.h"
#include "utils/expect.h"
#include "utils/header.h"
#include "utils/layout.h"

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
    const auto offset = PageLayout::header_offset(m_page_id) + PageLayout::LSN_OFFSET;
    return Id {get_u64(m_bytes.range(offset))};
}

auto Frame::ref(Pager &source, bool is_writable) -> Page_
{
    CALICO_EXPECT_FALSE(m_is_writable);

    if (is_writable) {
        CALICO_EXPECT_EQ(m_ref_count, 0);
        m_is_writable = true;
    }
    m_ref_count++;
    return Page_ {{m_page_id, data(), &source, is_writable}};
}

auto Frame::unref(Page_ &page) -> void
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

auto Frame::ref_(bool is_writable) -> Page
{
    CALICO_EXPECT_FALSE(m_is_writable);

    if (is_writable) {
        CALICO_EXPECT_EQ(m_ref_count, 0);
        m_is_writable = true;
    }
    m_ref_count++;
    return Page {m_page_id, data(), is_writable};
}

auto Frame::unref_(Page &page) -> void
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

auto Framer::open(const std::string &prefix, Storage *storage, Size page_size, Size frame_count) -> tl::expected<Framer, Status>
{
    CALICO_EXPECT_TRUE(is_power_of_two(page_size));
    CALICO_EXPECT_GE(page_size, MINIMUM_PAGE_SIZE);
    CALICO_EXPECT_LE(page_size, MAXIMUM_PAGE_SIZE);

    RandomEditor *temp_file {};
    auto s = storage->open_random_editor(prefix + DATA_FILENAME, &temp_file);
    std::unique_ptr<RandomEditor> file {temp_file};

    // Allocate the frames, i.e. where pages from disk are stored in memory. Aligned to the page size, so it could
    // potentially be used for direct I/O.
    const auto cache_size = page_size * frame_count;
    AlignedBuffer buffer {cache_size, page_size};
    if (buffer.get() == nullptr)
        return tl::make_unexpected(system_error("cannot make_room frames: out of memory"));

    return Framer {std::move(file), std::move(buffer), page_size, frame_count};
}

Framer::Framer(std::unique_ptr<RandomEditor> file, AlignedBuffer buffer, Size page_size, Size frame_count)
    : m_buffer {std::move(buffer)},
      m_file {std::move(file)},
      m_page_size {page_size}
{
    // The buffer should be aligned to the page size.
    CALICO_EXPECT_EQ(reinterpret_cast<std::uintptr_t>(m_buffer.get()) % page_size, 0);
    mem_clear({m_buffer.get(), page_size * frame_count});

    while (m_frames.size() < frame_count)
        m_frames.emplace_back(m_buffer.get(), m_frames.size(), page_size);
    
    while (m_available.size() < m_frames.size())
        m_available.emplace_back(Size {m_available.size()});
}

auto Framer::ref(Size id, Pager &source, bool is_writable) -> Page_
{
    CALICO_EXPECT_LT(id, m_frames.size());
    m_ref_sum++;
    return m_frames[id].ref(source, is_writable);
}

auto Framer::ref_(Size index) -> Page
{
    CALICO_EXPECT_LT(index, m_frames.size());
    m_ref_sum++;
    return m_frames[index].ref_(false);
}

auto Framer::unref_(Size index, Page page) -> void
{
    CALICO_EXPECT_LT(index, m_frames.size());
    m_frames[index].unref_(page);
    m_ref_sum--;
}

auto Framer::upgrade_(Size index, Page &page) -> void
{
    CALICO_EXPECT_LT(index, m_frames.size());
    m_frames[index].unref_(page);
    page = m_frames[index].ref_(true);
}

auto Framer::unref(Size id, Page_ &page) -> void
{
    CALICO_EXPECT_LT(id, m_frames.size());
    m_frames[id].unref(page);
    m_ref_sum--;
}

auto Framer::pin(Id pid) -> tl::expected<Size, Status>
{
    CALICO_EXPECT_FALSE(pid.is_null());
    if (m_available.empty())
        return tl::make_unexpected(not_found(
            "could not pin page: unable to find an available frame (unpin a page and try again)"));

    auto fid = m_available.back();
    auto &frame = frame_at_impl(fid);
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

auto Framer::discard(Size id) -> void
{
    CALICO_EXPECT_EQ(frame_at_impl(id).ref_count(), 0);
    frame_at_impl(id).reset(Id::null());
    m_available.emplace_back(id);
}

auto Framer::unpin(Size id) -> void
{
    auto &frame = frame_at_impl(id);
    CALICO_EXPECT_EQ(frame.ref_count(), 0);
    frame.reset(Id::null());
    m_available.emplace_back(id);
}

auto Framer::write_back(Size id) -> Status
{
    auto &frame = frame_at_impl(id);
    CALICO_EXPECT_LE(frame.ref_count(), 1);

    // If this fails, the caller will need to roll back the database state or exit.
    m_bytes_written += m_page_size;
    return write_page_to_file(frame.pid(), frame.data());
}

auto Framer::sync() -> Status
{
    return m_file->sync();
}

auto Framer::read_page_from_file(Id id, Span out) const -> tl::expected<bool, Status>
{
    CALICO_EXPECT_EQ(m_page_size, out.size());
    const auto file_size = m_page_count * m_page_size;
    const auto offset = id.as_index() * out.size();

    // Don't even try to call read() if the file isn't large enough. The system call can be pretty slow even if it doesn't read anything.
    // This happens when we are allocating a page from the end of the file.
    if (offset >= file_size)
        return false;

    auto read_size = out.size();
    auto s = m_file->read(out.data(), read_size, offset);
    if (!s.is_ok()) return tl::make_unexpected(s);

    // We should always read exactly what we requested, unless we are allocating a page during recovery.
    if (read_size == m_page_size)
        return true;

    // In that case, we will hit EOF here.
    if (read_size == 0)
        return false;

    return tl::make_unexpected(system_error(
        "could not read page {}: incomplete read (read {}/{} B)", id.value, out.size(), m_page_size));
}

auto Framer::write_page_to_file(Id pid, const Slice &page) const -> Status
{
    CALICO_EXPECT_EQ(m_page_size, page.size());
    return m_file->write(page, pid.as_index() * page.size());
}

auto Framer::load_state(const FileHeader__ &header) -> void
{
    m_page_count = header.page_count;
}

auto Framer::save_state(FileHeader__ &header) const -> void
{
    header.page_count = m_page_count;
}

} // namespace Calico
