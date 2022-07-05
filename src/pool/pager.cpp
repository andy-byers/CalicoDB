#include "pager.h"

#include "frame.h"
#include "calico/exception.h"
#include "page/page.h"
#include "storage/file.h"
#include "utils/expect.h"
#include "utils/layout.h"

namespace calico {

Pager::Pager(Parameters param)
    : m_reader {std::move(param.reader)},
      m_writer {std::move(param.writer)},
      m_frame_count {param.frame_count},
      m_page_size {param.page_size}
{
    if (m_frame_count < MINIMUM_FRAME_COUNT)
        throw std::invalid_argument {"Frame count is too small"};

    if (m_frame_count > MAXIMUM_FRAME_COUNT)
        throw std::invalid_argument {"Frame count is too large"};

    while (m_available.size() < m_frame_count)
        m_available.emplace_back(m_page_size);
}

auto Pager::available() const -> Size 
{
    return m_available.size();
}

auto Pager::page_size() const -> Size
{
    return m_page_size;
}

auto Pager::truncate(Size page_count) -> void
{
    CALICO_EXPECT_EQ(m_available.size(), m_frame_count);
    m_writer->resize(page_count * page_size());
}

/**
 * Pin a database page to an available frame.
 *
 * Provides the strong guarantee concerning exceptions thrown by the file object during the read. Also makes sure that
 * newly allocated pages are zeroed out.
 *
 * @param id Page ID of the page we want to pin.
 * @return A frame with the requested database page contents pinned.
 */
auto Pager::pin(PID id) -> std::optional<Frame>
{
    CALICO_EXPECT_FALSE(id.is_null());
    if (m_available.empty())
        return std::nullopt;

    auto &frame = m_available.back();

    // This can throw. If it does, we haven't done anything yet, so we should be okay. Also, when we hit EOF we make
    // sure to clear the frame, since we are essentially allocating a new page.
    if (!try_read_page_from_file(id, frame.data()))
        mem_clear(frame.data());

    auto moved = std::move(m_available.back());
    m_available.pop_back();
    moved.reset(id);
    return moved;
}

auto Pager::discard(Frame frame) -> void
{
    frame.clean();
    unpin(std::move(frame));
}

auto Pager::unpin(Frame frame) -> void
{
    CALICO_EXPECT_EQ(frame.ref_count(), 0);
    const auto needs_write = frame.is_dirty();
    const auto id = frame.page_id();
    const auto data = frame.data();

    frame.reset(PID::null());
    if (m_available.size() < m_frame_count)
        m_available.emplace_back(std::move(frame));

    if (needs_write)
        write_page_to_file(id, data);
}

auto Pager::try_read_page_from_file(PID id, Bytes out) const -> bool
{
    CALICO_EXPECT_FALSE(id.is_null());
    CALICO_EXPECT_EQ(page_size(), out.size());
    const auto offset = FileLayout::page_offset(id, out.size());
    if (const auto read_size = m_reader->read_at(out, offset); !read_size) {
        return false;
    } else if (read_size != out.size()) {
        throw IOError {"partial read"};
    }
    return true;
}

auto Pager::write_page_to_file(PID id, BytesView in) const -> void
{
    CALICO_EXPECT_FALSE(id.is_null());
    CALICO_EXPECT_EQ(page_size(), in.size());
    const auto offset = FileLayout::page_offset(id, in.size());
    write_all_at(*m_writer, in, offset);
}

auto Pager::sync() -> void
{
    m_writer->sync();
}

} // calico
