#include "pager.h"

#include "frame.h"
#include "cub/exception.h"
#include "file/interface.h"
#include "page/page.h"
#include "utils/expect.h"
#include "utils/layout.h"

namespace cub {

Pager::Pager(Parameters param)
    : m_file {std::move(param.file)}
    , m_frame_count {param.frame_count}
    , m_page_size {param.page_size}
{
    if (m_frame_count < MIN_FRAME_COUNT)
        throw std::invalid_argument {"Frame count is too small"};

    if (m_frame_count > MAX_FRAME_COUNT)
        throw std::invalid_argument {"Frame count is too large"};

    CUB_EXPECT_NOT_NULL(m_file);
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
    CUB_EXPECT_EQ(m_available.size(), m_frame_count);
    m_file->resize(page_count * page_size());
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
    CUB_EXPECT_FALSE(id.is_null());
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
    CUB_EXPECT_EQ(frame.ref_count(), 0);
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
    CUB_EXPECT_FALSE(id.is_null());
    CUB_EXPECT_EQ(page_size(), out.size());
    const auto offset = FileLayout::page_offset(id, out.size());
    if (const auto read_size = m_file->read_at(out, offset); !read_size) {
        return false;
    } else if (read_size != out.size()) {
        throw IOError {"partial read"};
    }
    return true;
}

auto Pager::write_page_to_file(PID id, BytesView in) const -> void
{
    CUB_EXPECT_FALSE(id.is_null());
    CUB_EXPECT_EQ(page_size(), in.size());
    const auto offset = FileLayout::page_offset(id, in.size());
    write_exact_at(*m_file, in, offset);
}

} // cub
