#include "pager.h"
#include "frame.h"
#include "storage/file.h"
#include "utils/expect.h"
#include "utils/layout.h"
#include "utils/logging.h"

namespace cco {

auto Pager::open(Parameters param) -> Result<std::unique_ptr<Pager>>
{
    CCO_EXPECT_TRUE(is_power_of_two(param.page_size));
    CCO_EXPECT_GE(param.page_size, MINIMUM_PAGE_SIZE);
    CCO_EXPECT_LE(param.page_size, MAXIMUM_PAGE_SIZE);
    CCO_EXPECT_GE(param.frame_count, MINIMUM_FRAME_COUNT);
    CCO_EXPECT_LE(param.frame_count, MAXIMUM_FRAME_COUNT);

    const auto cache_size = param.page_size * param.frame_count;
    auto buffer = std::unique_ptr<Byte[], AlignedDeleter> {
        new (static_cast<std::align_val_t>(param.page_size), std::nothrow) Byte[cache_size],
        AlignedDeleter {static_cast<std::align_val_t>(param.page_size)}};

    if (!buffer) {
        ThreePartMessage message;
        message.set_primary("cannot open pager");
        message.set_detail("allocation of {} B cache failed", cache_size);
        message.set_hint("out of memory");
        return Err {message.system_error()};
    }
    return std::unique_ptr<Pager> {new Pager {std::move(buffer), std::move(param)}};
}

Pager::Pager(AlignedBuffer buffer, Parameters param)
    : m_buffer {std::move(buffer)},
      m_file {std::move(param.file)},
      m_flushed_lsn {param.flushed_lsn},
      m_frame_count {param.frame_count},
      m_page_size {param.page_size}
{
    // The buffer should be aligned to the page size.
    CCO_EXPECT_EQ(reinterpret_cast<std::uintptr_t>(m_buffer.get()) % m_page_size, 0);
    mem_clear({m_buffer.get(), m_page_size * m_frame_count});

    while (m_available.size() < m_frame_count)
        m_available.emplace_back(m_buffer.get(), m_available.size(), m_page_size);
}

auto Pager::close() -> Result<void>
{
    return m_file->close();
}

auto Pager::available() const -> Size
{
    return m_available.size();
}

auto Pager::page_size() const -> Size
{
    return m_page_size;
}

auto Pager::truncate(Size page_count) -> Result<void>
{
    CCO_EXPECT_EQ(m_available.size(), m_frame_count);
    return m_file->resize(page_count * page_size());
}

auto Pager::pin(PageId id) -> Result<Frame>
{
    CCO_EXPECT_FALSE(id.is_null());
    if (m_available.empty())
        m_available.emplace_back(Frame {page_size()});

    auto &frame = m_available.back();

    if (auto result = read_page_from_file(id, frame.data()); !result) {
        return Err {result.error()};
    } else if (!result.value()) {
        mem_clear(frame.data());
    }

    auto moved = std::move(m_available.back());
    m_available.pop_back();
    moved.reset(id);
    return moved;
}

auto Pager::clean(Frame &frame) -> Result<void>
{
    CCO_EXPECT_TRUE(frame.is_dirty());
    auto result = write_page_to_file(frame.page_id(), frame.data());
    if (result.has_value())
        frame.reset(frame.page_id());
    return result;
}

auto Pager::discard(Frame frame) -> void
{
    frame.purge();
    if (!frame.is_owned())
        m_available.emplace_back(std::move(frame));
}

auto Pager::unpin(Frame frame) -> Result<void>
{
    CCO_EXPECT_EQ(frame.ref_count(), 0);
    Result<void> result;

    // If this fails, the caller (buffer pool) will need to roll back the database state or exit.
    if (frame.is_dirty()) {
        result = write_page_to_file(frame.page_id(), frame.data());

        if (result.has_value())
            m_flushed_lsn = std::max(m_flushed_lsn, frame.page_lsn());
    }

    frame.reset(PageId::null());
    if (!frame.is_owned())
        m_available.emplace_back(std::move(frame));
    return result;
}

auto Pager::sync() -> Result<void>
{
    return m_file->sync();
}

auto Pager::read_page_from_file(PageId id, Bytes out) const -> Result<bool>
{
    static constexpr auto ERROR_PRIMARY = "cannot read page";
    static constexpr auto ERROR_DETAIL = "page ID is {}";
    CCO_EXPECT_EQ(page_size(), out.size());

    CCO_TRY_CREATE(file_size, m_file->size());
    const auto offset = FileLayout::page_offset(id, out.size());

    // Don't even try to call read() if the file isn't large enough. It's pretty slow even if it doesn't read anything.
    if (offset + out.size() > file_size)
        return false;

    const auto was_read = m_file->read(out, offset);

    // System call error.
    if (!was_read.has_value())
        return Err {was_read.error()};

    if (const auto read_size = *was_read; read_size == out.size()) {
        // Just read the whole page.
        return true;
    } else {
        // Incomplete read.
        ThreePartMessage message;
        message.set_primary(ERROR_PRIMARY);
        message.set_detail(ERROR_DETAIL, id.value);
        message.set_hint("incomplete read of {} B", read_size);
        return Err {message.system_error()};
    }
}

auto Pager::write_page_to_file(PageId id, BytesView in) const -> Result<void>
{
    CCO_EXPECT_EQ(page_size(), in.size());
    return write_all(*m_file, in, FileLayout::page_offset(id, in.size()));
}

auto Pager::load_header(const FileHeaderReader &reader) -> void
{
    m_flushed_lsn = reader.flushed_lsn();
}

auto Pager::save_header(FileHeaderWriter &writer) -> void
{
    writer.set_flushed_lsn(m_flushed_lsn);
}

} // namespace cco
