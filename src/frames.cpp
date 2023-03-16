// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#include "frames.h"
#include "calicodb/env.h"
#include "encoding.h"
#include "header.h"
#include "page.h"

namespace calicodb
{

Frame::Frame(char *ptr)
    : data {ptr}
{
}

auto Frame::lsn() const -> Lsn
{
    return {get_u64(data + page_id.is_root() * FileHeader::kSize)};
}

auto Frame::ref() -> void
{
    CALICODB_EXPECT_FALSE(write);
    ++ref_count;
}

auto Frame::upgrade() -> void
{
    CALICODB_EXPECT_FALSE(write);
    write = true;
}

auto Frame::unref() -> void
{
    CALICODB_EXPECT_GT(ref_count, 0);
    write = false;
    --ref_count;
}

FrameManager::FrameManager(Editor &file, AlignedBuffer buffer, std::size_t page_size, std::size_t frame_count)
    : m_buffer {std::move(buffer)},
      m_file {&file},
      m_page_size {page_size}
{
    // The buffer should be aligned to the page size.
    CALICODB_EXPECT_EQ(reinterpret_cast<std::uintptr_t>(m_buffer.get()) % page_size, 0);

    while (m_frames.size() < frame_count) {
        m_frames.emplace_back(m_buffer.get() + m_frames.size() * page_size);
    }

    while (m_available.size() < m_frames.size()) {
        m_available.emplace_back(m_available.size());
    }
}

auto FrameManager::ref(std::size_t index, Page &out) -> void
{
    ++m_ref_sum;
    CALICODB_EXPECT_LT(index, m_frames.size());
    m_frames[index].ref();
    out.m_id = m_frames[index].page_id;
    out.m_data = m_frames[index].data;
    out.m_size = m_page_size;
    out.m_write = false;
}

auto FrameManager::unref(std::size_t index, Page) -> void
{
    CALICODB_EXPECT_LT(index, m_frames.size());
    m_frames[index].unref();
    --m_ref_sum;
}

auto FrameManager::upgrade(std::size_t index, Page &page) -> void
{
    CALICODB_EXPECT_LT(index, m_frames.size());
    CALICODB_EXPECT_FALSE(m_frames[index].write);
    CALICODB_EXPECT_FALSE(page.is_writable());
    m_frames[index].upgrade();
    page.m_write = true;
}

auto FrameManager::pin(Id page_id, std::size_t &index) -> Status
{
    CALICODB_EXPECT_FALSE(page_id.is_null());

    if (m_available.empty()) {
        return Status::not_found("out of frames");
    }

    index = m_available.back();
    CALICODB_EXPECT_LT(index, m_frames.size());
    auto &frame = m_frames[index];
    CALICODB_EXPECT_EQ(frame.ref_count, 0);

    auto s = read_page_from_file(page_id, frame.data);
    if (s.is_not_found()) {
        // We just tried to read at or past EOF. This happens when we allocate a new page or roll the WAL forward.
        std::memset(frame.data, 0, m_page_size);
        ++m_page_count;
    } else if (!s.is_ok()) {
        return s;
    }
    m_available.pop_back();
    frame.page_id = page_id;
    return Status::ok();
}

auto FrameManager::unpin(std::size_t index) -> void
{
    CALICODB_EXPECT_LT(index, m_frames.size());
    CALICODB_EXPECT_EQ(m_frames[index].ref_count, 0);
    m_frames[index].page_id = Id::null();
    m_available.emplace_back(index);
}

auto FrameManager::write_back(std::size_t index) -> Status
{
    auto &frame = get_frame(index);
    CALICODB_EXPECT_LE(frame.ref_count, 1);

    m_bytes_written += m_page_size;
    return write_page_to_file(frame.page_id, frame.data);
}

auto FrameManager::sync() -> Status
{
    return m_file->sync();
}

auto FrameManager::read_page_from_file(Id page_id, char *out) const -> Status
{
    const auto offset = page_id.as_index() * m_page_size;

    // Don't even try to call read() if the file isn't large enough. The system call can be pretty slow even if it doesn't read anything.
    // This happens when we are allocating a page from the end of the file.
    if (offset >= m_page_count * m_page_size) {
        return Status::not_found("end of file");
    }

    Slice slice;
    CALICODB_TRY(m_file->read(offset, m_page_size, out, &slice));
    m_bytes_read += slice.size();

    // We should always read exactly what we requested, unless we are allocating a page during recovery.
    if (slice.size() == m_page_size) {
        return Status::ok();
    }

    // In that case, we will hit EOF here.
    if (slice.is_empty()) {
        return Status::not_found("end of file");
    }

    return Status::io_error("incomplete read");
}

auto FrameManager::write_page_to_file(Id pid, const char *in) const -> Status
{
    m_bytes_written += m_page_size;
    return m_file->write(pid.as_index() * m_page_size, {in, m_page_size});
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

} // namespace calicodb
