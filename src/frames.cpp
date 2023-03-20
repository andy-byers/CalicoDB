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

AlignedBuffer::AlignedBuffer(std::size_t size, std::size_t alignment)
    : m_data {
          new (std::align_val_t {alignment}) char[size](),
          Deleter {std::align_val_t {alignment}},
      }
{
    CALICODB_EXPECT_TRUE(is_power_of_two(alignment));
    CALICODB_EXPECT_EQ(size % alignment, 0);
}

auto DirtyTable::size() const -> std::size_t
{
    return m_dirty.size();
}

auto DirtyTable::begin() -> Iterator
{
    using std::begin;
    return begin(m_dirty);
}

auto DirtyTable::end() -> Iterator
{
    using std::end;
    return end(m_dirty);
}

auto DirtyTable::insert(Id page_id, Lsn record_lsn) -> Iterator
{
    // Use find() to get the insert() overload that returns an iterator, and to
    // assert that non-NULL LSNs are unique. The "record_lsn" is NULL when the
    // page has never been written to.
    auto itr = m_dirty.find(record_lsn);
    CALICODB_EXPECT_TRUE(record_lsn.is_null() || itr == end());
    return m_dirty.insert(itr, {record_lsn, page_id});
}

auto DirtyTable::remove(Iterator itr) -> Iterator
{
    return m_dirty.erase(itr);
}

auto DirtyTable::recovery_lsn() const -> Lsn
{
    auto itr = m_dirty.lower_bound(Lsn::root());
    if (itr == m_dirty.end()) {
        return Lsn::null();
    }
    return itr->first;
}

auto PageCache::size() const -> std::size_t
{
    return m_map.size();
}

auto PageCache::query(Id page_id) -> CacheEntry *
{
    auto itr = m_map.find(page_id);
    if (itr == end(m_map)) {
        return nullptr;
    }
    return &*itr->second;
}

auto PageCache::get(Id page_id) -> CacheEntry *
{
    auto itr = m_map.find(page_id);
    if (itr == end(m_map)) {
        ++m_misses;
        return nullptr;
    }
    ++m_hits;
    m_list.splice(end(m_list), m_list, itr->second);
    return &*itr->second;
}

auto PageCache::put(CacheEntry entry) -> CacheEntry *
{
    CALICODB_EXPECT_EQ(query(entry.page_id), nullptr);
    auto [itr, _] = m_map.emplace(
        entry.page_id, m_list.emplace(end(m_list), entry));
    return &*itr->second;
}

auto PageCache::erase(Id page_id) -> bool
{
    const auto itr = m_map.find(page_id);
    if (itr == end(m_map)) {
        return false;
    }
    const auto &[_, entry] = *itr;
    m_list.erase(entry);
    m_map.erase(itr);
    return true;
}

auto PageCache::evict() -> std::optional<CacheEntry>
{
    auto itr = begin(m_list);
    while (itr != end(m_list)) {
        if (itr->refcount == 0) {
            auto entry = *itr;
            m_map.erase(itr->page_id);
            m_list.erase(itr);
            return entry;
        }
        ++itr;
    }
    return std::nullopt;
}

Frame::Frame(char *ptr)
    : data {ptr}
{
}

auto Frame::lsn() const -> Lsn
{
    return {get_u64(data + page_id.is_root() * FileHeader::kSize)};
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

    while (m_unpinned.size() < m_frames.size()) {
        m_unpinned.emplace_back(m_unpinned.size());
    }
}

auto FrameManager::upgrade(Page &page) -> void
{
    CALICODB_EXPECT_FALSE(page.is_writable());
    page.m_write = true;
}

auto FrameManager::pin(Id page_id, CacheEntry &entry) -> Status
{
    CALICODB_EXPECT_FALSE(page_id.is_null());
    CALICODB_EXPECT_FALSE(m_unpinned.empty());
    CALICODB_EXPECT_EQ(entry.refcount, 0);

    auto &frame = m_frames[m_unpinned.back()];
    CALICODB_TRY(read_page_from_file(page_id, frame.data));

    // Associate the page ID with the frame index.
    entry.page_id = page_id;
    frame.page_id = page_id;
    entry.index = m_unpinned.back();
    m_unpinned.pop_back();
    return Status::ok();
}

auto FrameManager::unpin(CacheEntry &entry) -> void
{
    CALICODB_EXPECT_LT(entry.index, m_frames.size());
    CALICODB_EXPECT_FALSE(entry.page_id.is_null());
    CALICODB_EXPECT_EQ(entry.refcount, 0);
    m_frames[entry.index].page_id = Id::null();
    m_unpinned.emplace_back(entry.index);
}

auto FrameManager::ref(CacheEntry &entry, Page &page) -> Status
{
    CALICODB_EXPECT_FALSE(entry.page_id.is_null());

    ++m_refsum;
    ++entry.refcount;
    page.m_id = entry.page_id;
    page.m_data = m_frames[entry.index].data;
    page.m_size = m_page_size;
    page.m_write = false;
    return Status::ok();
}

auto FrameManager::unref(CacheEntry &entry) -> void
{
    CALICODB_EXPECT_FALSE(entry.page_id.is_null());
    CALICODB_EXPECT_NE(entry.refcount, 0);
    CALICODB_EXPECT_NE(m_refsum, 0);

    --m_refsum;
    --entry.refcount;
}

auto FrameManager::write_back(std::size_t index) -> Status
{
    auto &frame = get_frame(index);
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

    Slice slice;
    CALICODB_TRY(m_file->read(offset, m_page_size, out, &slice));
    m_bytes_read += slice.size();

    if (slice.is_empty()) {
        std::memset(out, 0, m_page_size);
    } else if (slice.size() != m_page_size) {
        return Status::io_error("incomplete read");
    }
    return Status::ok();
}

auto FrameManager::write_page_to_file(Id pid, const char *in) const -> Status
{
    m_bytes_written += m_page_size;
    return m_file->write(pid.as_index() * m_page_size, {in, m_page_size});
}

} // namespace calicodb
