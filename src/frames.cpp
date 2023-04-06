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
    : m_data(
          new(std::align_val_t {alignment}) char[size](),
          Deleter {std::align_val_t {alignment}})
{
    CALICODB_EXPECT_TRUE(is_power_of_two(alignment));
    CALICODB_EXPECT_EQ(size % alignment, 0);

    data = m_data.get();
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

auto PageCache::alloc(Id page_id) -> CacheEntry *
{
    CALICODB_EXPECT_EQ(query(page_id), nullptr);
    auto [itr, _] = m_map.emplace(
        page_id, m_list.emplace(end(m_list)));
    itr->second->page_id = page_id;
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

auto PageCache::next_victim() -> CacheEntry *
{
    for (auto &entry : m_list) {
        if (entry.refcount == 0) {
            return &entry;
        }
    }
    return nullptr;
}

auto PageCache::hits() const -> U64
{
    return m_hits;
}

auto PageCache::misses() const -> U64
{
    return m_misses;
}

FrameManager::FrameManager(AlignedBuffer buffer, std::size_t page_size, std::size_t frame_count)
    : m_buffer(std::move(buffer)),
      m_frame_count(frame_count),
      m_page_size(page_size)
{
    // The buffer should be aligned to the page size.
    CALICODB_EXPECT_EQ(reinterpret_cast<std::uintptr_t>(m_buffer.data) % page_size, 0);

    while (m_unpinned.size() < frame_count) {
        m_unpinned.emplace_back(m_unpinned.size());
    }
}

auto FrameManager::get_frame_pointer(std::size_t index) -> char *
{
    CALICODB_EXPECT_LT(index, m_frame_count);
    return m_buffer.data + index * m_page_size;
}

auto FrameManager::upgrade(Page &page) -> void
{
    CALICODB_EXPECT_FALSE(page.is_writable());
    page.m_write = true;
}

auto FrameManager::pin(CacheEntry &entry) -> void
{
    CALICODB_EXPECT_FALSE(entry.page_id.is_null());
    CALICODB_EXPECT_FALSE(m_unpinned.empty());
    CALICODB_EXPECT_EQ(entry.refcount, 0);

    entry.index = m_unpinned.back();
    entry.page = get_frame_pointer(m_unpinned.back());
    m_unpinned.pop_back();
}

auto FrameManager::unpin(CacheEntry &entry) -> void
{
    CALICODB_EXPECT_LT(entry.index, m_frame_count);
    CALICODB_EXPECT_FALSE(entry.page_id.is_null());
    CALICODB_EXPECT_EQ(entry.refcount, 0);
    m_unpinned.emplace_back(entry.index);
}

auto FrameManager::ref(CacheEntry &entry, Page &page) -> void
{
    CALICODB_EXPECT_FALSE(entry.page_id.is_null());
    CALICODB_EXPECT_EQ(entry.page, get_frame_pointer(entry.index));

    ++m_refsum;
    ++entry.refcount;
    page.m_id = entry.page_id;
    page.m_entry = &entry;
    page.m_data = entry.page;
    page.m_size = m_page_size;
    page.m_write = false;
}

auto FrameManager::unref(CacheEntry &entry) -> void
{
    CALICODB_EXPECT_FALSE(entry.page_id.is_null());
    CALICODB_EXPECT_NE(entry.refcount, 0);
    CALICODB_EXPECT_NE(m_refsum, 0);

    --entry.refcount;
    --m_refsum;
}

} // namespace calicodb
