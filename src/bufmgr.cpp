// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#include "bufmgr.h"
#include "calicodb/env.h"
#include "encoding.h"
#include "header.h"
#include "page.h"

namespace calicodb
{

Bufmgr::Bufmgr(std::size_t page_size, std::size_t frame_count)
    : m_buffer(new(std::align_val_t{page_size}) char[page_size * frame_count]()),
      m_frame_count(frame_count),
      m_page_size(page_size)
{
    // The buffer should be aligned to the power-of-2 page size.
    CALICODB_EXPECT_TRUE(is_power_of_two(page_size));
    CALICODB_EXPECT_EQ(reinterpret_cast<std::uintptr_t>(m_buffer) % page_size, 0);

    while (m_available.size() + 1 < frame_count) {
        m_available.emplace_back(buffer_slot(m_available.size() + 1));
    }

    m_root.page_id = Id::root();
    m_root.page = buffer_slot(0);
}

Bufmgr::~Bufmgr()
{
    operator delete[](m_buffer, std::align_val_t{m_page_size});
}

auto Bufmgr::size() const -> std::size_t
{
    return m_map.size();
}

auto Bufmgr::query(Id page_id) -> PageRef *
{
    auto itr = m_map.find(page_id);
    if (itr == end(m_map)) {
        return nullptr;
    }
    return &*itr->second;
}

auto Bufmgr::get(Id page_id) -> PageRef *
{
    CALICODB_EXPECT_FALSE(page_id.is_root());
    auto itr = m_map.find(page_id);
    if (itr == end(m_map)) {
        ++m_misses;
        return nullptr;
    }
    ++m_hits;
    m_list.splice(end(m_list), m_list, itr->second);
    return &*itr->second;
}

auto Bufmgr::root() -> PageRef *
{
    return &m_root;
}

auto Bufmgr::alloc(Id page_id) -> PageRef *
{
    // The root page is already in a buffer slot. Use root() to get a reference.
    CALICODB_EXPECT_FALSE(page_id.is_root());
    CALICODB_EXPECT_EQ(query(page_id), nullptr);
    auto [itr, _] = m_map.emplace(
        page_id, m_list.emplace(end(m_list)));
    itr->second->page_id = page_id;
    pin(*itr->second);
    return &*itr->second;
}

auto Bufmgr::erase(Id page_id) -> bool
{
    CALICODB_EXPECT_FALSE(page_id.is_root());
    const auto itr = m_map.find(page_id);
    if (itr == end(m_map)) {
        return false;
    }
    unpin(*itr->second);
    m_list.erase(itr->second);
    m_map.erase(itr);
    return true;
}

auto Bufmgr::next_victim() -> PageRef *
{
    for (auto &ref : m_list) {
        if (ref.refcount == 0) {
            return &ref;
        }
    }
    return nullptr;
}

auto Bufmgr::pin(PageRef &ref) -> void
{
    CALICODB_EXPECT_FALSE(ref.page_id.is_null());
    CALICODB_EXPECT_FALSE(ref.page_id.is_root());
    CALICODB_EXPECT_FALSE(m_available.empty());
    CALICODB_EXPECT_EQ(ref.refcount, 0);

    ref.page = m_available.back();
    m_available.pop_back();
}

auto Bufmgr::unpin(PageRef &ref) -> void
{
    CALICODB_EXPECT_FALSE(ref.page_id.is_null());
    CALICODB_EXPECT_FALSE(ref.page_id.is_root());
    CALICODB_EXPECT_EQ(ref.refcount, 0);

    // The pointer put back into the available pool must (a) not belong to the root
    // page, and (b) point to the start of a page in the buffer.
    CALICODB_EXPECT_GE(ref.page, m_buffer + m_page_size);
    CALICODB_EXPECT_LE(ref.page, m_buffer + (m_frame_count - 1) * m_page_size);
    CALICODB_EXPECT_EQ(reinterpret_cast<std::uintptr_t>(ref.page) % m_page_size, 0);

    m_available.emplace_back(ref.page);
}

auto Bufmgr::ref(PageRef &ref) -> void
{
    CALICODB_EXPECT_FALSE(ref.page_id.is_null());

    ++m_refsum;
    ++ref.refcount;
}

auto Bufmgr::unref(PageRef &ref) -> void
{
    CALICODB_EXPECT_FALSE(ref.page_id.is_null());
    CALICODB_EXPECT_NE(ref.refcount, 0);
    CALICODB_EXPECT_NE(m_refsum, 0);

    --ref.refcount;
    --m_refsum;
}

auto Bufmgr::buffer_slot(std::size_t index) -> char *
{
    CALICODB_EXPECT_LT(index, m_frame_count);
    return m_buffer + index * m_page_size;
}

auto Bufmgr::hits() const -> U64
{
    return m_hits;
}

auto Bufmgr::misses() const -> U64
{
    return m_misses;
}

auto Dirtylist::remove(PageRef &ref) -> PageRef *
{
    CALICODB_EXPECT_TRUE(head);
    CALICODB_EXPECT_FALSE(head->prev);
    CALICODB_EXPECT_TRUE(ref.dirty);
    ref.dirty = false;

    if (ref.prev) {
        ref.prev->next = ref.next;
    } else {
        CALICODB_EXPECT_EQ(&ref, head);
        head = ref.next;
    }
    auto *next = ref.next;
    if (next) {
        next->prev = ref.prev;
    }
    ref.prev = nullptr;
    ref.next = nullptr;
    return next;
}

auto Dirtylist::add(PageRef &ref) -> void
{
    CALICODB_EXPECT_FALSE(ref.dirty);
    if (head) {
        CALICODB_EXPECT_FALSE(head->prev);
        head->prev = &ref;
    }
    ref.dirty = true;
    ref.prev = nullptr;
    ref.next = head;
    head = &ref;
}

} // namespace calicodb
