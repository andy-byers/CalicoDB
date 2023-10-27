// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#include "pointer_map.h"
#include "encoding.h"
#include "pager.h"
#include "status_internal.h"

namespace calicodb
{

namespace
{

constexpr auto kEntrySize =
    sizeof(char) +    // Type (1 B)
    sizeof(uint32_t); // Back pointer (4 B)

auto entry_offset(Id map_id, Id page_id) -> size_t
{
    CALICODB_EXPECT_LT(map_id, page_id);
    return (page_id.value - map_id.value - 1) * kEntrySize;
}

auto decode_entry(const char *data) -> PointerMap::Entry
{
    return {
        Id(get_u32(data + 1)),
        static_cast<PageType>(*data),
    };
}

} // namespace

auto PointerMap::lookup(Id page_id, size_t page_size) -> Id
{
    // Root page (1) has no parents, and page 2 is the first pointer map page. If `page_id` is a pointer map
    // page, `page_id` will be returned.
    if (page_id.value < kFirstMapPage) {
        return Id::null();
    }
    const auto len = page_size / kEntrySize + 1;
    const auto idx = (page_id.value - kFirstMapPage) / len;
    return Id(idx * len + kFirstMapPage);
}

auto PointerMap::read_entry(Pager &pager, Id page_id, Entry &entry_out) -> Status
{
    const auto map_id = lookup(page_id, pager.page_size());
    if (map_id.is_null() || page_id <= map_id) {
        return Status::corruption();
    }
    const auto offset = entry_offset(map_id, page_id);
    CALICODB_EXPECT_LE(offset + kEntrySize, pager.page_size());

    PageRef *map;
    auto s = pager.acquire(map_id, map);
    if (s.is_ok()) {
        entry_out = decode_entry(map->data + offset);
        pager.release(map);
        if (entry_out.type == kInvalidPage || entry_out.type >= kPageTypeCount) {
            s = StatusBuilder::corruption("pointer map page type %u is invalid",
                                          entry_out.type);
        }
    }
    return s;
}

auto PointerMap::write_entry(Pager &pager, Id page_id, Entry entry, Status &s) -> void
{
    if (!s.is_ok()) {
        return;
    }
    const auto map_id = lookup(page_id, pager.page_size());
    if (map_id.is_null() || page_id <= map_id) {
        s = Status::corruption();
        return;
    }
    PageRef *map;
    s = pager.acquire(map_id, map);
    if (s.is_ok()) {
        const auto offset = entry_offset(map_id, page_id);
        CALICODB_EXPECT_LE(offset + kEntrySize, pager.page_size());

        const auto [back_ptr, type] = decode_entry(
            map->data + offset);
        if (entry.back_ptr != back_ptr || entry.type != type) {
            pager.mark_dirty(*map);
            auto *data = map->data + offset;
            *data++ = static_cast<char>(entry.type);
            put_u32(data, entry.back_ptr.value);
        }
        pager.release(map);
    }
}

} // namespace calicodb