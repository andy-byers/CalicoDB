// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#include "pointer_map.h"
#include "encoding.h"
#include "pager.h"

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
        PointerMap::Type{*data},
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
    const auto mid = lookup(page_id, pager.page_size());
    const auto offset = entry_offset(mid, page_id);
    if (offset + kEntrySize > pager.page_size()) {
        return Status::corruption();
    }

    PageRef *map;
    auto s = pager.acquire(mid, map);
    if (s.is_ok()) {
        entry_out = decode_entry(map->data + offset);
        pager.release(map);
        if (entry_out.type <= kEmpty || entry_out.type >= kTypeCount) {
            s = Status::corruption();
        }
    }
    return s;
}

auto PointerMap::write_entry(Pager &pager, Id page_id, Entry entry) -> Status
{
    const auto mid = lookup(page_id, pager.page_size());

    PageRef *map;
    auto s = pager.acquire(mid, map);
    if (s.is_ok()) {
        const auto offset = entry_offset(mid, page_id);
        if (offset + kEntrySize > pager.page_size()) {
            return Status::corruption();
        }
        const auto [back_ptr, type] = decode_entry(
            map->data + offset);
        if (entry.back_ptr != back_ptr || entry.type != type) {
            pager.mark_dirty(*map);
            auto *data = map->data + offset;
            *data++ = entry.type;
            put_u32(data, entry.back_ptr.value);
        }
        pager.release(map);
    }
    return s;
}

} // namespace calicodb