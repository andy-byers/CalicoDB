// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#include "page.h"
#include "encoding.h"
#include "frames.h"
#include "header.h"
#include "pager.h"

namespace calicodb
{

auto Page::is_writable() const -> bool
{
    return m_write;
}

auto Page::id() const -> Id
{
    return m_id;
}

auto Page::entry() const -> const CacheEntry *
{
    return m_entry;
}

auto Page::view() const -> Slice
{
    return Slice(m_data, m_size);
}

auto Page::data() -> char *
{
    return m_data;
}

auto Page::data() const -> const char *
{
    return m_data;
}

auto Page::size() const -> std::size_t
{
    return m_size;
}

[[nodiscard]] auto page_offset(Id page_id) -> std::size_t
{
    return FileHeader::kSize * page_id.is_root();
}

[[nodiscard]] auto read_next_id(const Page &page) -> Id
{
    return Id(get_u32(page.data() + page_offset(page.id())));
}

auto write_next_id(Page &page, Id next_id) -> void
{
    put_u32(page.data() + page_offset(page.id()), next_id.value);
}

} // namespace calicodb
