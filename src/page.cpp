// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#include "page.h"
#include "bufmgr.h"
#include "encoding.h"
#include "header.h"
#include "pager.h"

namespace calicodb
{

Page::Page(Pager &pager, PageRef &ref)
    : m_pager(&pager),
      m_ref(&ref),
      m_data(ref.page),
      m_size(pager.page_size()),
      m_id(ref.page_id)
{
}

Page::~Page()
{
    if (m_pager) {
        m_pager->release(std::move(*this));
    }
}

Page::Page(Page &&rhs) noexcept
{
    *this = std::move(rhs);
}

auto Page::operator=(Page &&rhs) noexcept -> Page &
{
    if (this != &rhs) {
        m_pager = rhs.m_pager;
        rhs.m_pager = nullptr;

        m_ref = rhs.m_ref;
        m_data = rhs.m_data;
        m_size = rhs.m_size;
        m_id = rhs.m_id;
        m_write = rhs.m_write;
    }
    return *this;
}

auto Page::is_writable() const -> bool
{
    return m_write;
}

auto Page::id() const -> Id
{
    return m_id;
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
