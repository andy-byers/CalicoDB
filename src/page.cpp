// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#include "page.h"
#include "bufmgr.h"
#include "header.h"
#include "pager.h"

namespace calicodb
{

Page::~Page()
{
    // TODO: Is this really necessary? If the tree layer is more careful and doesn't drop
    //       pages when it encounters an error (careless use of CALICODB_TRY to return early
    //       b/c it's convenient).
    if (m_pager) {
        m_pager->release(std::move(*this));
        m_pager = nullptr;
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
    }
    return *this;
}

} // namespace calicodb
