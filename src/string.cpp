// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#include "calicodb/string.h"
#include "alloc.h"

namespace calicodb
{

auto String::clear() -> void
{
    Alloc::free(m_ptr);
    m_ptr = nullptr;
    m_len = 0;
    m_cap = 0;
}

} // namespace calicodb