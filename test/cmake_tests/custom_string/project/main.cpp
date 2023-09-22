// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#include <cstdlib>

class CustomString
{
public:
    CustomString(const char *)
    {
    }

    CustomString(const char *, size_t)
    {
    }

    auto data() const -> const char *
    {
        return "";
    }

    auto size() const -> size_t
    {
        return 0;
    }

    auto append(const char *, size_t) -> void
    {
    }
};

#define CALICODB_STRING CustomString
#include "calicodb/slice.h"

auto main(int, const char *[]) -> int
{
    CALICODB_STRING string("42");
    (void)calicodb::Slice(string);
    return 0;
}