// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#include <cstdio>

class CustomString
{
public:
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

    void append(const char *, size_t)
    {
    }
};

#define CALICODB_STRING CustomString
#include "calicodb/slice.h"

int main(int, const char *[])
{
    CALICODB_STRING string("42", 2);
    (void)calicodb::Slice(string);
    return 0;
}