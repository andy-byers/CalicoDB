// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#include <gtest/gtest.h>

extern "C" {
auto __ubsan_on_report() -> void
{
    FAIL() << "UBSan detected a problem";
}
auto __asan_on_error() -> void
{
    FAIL() << "ASan detected a problem";
}
auto __tsan_on_report() -> void
{
    FAIL() << "TSan detected a problem";
}
} // extern "C"
