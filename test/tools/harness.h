// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#ifndef CALICODB_TEST_TOOLS_HARNESS_H
#define CALICODB_TEST_TOOLS_HARNESS_H

#include "calicodb/status.h"
#include <gtest/gtest.h>

namespace calicodb
{

#define ASSERT_OK(s) ASSERT_PRED_FORMAT1(check_status, s)
#define ASSERT_NOK(s) ASSERT_FALSE((s).is_ok())
#define EXPECT_OK(s) EXPECT_PRED_FORMAT1(check_status, s)
#define EXPECT_NOK(s) EXPECT_FALSE((s).is_ok())

auto check_status(const char *expr, const Status &s) -> testing::AssertionResult;

} // namespace calicodb

#endif // CALICODB_TEST_TOOLS_HARNESS_H
