// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#ifndef CALICODB_TEST_TEST_H
#define CALICODB_TEST_TEST_H

#include "calicodb/cursor.h"
#include "calicodb/db.h"
#include "calicodb/env.h"
#include "stest/rule.h"
#include <gtest/gtest.h>

#ifndef TEST_PAGE_SIZE
#define TEST_PAGE_SIZE 1'024U
#endif // TEST_PAGE_SIZE

namespace calicodb
{

class Cursor;
void PrintTo(const Slice &s, std::ostream *os);
void PrintTo(const Status &s, std::ostream *os);
void PrintTo(const Cursor &c, std::ostream *os);

namespace test
{

// Get a reference to a stream used for writing messages during testing. Defaults
// to a stream that doesn't write anything. Tests that wish to write log messages
// to a file on disk should write a message identifying themselves so that their
// output has more context (gtest output is not written to TEST_LOG).
#define TEST_LOG (*calicodb::test::g_logger)
extern std::ostream *g_logger;

#define ASSERT_OK(s) ASSERT_PRED_FORMAT1(calicodb::test::check_status, s)
#define ASSERT_NOK(s) ASSERT_FALSE((s).is_ok())
#define EXPECT_OK(s) EXPECT_PRED_FORMAT1(calicodb::test::check_status, s)
#define EXPECT_NOK(s) EXPECT_FALSE((s).is_ok())

auto check_status(const char *expr, const Status &s) -> testing::AssertionResult;

auto read_file_to_string(Env &env, const char *filename) -> std::string;
void write_string_to_file(Env &env, const char *filename, const std::string &buffer, long offset = -1);
void remove_calicodb_files(const std::string &db_name);

} // namespace test

} // namespace calicodb

#endif // CALICODB_TEST_TEST_H
