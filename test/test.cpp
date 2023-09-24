// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#include "test.h"
#include "calicodb/config.h"
#include "calicodb/cursor.h"
#include "common.h"
#include "logging.h"
#include <fstream>

namespace calicodb
{

auto PrintTo(const Slice &s, std::ostream *os) -> void
{
    String str;
    ASSERT_EQ(append_escaped_string(str, s), 0);
    *os << str.c_str();
}

auto PrintTo(const Status &s, std::ostream *os) -> void
{
    *os << s.message();
}

auto PrintTo(const Cursor &c, std::ostream *os) -> void
{
    *os << "Cursor{";
    if (c.is_valid()) {
        String s;
        ASSERT_EQ(append_strings(s, "\""), 0);
        ASSERT_EQ(append_escaped_string(s, c.key()), 0);
        ASSERT_EQ(append_strings(s, R"(",")"), 0);
        ASSERT_EQ(append_escaped_string(s, c.value()), 0);
        ASSERT_EQ(append_strings(s, "\""), 0);
    }
    *os << '}';
}

namespace test
{

auto check_status(const char *expr, const Status &s) -> testing::AssertionResult
{
    if (s.is_ok()) {
        return testing::AssertionSuccess();
    } else {
        return testing::AssertionFailure() << expr << ": " << s.message();
    }
}

auto read_file_to_string(Env &env, const char *filename) -> std::string
{
    size_t file_size;
    auto s = env.file_size(filename, file_size);
    if (!s.is_ok()) {
        if (!s.is_io_error()) {
            ADD_FAILURE() << s.message();
        }
        return "";
    }
    std::string buffer(file_size, '\0');

    File *file;
    s = env.new_file(filename, Env::kReadOnly, file);
    if (s.is_ok()) {
        s = file->read_exact(0, file_size, buffer.data());
    }
    delete file;
    EXPECT_OK(s);
    return buffer;
}

auto write_string_to_file(Env &env, const char *filename, const std::string &buffer, long offset) -> void
{
    File *file;
    ASSERT_OK(env.new_file(filename, Env::kCreate, file));

    size_t write_pos;
    if (offset < 0) {
        ASSERT_OK(env.file_size(filename, write_pos));
    } else {
        write_pos = static_cast<size_t>(offset);
    }
    ASSERT_OK(file->write(write_pos, buffer.c_str()));
    ASSERT_OK(file->sync());
    delete file;
}

// From john's answer to https://stackoverflow.com/questions/11826554
class NullBuffer : public std::streambuf
{
public:
    auto overflow(int c) -> int
    {
        return c;
    }
} g_null_buffer;

std::ostream g_null_stream(&g_null_buffer);
std::ofstream g_file_stream;

// Write extra messages to this stream during testing.
std::ostream *g_logger = &g_null_stream;

} // namespace test

} // namespace calicodb

// Indicate where the driver code encountered a problem. Additional information is written
// to *g_logger.
#define REPORT_DRIVER_ERROR std::cerr << "driver error (" __FILE__ << ':' << __LINE__ << ")\n"

auto main(int argc, char **argv) -> int
{
    using namespace calicodb;
    using namespace calicodb::test;
    ::testing::InitGoogleTest(&argc, argv);

#define STR(name) #name
#define XSTR(name) STR(name)
    static constexpr Slice kLogFile(XSTR(CALICODB_TEST_LOG_FILE));
    if (kLogFile.starts_with("STDOUT")) {
        g_logger = &std::cout;
    } else if (kLogFile.starts_with("STDERR")) {
        g_logger = &std::cerr;
    } else {
        g_file_stream.open(kLogFile.to_string(), std::ios::app);
        if (g_file_stream.is_open()) {
            g_logger = &g_file_stream;
        }
    }

    TEST_LOG << "Running \"" << argv[0] << "\"...\n";

    int rc = 0;
    auto s = configure(kSetAllocator, DebugAllocator::config());
    if (!s.is_ok()) {
        TEST_LOG << "failed to set debug allocator: " << s.message() << '\n';
        REPORT_DRIVER_ERROR;
        rc = -1;
    }
    if (rc == 0) {
        rc = RUN_ALL_TESTS(); // Run the registered tests.
    }
    if (rc == 0 && DebugAllocator::bytes_used() > 0) {
        // Only report leaks if the test passed. There will likely be a leak if an
        // ASSERT_*() fired.
        TEST_LOG << "error: test leaked " << DebugAllocator::bytes_used() << " byte(s)\n";
        REPORT_DRIVER_ERROR;
        rc = -1;
    }

    TEST_LOG << '[' << (rc ? "FAIL" : "PASS") << "]\n";
    return rc;
}