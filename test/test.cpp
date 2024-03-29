// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#include "test.h"
#include "calicodb/config.h"
#include "calicodb/cursor.h"
#include "common.h"
#include "logging.h"
#include <filesystem>
#include <fstream>

namespace calicodb
{

void PrintTo(const Slice &s, std::ostream *os)
{
    String str;
    ASSERT_EQ(append_escaped_string(str, s), 0);
    *os << str.c_str();
}

void PrintTo(const Status &s, std::ostream *os)
{
    *os << s.message();
}

void PrintTo(const Cursor &c, std::ostream *os)
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
    File *file;
    uint64_t file_size;
    std::string buffer;
    auto s = env.new_file(filename, Env::kReadOnly, file);
    if (s.is_ok()) {
        s = file->get_size(file_size);
    }
    if (s.is_ok()) {
        buffer.resize(file_size, '\0');
        s = file->read_exact(0, file_size, buffer.data());
    }
    delete file;
    EXPECT_OK(s);
    return buffer;
}

void write_string_to_file(Env &env, const char *filename, const std::string &buffer, long offset)
{
    File *file;
    ASSERT_OK(env.new_file(filename, Env::kCreate, file));

    uint64_t write_pos;
    if (offset < 0) {
        ASSERT_OK(file->get_size(write_pos));
    } else {
        write_pos = static_cast<uint64_t>(offset);
    }
    ASSERT_OK(file->write(write_pos, buffer.c_str()));
    ASSERT_OK(file->sync());
    delete file;
}

void remove_calicodb_files(const std::string &db_name)
{
    std::filesystem::remove_all(db_name);
    std::filesystem::remove_all(db_name + kDefaultWalSuffix.to_string());
    std::filesystem::remove_all(db_name + kDefaultShmSuffix.to_string());
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

int main(int argc, char **argv)
{
    using namespace calicodb;
    using namespace calicodb::test;
    testing::InitGoogleTest(&argc, argv);
    default_env().srand(42);

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
    auto s = configure(kReplaceAllocator, DebugAllocator::config());
    if (!s.is_ok()) {
        TEST_LOG << "failed to set debug allocator: " << s.message() << '\n';
        REPORT_DRIVER_ERROR;
        rc = -1;
    }
    if (rc == 0) {
        rc = RUN_ALL_TESTS(); // Run the registered tests.
    }
    if (rc == 0 && DebugAllocator::bytes_used() > 0) {
        // Only report leaks if the test passed. There may be a leak if an ASSERT_*() fired.
        TEST_LOG << "error: test leaked " << DebugAllocator::bytes_used() << " byte(s)\n";
        REPORT_DRIVER_ERROR;
        rc = -1;
    }

    TEST_LOG << '[' << (rc ? "FAIL" : "PASS") << "]\n";
    return rc;
}