// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#include "test.h"
#include "allocator.h"
#include "calicodb/cursor.h"
#include "logging.h"

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

} // namespace test

} // namespace calicodb

auto main(int argc, char **argv) -> int
{
    ::testing::InitGoogleTest(&argc, argv);
    calicodb::Mem::set_methods(calicodb::DebugAllocator::methods());
    return RUN_ALL_TESTS();
}