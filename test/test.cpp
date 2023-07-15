// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#include "test.h"
#include "calicodb/cursor.h"
#include "logging.h"

namespace calicodb
{

auto PrintTo(const Slice &s, std::ostream *os) -> void
{
    *os << escape_string(s);
}

auto PrintTo(const Status &s, std::ostream *os) -> void
{
    *os << s.to_string();
}

auto PrintTo(const Cursor &c, std::ostream *os) -> void
{
    *os << "Cursor{";
    if (c.is_valid()) {
        const auto total_k = c.key();
        const auto short_k = escape_string(total_k)
                                 .substr(0, std::min(total_k.size(), 5UL));
        *os << '"' << short_k;
        if (total_k.size() > short_k.size()) {
            *os << R"(" + <)" << total_k.size() - short_k.size() << " bytes>";
        } else {
            *os << '"';
        }

        const auto total_v = c.value();
        const auto short_v = escape_string(total_v)
                                 .substr(0, std::min(total_v.size(), 5UL));
        *os << R"(",")" << short_v;
        if (total_v.size() > short_v.size()) {
            *os << R"(" + <)" << total_v.size() - short_v.size() << " bytes>";
        } else {
            *os << '"';
        }
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
        return testing::AssertionFailure() << expr << ": " << s.to_string();
    }
}

auto read_file_to_string(Env &env, const std::string &filename) -> std::string
{
    std::size_t file_size;
    const auto s = env.file_size(filename, file_size);
    if (s.is_io_error()) {
        // File was unlinked.
        return "";
    }
    std::string buffer(file_size, '\0');

    File *file;
    EXPECT_OK(env.new_file(filename, Env::kReadOnly, file));
    EXPECT_OK(file->read_exact(0, file_size, buffer.data()));
    delete file;

    return buffer;
}

auto write_string_to_file(Env &env, const std::string &filename, const std::string &buffer, long offset) -> void
{
    File *file;
    ASSERT_OK(env.new_file(filename, Env::kCreate, file));

    std::size_t write_pos;
    if (offset < 0) {
        ASSERT_OK(env.file_size(filename, write_pos));
    } else {
        write_pos = static_cast<std::size_t>(offset);
    }
    ASSERT_OK(file->write(write_pos, buffer));
    ASSERT_OK(file->sync());
    delete file;
}

} // namespace test

} // namespace calicodb
