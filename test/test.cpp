// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#include "test.h"

namespace calicodb::test
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

} // namespace calicodb::test