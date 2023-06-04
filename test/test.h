// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#ifndef CALICODB_TEST_TEST_H
#define CALICODB_TEST_TEST_H

#include "calicodb/env.h"
#include <gtest/gtest.h>

namespace calicodb::test
{

#define ASSERT_OK(s) ASSERT_PRED_FORMAT1(calicodb::test::check_status, s)
#define ASSERT_NOK(s) ASSERT_FALSE((s).is_ok())
#define EXPECT_OK(s) EXPECT_PRED_FORMAT1(calicodb::test::check_status, s)
#define EXPECT_NOK(s) EXPECT_FALSE((s).is_ok())

auto check_status(const char *expr, const Status &s) -> testing::AssertionResult;

class FileWrapper : public File
{
public:
    explicit FileWrapper(File &target)
        : m_target(&target)
    {
    }

    ~FileWrapper() override = default;

    [[nodiscard]] auto read(std::size_t offset, std::size_t size, char *scratch, Slice *out) -> Status override
    {
        return m_target->read(offset, size, scratch, out);
    }

    [[nodiscard]] auto write(std::size_t offset, const Slice &in) -> Status override
    {
        return m_target->write(offset, in);
    }

    [[nodiscard]] auto sync() -> Status override
    {
        return m_target->sync();
    }

    [[nodiscard]] auto file_lock(FileLockMode mode) -> Status override
    {
        return m_target->file_lock(mode);
    }

    auto file_unlock() -> void override
    {
        return m_target->file_unlock();
    }

    [[nodiscard]] auto shm_map(std::size_t r, bool extend, volatile void *&out) -> Status override
    {
        return m_target->shm_map(r, extend, out);
    }

    [[nodiscard]] auto shm_lock(std::size_t r, std::size_t n, ShmLockFlag flags) -> Status override
    {
        return m_target->shm_lock(r, n, flags);
    }

    auto shm_unmap(bool unlink) -> void override
    {
        return m_target->shm_unmap(unlink);
    }

    auto shm_barrier() -> void override
    {
        return m_target->shm_barrier();
    }

protected:
    File *m_target;
};

template <std::size_t Length = 16>
static auto numeric_key(std::size_t key, char padding = '0') -> std::string
{
    const auto key_string = std::to_string(key);
    EXPECT_LE(key_string.size(), Length);
    return std::string(Length - key_string.size(), padding) + key_string;
}

} // namespace calicodb::test

#endif // CALICODB_TEST_TEST_H
