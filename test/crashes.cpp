// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#include "calicodb/db.h"
#include "calicodb/env.h"
#include "logging.h"
#include "test.h"

namespace calicodb::test
{

#define MAYBE_CRASH(target)                              \
    do {                                                 \
        if ((target)->should_next_syscall_fail()) {      \
            return Status::io_error("<injected_fault>"); \
        }                                                \
    } while (0)

class CrashEnv : public EnvWrapper
{
public:
    const bool m_fixed;

    mutable std::size_t m_max_num = 0;
    mutable std::size_t m_num = 0;

    explicit CrashEnv(Env &env, std::size_t max_num = 0)
        : EnvWrapper(env),
          m_fixed(max_num > 0),
          m_max_num(max_num)
    {
    }

    ~CrashEnv() override = default;

    [[nodiscard]] auto should_next_syscall_fail() const -> bool
    {
        if (m_num++ >= m_max_num) {
            m_max_num += !m_fixed;
            m_num = 0;
            return true;
        }
        return false;
    }

    auto new_file(const std::string &filename, OpenMode mode, File *&file_out) -> Status override
    {
        class CrashFile : public FileWrapper
        {
            CrashEnv *m_env;

        public:
            explicit CrashFile(CrashEnv &env, File &base)
                : FileWrapper(base),
                  m_env(&env)
            {
            }

            ~CrashFile() override
            {
                delete m_target;
            }

            [[nodiscard]] auto read(std::size_t offset, std::size_t size, char *scratch, Slice *out) -> Status override
            {
                MAYBE_CRASH(m_env);
                return FileWrapper::read(offset, size, scratch, out);
            }

            [[nodiscard]] auto write(std::size_t offset, const Slice &in) -> Status override
            {
                MAYBE_CRASH(m_env);
                return FileWrapper::write(offset, in);
            }

            [[nodiscard]] auto sync() -> Status override
            {
                MAYBE_CRASH(m_env);
                return FileWrapper::sync();
            }

            [[nodiscard]] auto file_lock(FileLockMode mode) -> Status override
            {
                MAYBE_CRASH(m_env);
                return FileWrapper::file_lock(mode);
            }

            [[nodiscard]] auto shm_map(std::size_t r, bool extend, volatile void *&out) -> Status override
            {
                MAYBE_CRASH(m_env);
                return FileWrapper::shm_map(r, extend, out);
            }

            [[nodiscard]] auto shm_lock(std::size_t r, std::size_t n, ShmLockFlag flags) -> Status override
            {
                if (flags & kShmLock) {
                    MAYBE_CRASH(m_env);
                }
                return FileWrapper::shm_lock(r, n, flags);
            }
        };

        auto s = target()->new_file(filename, mode, file_out);
        if (s.is_ok()) {
            file_out = new CrashFile(*this, *file_out);
        }
        return s;
    }
};

#undef MAYBE_CRASH

class TestCrashes : public testing::Test
{
protected:
    std::string m_filename;
    CrashEnv *m_env;

    explicit TestCrashes()
        : m_filename(testing::TempDir() + "crashes")
    {
    }

    ~TestCrashes() override = default;

    using Task = std::function<Status()>;
    static auto run_until_completion(const Task &task) -> void
    {
        Status s;
        do {
            s = task();
        } while (s.to_string() == "I/O error: <injected_fault>");
        ASSERT_OK(s) << "unexpected fault";
    }
};

TEST_F(TestCrashes, 0)
{
    run_test({1, 0, 0});
    run_test({0, 1, 0});
    run_test({0, 0, 1});
}

} // namespace calicodb::test