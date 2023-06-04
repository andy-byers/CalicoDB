// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#ifndef CALICODB_TEST_UNIT_TESTS_H
#define CALICODB_TEST_UNIT_TESTS_H

#include "../test.h"
#include "calicodb/status.h"
#include "db_impl.h"
#include "encoding.h"
#include "env_helpers.h"
#include "env_posix.h"
#include "harness.h"
#include "page.h"
#include "tools.h"
#include "wal.h"
#include <atomic>
#include <filesystem>
#include <gtest/gtest.h>
#include <iomanip>
#include <sstream>
#include <thread>

namespace calicodb
{

static constexpr auto kDBFilename = "./_test-db";
static constexpr auto kWalFilename = "./_test-wal";
static constexpr auto kShmFilename = "./_test-shm";

#define CLEAR_INTERCEPTORS()        \
    do {                            \
        env().clear_interceptors(); \
    } while (0)

#define QUICK_INTERCEPTOR(filename__, type__)                                            \
    do {                                                                                 \
        env().add_interceptor(filename__, tools::Interceptor{(type__), [] {              \
                                                                 return special_error(); \
                                                             }});                        \
    } while (0)

#define COUNTING_INTERCEPTOR(filename__, type__, n__)                                        \
    do {                                                                                     \
        env().add_interceptor(filename__, tools::Interceptor{(type__), [&n = (n__)] {        \
                                                                 if (n-- <= 0) {             \
                                                                     return special_error(); \
                                                                 }                           \
                                                                 return Status::ok();        \
                                                             }});                            \
    } while (0)

static constexpr auto kExpectationMatcher = "^expectation";

template <class EnvType>
class EnvTestHarness
{
public:
    explicit EnvTestHarness()
    {
        if constexpr (std::is_same_v<EnvType, PosixEnv>) {
            m_env = new tools::TestEnv(*Env::default_env());
        } else if constexpr (!std::is_same_v<EnvType, tools::TestEnv>) {
            m_env = new tools::TestEnv(*new EnvType());
        } else {
            m_env = new tools::TestEnv;
        }
        (void)m_env->remove_file(kDBFilename);
        (void)m_env->remove_file(kWalFilename);
        (void)m_env->remove_file(kShmFilename);
    }

    virtual ~EnvTestHarness()
    {
        (void)m_env->remove_file(kDBFilename);
        (void)m_env->remove_file(kWalFilename);
        (void)m_env->remove_file(kShmFilename);
        delete m_env;
    }

    [[nodiscard]] auto env() -> tools::TestEnv &
    {
        return *m_env;
    }

    [[nodiscard]] auto env() const -> const tools::TestEnv &
    {
        return *m_env;
    }

protected:
    tools::TestEnv *m_env;
};

template <class EnvType>
class PagerTestHarness : public EnvTestHarness<EnvType>
{
public:
    using Base = EnvTestHarness<EnvType>;
    static constexpr auto kFrameCount = kMinFrameCount;

    PagerTestHarness()
    {
        std::string buffer(kPageSize, '\0');
        std::memcpy(buffer.data(), FileHeader::kFmtString, sizeof(FileHeader::kFmtString));
        buffer[FileHeader::kFmtVersionOffset] = FileHeader::kFmtVersion;
        put_u32(buffer.data() + FileHeader::kPageCountOffset, 1);
        tools::write_string_to_file(Base::env(), kDBFilename, buffer);

        File *file;
        EXPECT_OK(Base::env().new_file(kDBFilename, Env::kCreate, file));

        const Pager::Parameters pager_param = {
            kDBFilename,
            kWalFilename,
            file,
            &Base::env(),
            nullptr,
            &m_status,
            nullptr,
            kFrameCount,
        };

        CHECK_OK(Pager::open(pager_param, m_pager));
        m_pager->set_page_count(1);
    }

    ~PagerTestHarness() override
    {
        (void)m_pager->close();
        delete m_pager;
        m_pager = nullptr;
    }

protected:
    Status m_status;
    Pager *m_pager = nullptr;
};

class SharedCount
{
    volatile U32 *m_ptr = nullptr;
    File *m_file = nullptr;

public:
    explicit SharedCount(Env &env, const std::string &name)
    {
        volatile void *ptr;
        CHECK_OK(env.new_file(name, Env::kCreate | Env::kReadWrite, m_file));
        CHECK_OK(m_file->shm_map(0, true, ptr));
        CHECK_TRUE(ptr);
        m_ptr = reinterpret_cast<volatile U32 *>(ptr);
    }

    ~SharedCount()
    {
        m_file->shm_unmap(true);
        delete m_file;
    }

    enum MemoryOrder : int {
        kRelaxed = __ATOMIC_RELAXED,
        kAcquire = __ATOMIC_ACQUIRE,
        kRelease = __ATOMIC_RELEASE,
        kAcqRel = __ATOMIC_ACQ_REL,
        kSeqCst = __ATOMIC_SEQ_CST,
    };
    auto load(MemoryOrder order = kAcquire) const -> U32
    {
        return __atomic_load_n(m_ptr, order);
    }
    auto store(U32 value, MemoryOrder order = kRelease) -> void
    {
        __atomic_store_n(m_ptr, value, order);
    }
    auto increase(U32 n, MemoryOrder order = kRelaxed) -> U32
    {
        return __atomic_add_fetch(m_ptr, n, order);
    }
};

[[nodiscard]] inline auto special_error()
{
    return Status::io_error("42");
}

inline auto assert_special_error(const Status &s)
{
    if (!s.is_io_error() || s.to_string() != special_error().to_string()) {
        std::fprintf(stderr, "error expected special error: %s", s.is_ok() ? "OK" : s.to_string().data());
        std::abort();
    }
}

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

} // namespace calicodb

#endif // CALICODB_TEST_UNIT_TESTS_H
