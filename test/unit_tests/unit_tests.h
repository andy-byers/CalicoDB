// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#ifndef CALICODB_TEST_UNIT_TESTS_H
#define CALICODB_TEST_UNIT_TESTS_H

#include "../test.h"
#include "calicodb/status.h"
#include "common.h"
#include "db_impl.h"
#include "encoding.h"
#include "env_posix.h"
#include "logging.h"
#include "page.h"
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

#define QUICK_INTERCEPTOR(filename__, type__)                                     \
    do {                                                                          \
        env().add_interceptor(filename__, Interceptor{(type__), [] {              \
                                                          return special_error(); \
                                                      }});                        \
    } while (0)

#define COUNTING_INTERCEPTOR(filename__, type__, n__)                                 \
    do {                                                                              \
        env().add_interceptor(filename__, Interceptor{(type__), [&n = (n__)] {        \
                                                          if (n-- <= 0) {             \
                                                              return special_error(); \
                                                          }                           \
                                                          return Status::ok();        \
                                                      }});                            \
    } while (0)

static constexpr auto kExpectationMatcher = "^expectation";

using SyscallType = U64;
static constexpr SyscallType kSyscallRead = 1;
static constexpr SyscallType kSyscallWrite = kSyscallRead << 1;
static constexpr SyscallType kSyscallOpen = kSyscallWrite << 1;
static constexpr SyscallType kSyscallSync = kSyscallOpen << 1;
static constexpr SyscallType kSyscallUnlink = kSyscallSync << 1;
static constexpr SyscallType kSyscallResize = kSyscallUnlink << 1;
static constexpr SyscallType kSyscallFileLock = kSyscallResize << 1;
static constexpr SyscallType kSyscallShmMap = kSyscallFileLock << 1;
static constexpr SyscallType kSyscallShmLock = kSyscallShmMap << 1;
static constexpr std::size_t kNumSyscalls = 9;

using Callback = std::function<Status()>;

struct Interceptor {
    explicit Interceptor(SyscallType t, Callback c)
        : callback(std::move(c)),
          type(t)
    {
    }

    [[nodiscard]] auto operator()() const -> Status
    {
        return callback();
    }

    Callback callback;
    SyscallType type;
};

struct FileCounters {
    std::size_t values[kNumSyscalls] = {};
};

class TestEnv : public EnvWrapper
{
public:
    explicit TestEnv();
    explicit TestEnv(Env &env);
    ~TestEnv() override;

    // NOTE: clone() always clones files into a FakeEnv, and only works properly if
    //       the wrapped Env was empty when passed to the constructor.
    [[nodiscard]] virtual auto clone() -> Env *;

    virtual auto copy_file(const std::string &source, const std::string &target) -> void;

    // The TestFile wrapper reads the whole file and saves it in memory after a
    // successful call to sync().
    virtual auto drop_after_last_sync(const std::string &filename) -> void;
    virtual auto drop_after_last_sync() -> void;

    virtual auto add_interceptor(const std::string &filename, Interceptor interceptor) -> void;
    virtual auto clear_interceptors() -> void;
    virtual auto clear_interceptors(const std::string &filename) -> void;

    [[nodiscard]] auto new_file(const std::string &filename, OpenMode mode, File *&out) -> Status override;
    [[nodiscard]] auto resize_file(const std::string &filename, std::size_t file_size) -> Status override;
    [[nodiscard]] auto remove_file(const std::string &filename) -> Status override;

    [[nodiscard]] auto find_counters(const std::string &filename) -> const FileCounters *;

private:
    friend class TestFile;

    struct FileState {
        std::list<Interceptor> interceptors;
        FileCounters counters;
        std::string saved_state;
        bool unlinked = false;
    };

    mutable std::unordered_map<std::string, FileState> m_state;
    mutable std::mutex m_mutex;

    [[nodiscard]] auto try_intercept_syscall(SyscallType type, const std::string &filename) -> Status;
    auto get_state(const std::string &filename) -> FileState &;
    auto save_file_contents(const std::string &filename) -> void;
    auto overwrite_file(const std::string &filename, const std::string &contents) -> void;
};

class TestFile : public File
{
    friend class TestEnv;

    std::string m_filename;
    TestEnv *m_env;
    TestEnv::FileState *m_state;
    File *m_target;

    explicit TestFile(std::string filename, File &file, TestEnv &env, TestEnv::FileState &state);

public:
    ~TestFile() override;
    [[nodiscard]] auto read(std::size_t offset, std::size_t size, char *scratch, Slice *out) -> Status override;
    [[nodiscard]] auto read_exact(std::size_t offset, std::size_t size, char *out) -> Status override;
    [[nodiscard]] auto write(std::size_t offset, const Slice &in) -> Status override;
    [[nodiscard]] auto sync() -> Status override;
    [[nodiscard]] auto file_lock(FileLockMode mode) -> Status override;
    [[nodiscard]] auto shm_map(std::size_t r, bool extend, volatile void *&ptr_out) -> Status override;
    [[nodiscard]] auto shm_lock(std::size_t r, std::size_t n, ShmLockFlag flag) -> Status override;
    auto file_unlock() -> void override
    {
        m_target->file_unlock();
    }
    auto shm_barrier() -> void override
    {
        m_target->shm_barrier();
    }
    auto shm_unmap(bool unlink) -> void override
    {
        m_target->shm_unmap(unlink);
    }
};

template <class EnvType>
class EnvTestHarness
{
public:
    explicit EnvTestHarness()
    {
        if constexpr (std::is_same_v<EnvType, PosixEnv>) {
            m_env = new TestEnv(*Env::default_env());
        } else if constexpr (!std::is_same_v<EnvType, TestEnv>) {
            m_env = new TestEnv(*new EnvType());
        } else {
            m_env = new TestEnv;
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

    [[nodiscard]] auto env() -> TestEnv &
    {
        return *m_env;
    }

    [[nodiscard]] auto env() const -> const TestEnv &
    {
        return *m_env;
    }

protected:
    TestEnv *m_env;
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
        write_string_to_file(Base::env(), kDBFilename, buffer);

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
            Options::kSyncNormal,
            Options::kLockNormal,
        };

        EXPECT_OK(Pager::open(pager_param, m_pager));
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
        EXPECT_OK(env.new_file(name, Env::kCreate | Env::kReadWrite, m_file));
        EXPECT_OK(m_file->shm_map(0, true, ptr));
        EXPECT_TRUE(ptr);
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

auto read_file_to_string(Env &env, const std::string &filename) -> std::string;
auto write_string_to_file(Env &env, const std::string &filename, const std::string &buffer, long offset = -1) -> void;
auto assign_file_contents(Env &env, const std::string &filename, const std::string &contents) -> void;
auto fill_db(DB &db, const std::string &bname, RandomGenerator &random, std::size_t num_records, std::size_t max_payload_size = 100) -> std::map<std::string, std::string>;
auto fill_db(Tx &tx, const std::string &bname, RandomGenerator &random, std::size_t num_records, std::size_t max_payload_size = 100) -> std::map<std::string, std::string>;
auto fill_db(Tx &tx, const Bucket &b, RandomGenerator &random, std::size_t num_records, std::size_t max_payload_size = 100) -> std::map<std::string, std::string>;
auto expect_db_contains(DB &db, const std::string &bname, const std::map<std::string, std::string> &map) -> void;
auto expect_db_contains(const Tx &tx, const std::string &bname, const std::map<std::string, std::string> &map) -> void;
auto expect_db_contains(const Tx &tx, const Bucket &b, const std::map<std::string, std::string> &map) -> void;

template <class Fn>
[[nodiscard]] inline auto view_db(const std::string &filename, Fn &fn, const Options &options = {}) -> calicodb::Status
{
    calicodb::DB *db;
    auto s = calicodb::DB::open(options, filename, db);
    if (s.is_ok()) {
        s = db->view(fn);
        delete db;
    }
    return s;
}

template <class Fn>
[[nodiscard]] inline auto update_db(const std::string &filename, Fn &fn, const Options &options = {}) -> calicodb::Status
{
    calicodb::DB *db;
    auto s = calicodb::DB::open(options, filename, db);
    if (s.is_ok()) {
        do {
            s = db->update(fn);
        } while (s.is_busy());
        delete db;
    }
    return s;
}

class BusyCounter : public BusyHandler
{
public:
    ~BusyCounter() override = default;

    std::atomic<unsigned> count = 0;
    auto exec(unsigned) -> bool override
    {
        count.fetch_add(1);
        return true;
    }
};

template <std::size_t Length = 16>
class NumericKey
{
    std::string m_value;

public:
    explicit NumericKey()
        : m_value("0")
    {
    }

    explicit NumericKey(U64 number)
        : m_value(numeric_key<Length>(number))
    {
    }

    explicit NumericKey(std::string string)
        : m_value(std::move(string))
    {
        if (m_value.empty()) {
            m_value = "0";
        }
        // Make sure the string is a valid number.
        (void)number();
    }

    auto operator==(const NumericKey &rhs) const -> bool
    {
        return m_value == rhs.m_value;
    }

    auto operator!=(const NumericKey &rhs) const -> bool
    {
        return !(*this == rhs);
    }

    [[nodiscard]] auto number() const -> U64
    {
        U64 value;
        Slice slice(m_value);
        consume_decimal_number(slice, &value);
        return value;
    }

    [[nodiscard]] auto string() const -> const std::string &
    {
        return m_value;
    }

    auto operator++() -> NumericKey &
    {
        m_value = numeric_key<Length>(number() + 1);
        return *this;
    }

    auto operator--() -> NumericKey &
    {
        CALICODB_EXPECT_GT(number(), 0);
        m_value = numeric_key<Length>(number() - 1);
        return *this;
    }

    auto operator++(int) -> NumericKey
    {
        auto prev = *this;
        ++*this;
        return prev;
    }

    auto operator--(int) -> NumericKey
    {
        auto prev = *this;
        --*this;
        return prev;
    }
};

class WalStub : public Wal
{
public:
    ~WalStub() override = default;

    [[nodiscard]] auto read(Id, char *&) -> Status override
    {
        return Status::not_found("");
    }

    [[nodiscard]] auto write(PageRef *, std::size_t) -> Status override
    {
        return Status::ok();
    }

    [[nodiscard]] auto checkpoint(bool) -> Status override
    {
        return Status::ok();
    }

    auto rollback(const Undo &) -> void override
    {
    }

    [[nodiscard]] auto close() -> Status override
    {
        return Status::ok();
    }

    [[nodiscard]] auto stats() const -> const Wal::Stats & override
    {
        static constexpr Wal::Stats kEmpty;
        return kEmpty;
    }
};

class FakeWal : public Wal
{
    std::map<Id, std::string> m_committed;
    std::map<Id, std::string> m_pending;
    std::size_t m_db_size = 0;
    File *m_db_file;

public:
    explicit FakeWal(const Parameters &param);
    ~FakeWal() override = default;

    [[nodiscard]] auto read(Id page_id, char *&out) -> Status override;
    [[nodiscard]] auto write(PageRef *dirty, std::size_t db_size) -> Status override;
    [[nodiscard]] auto checkpoint(bool) -> Status override;
    [[nodiscard]] auto close() -> Status override;
    [[nodiscard]] auto start_reader(bool &) -> Status override { return Status::ok(); }
    [[nodiscard]] auto start_writer() -> Status override { return Status::ok(); }
    auto finish_reader() -> void override {}
    auto finish_writer() -> void override {}
    auto rollback(const Undo &undo) -> void override;

    [[nodiscard]] auto stats() const -> const Wal::Stats & override
    {
        static constexpr Wal::Stats kEmpty;
        return kEmpty;
    }
};

} // namespace calicodb

#endif // CALICODB_TEST_UNIT_TESTS_H
