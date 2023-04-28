// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#include "calicodb/env.h"
#include "encoding.h"
#include "tools.h"
#include "unit_tests.h"
#include "utils.h"
#include <gtest/gtest.h>
#include <mutex>
#include <thread>

namespace calicodb
{

TEST(PathParserTests, ExtractsDirnames)
{
    // NOTE: Expects the POSIX version of dirname().
    ASSERT_EQ(split_path("dirname/basename").first, "dirname");
    ASSERT_EQ(split_path(".dirname/basename").first, ".dirname");
    ASSERT_EQ(split_path(".dirname.ext/basename").first, ".dirname.ext");
    ASSERT_EQ(split_path("/dirname/basename").first, "/dirname");
    ASSERT_EQ(split_path("/dirname/extra/basename").first, "/dirname/extra");
    ASSERT_EQ(split_path("/dirname/extra.ext/basename").first, "/dirname/extra.ext");
    ASSERT_EQ(split_path("/dirname///basename//").first, "/dirname");
    ASSERT_EQ(split_path("basename").first, ".");
    ASSERT_EQ(split_path("basename/").first, ".");
    ASSERT_EQ(split_path("/basename").first, "/");
    ASSERT_EQ(split_path("/basename/").first, "/"); // basename() strips trailing '/'.
    ASSERT_EQ(split_path("").first, ".");
    ASSERT_EQ(split_path("/").first, "/");
}

TEST(PathParserTests, ExtractsBasenames)
{
    ASSERT_EQ(split_path("dirname/basename").second, "basename");
    ASSERT_EQ(split_path("dirname/.basename").second, ".basename");
    ASSERT_EQ(split_path(".dirname/basename").second, "basename");
    ASSERT_EQ(split_path("/dirname/basename").second, "basename");
    ASSERT_EQ(split_path("/dirname/basename.ext").second, "basename.ext");
    ASSERT_EQ(split_path("/dirname/extra/basename").second, "basename");
    ASSERT_EQ(split_path("/dirname/extra.ext/basename").second, "basename");
    ASSERT_EQ(split_path("basename").second, "basename");
    ASSERT_EQ(split_path("basename/").second, "basename");
    ASSERT_EQ(split_path("/basename").second, "basename");
    ASSERT_EQ(split_path("/basename/").second, "basename");
    ASSERT_EQ(split_path("").second, ".");
    // basename == dirname in this case. We can still join the components to get a valid path.
    ASSERT_EQ(split_path("/").second, "/");
}

TEST(PathParserTests, JoinsComponents)
{
    ASSERT_EQ(join_paths("dirname", "basename"), "dirname/basename");
}

static auto make_filename(std::size_t n)
{
    return tools::integral_key<10>(n);
}
static auto write_out_randomly(tools::RandomGenerator &random, File &writer, const Slice &message) -> void
{
    constexpr std::size_t kChunks = 20;
    ASSERT_GT(message.size(), kChunks) << "File is too small for this test";
    Slice in(message);
    std::size_t counter = 0;

    while (!in.is_empty()) {
        const auto chunk_size = std::min<std::size_t>(in.size(), random.Next(message.size() / kChunks));
        auto chunk = in.range(0, chunk_size);

        ASSERT_TRUE(writer.write(counter, chunk).is_ok());
        counter += chunk_size;
        in.advance(chunk_size);
    }
    ASSERT_TRUE(in.is_empty());
}
[[nodiscard]] static auto read_back_randomly(tools::RandomGenerator &random, File &reader, std::size_t size) -> std::string
{
    static constexpr std::size_t kChunks = 20;
    EXPECT_GT(size, kChunks) << "File is too small for this test";
    std::string backing(size, '\x00');
    auto *out_data = backing.data();
    std::size_t counter = 0;

    while (counter < size) {
        const auto chunk_size = std::min<std::size_t>(size - counter, random.Next(size / kChunks));
        const auto s = reader.read_exact(counter, chunk_size, out_data);
        EXPECT_TRUE(s.is_ok()) << "Error: " << s.to_string().data();
        out_data += chunk_size;
        counter += chunk_size;
    }
    return backing;
}
struct EnvWithFiles final {
    explicit EnvWithFiles()
        : testdir(".")
    {
    }

    ~EnvWithFiles()
    {
        cleanup_files();
        delete env;
    }

    auto cleanup_files() -> void
    {
        for (auto *file : files) {
            file->shm_unmap(true);
            delete file;
        }
        files.clear();
    }

    enum NextFileName {
        kSameName,
        kDifferentName,
    };

    [[nodiscard]] auto open_file(std::size_t id, Env::OpenMode mode) const -> File *
    {
        File *file;
        EXPECT_OK(env->new_file(
            testdir.as_child(make_filename(id)),
            mode,
            file));
        return file;
    }

    auto open_unowned_file(NextFileName name, Env::OpenMode mode) -> File *
    {
        std::lock_guard lock(mutex);
        if (name == kDifferentName) {
            ++last_id;
        }
        const auto id = last_id;
        auto *file = open_file(id, mode);
        files.emplace_back(file);
        return file;
    }

    mutable std::mutex mutex;
    tools::TestDir testdir;
    std::vector<File *> files;
    Env *env = nullptr;
    std::size_t last_id = 0;
};

// Helper for testing shared memory
class SharedBuffer final
{
public:
    explicit SharedBuffer(File &file)
        : m_file(&file)
    {
    }

    auto read(std::size_t offset, std::size_t size) -> std::string
    {
        std::string out(size, '\0');
        auto *ptr = out.data();
        for (auto r = offset / File::kShmRegionSize; size; ++r) {
            volatile void *mem;
            EXPECT_OK(m_file->shm_map(r, mem));
            const volatile char *begin = reinterpret_cast<volatile char *>(mem);
            std::size_t copy_offset = 0;
            if (ptr == out.data()) {
                copy_offset = offset % File::kShmRegionSize;
            }
            auto copy_size = std::min(size, File::kShmRegionSize - copy_offset);
            std::memcpy(ptr, const_cast<const char *>(begin) + copy_offset, copy_size);
            ptr += copy_size;
            size -= copy_size;
        }
        return out;
    }

    auto write(std::size_t offset, const Slice &in) -> void
    {
        const auto r1 = offset / File::kShmRegionSize;
        Slice copy(in);
        for (auto r = r1; !copy.is_empty(); ++r) {
            volatile void *mem;
            EXPECT_OK(m_file->shm_map(r, mem));
            volatile char *begin = reinterpret_cast<volatile char *>(mem);
            std::size_t copy_offset = 0;
            if (r == r1) {
                copy_offset = offset % File::kShmRegionSize;
            }
            auto copy_size = std::min(copy.size(), File::kShmRegionSize - copy_offset);
            std::memcpy(const_cast<char *>(begin) + copy_offset, copy.data(), copy_size);
            copy.advance(copy_size);
        }
    }

private:
    File *m_file;
};

static constexpr std::size_t kFileVersionOffset = 1024;
static constexpr std::size_t kVersionLengthInU32 = 128;
static constexpr auto kVersionLength = kVersionLengthInU32 * sizeof(U32);

// REQUIRES: kShared or greater file_lock is held on "file"
static constexpr U32 kBadVersion = -1;
static auto read_file_version(File &file) -> U32
{
    std::string version_string(kVersionLength, '\0');
    EXPECT_OK(file.read_exact(
        kFileVersionOffset,
        kVersionLength,
        version_string.data()));
    const auto version = get_u32(version_string.data());
    for (std::size_t i = 1; i < kVersionLengthInU32; ++i) {
        EXPECT_EQ(version, get_u32(version_string.data() + sizeof(U32) * i));
    }
    return version;
}
// REQUIRES: kShared file_lock is held on byte "index" of "shm"
static auto read_shm_version(File &file, std::size_t index) -> U32
{
    SharedBuffer sh(file);

    // Read/write the version string in-between mapped regions.
    const auto offset = (index + 1) * File::kShmRegionSize - kVersionLength / 2;
    const auto version_string = sh.read(offset, kVersionLength);
    const auto version = get_u32(version_string.data());
    for (std::size_t i = 1; i < kVersionLengthInU32; ++i) {
        EXPECT_EQ(version, get_u32(version_string.data() + sizeof(U32) * i));
    }
    return version;
}
// REQUIRES: kExclusive file_lock is held on "file"
static auto write_file_version(File &file, U32 version) -> void
{
    std::string version_string(kVersionLength, '\0');
    for (std::size_t i = 0; i < kVersionLengthInU32; ++i) {
        put_u32(version_string.data() + sizeof(U32) * i, version);
    }
    EXPECT_OK(file.write(
        kFileVersionOffset,
        version_string));
}
// REQUIRES: kExclusive file_lock is held on byte "index" of "shm"
static auto write_shm_version(File &file, U32 version, std::size_t index) -> void
{
    std::string version_string(kVersionLength, '\0');
    for (std::size_t i = 0; i < kVersionLengthInU32; ++i) {
        put_u32(version_string.data() + sizeof(U32) * i, version);
    }
    SharedBuffer sh(file);
    const auto offset = (index + 1) * File::kShmRegionSize - kVersionLength / 2;
    sh.write(offset, version_string);
}
static auto sum_shm_versions(File &file) -> U32
{
    U32 total = 0;
    for (std::size_t i = 0; i < File::kShmLockCount; ++i) {
        total += read_shm_version(file, i);
    }
    return total;
}

static constexpr auto kFilename = "./__testfile";

class FileTests : public testing::TestWithParam<std::size_t>
{
public:
    const std::size_t kCount = GetParam();

    explicit FileTests()
    {
        m_env = Env::default_env();
        m_helper.env = m_env;
    }

    ~FileTests() override = default;

    auto test_same_inode() -> void
    {
        const auto message = m_random.Generate(1'024);
        auto *original = m_helper.open_unowned_file(EnvWithFiles::kDifferentName, Env::kCreate);
        write_out_randomly(m_random, *original, message);
        for (std::size_t i = 0; i < kCount; ++i) {
            auto *file = m_helper.open_unowned_file(EnvWithFiles::kSameName, Env::kReadOnly);
            ASSERT_EQ(message, read_back_randomly(m_random, *file, message.size()));
        }
    }

protected:
    tools::RandomGenerator m_random;
    EnvWithFiles m_helper;
    // Pointer to an object owned by m_helper.
    Env *m_env;
};

TEST_P(FileTests, OpenAndClose)
{
    for (std::size_t i = 0; i < 2; ++i) {
        auto *file = m_helper.open_unowned_file(
            EnvWithFiles::kSameName,
            Env::kCreate);
        for (std::size_t j = 0; j < 2; ++j) {
            ASSERT_OK(m_helper.env->new_file("shmfile", Env::kCreate, file));
            ASSERT_NE(file, nullptr);
            delete file;
        }
    }
}

TEST_P(FileTests, SameINode)
{
    test_same_inode();
}

INSTANTIATE_TEST_SUITE_P(
    FileTests,
    FileTests,
    ::testing::Values(1, 2, 5, 10, 100));

class EnvLockStateTests : public testing::TestWithParam<std::size_t>
{
public:
    const std::size_t kReplicates = GetParam();

    explicit EnvLockStateTests()
    {
        m_env = Env::default_env();
        m_helper.env = m_env;
    }

    ~EnvLockStateTests() override
    {
        (void)m_env->remove_file(kFilename);
    }

    auto new_file(const std::string &filename) -> File *
    {
        File *file;
        EXPECT_OK(m_env->new_file(
            filename,
            Env::kCreate,
            file));
        m_helper.files.emplace_back(file);
        return file;
    }

    auto test_sequence(bool reserve) -> void
    {
        auto *f = new_file(kFilename);
        ASSERT_OK(f->file_lock(kLockShared));
        ASSERT_OK(f->file_lock(kLockExclusive));
        f->file_unlock();
    }

    auto test_shared() -> void
    {
        auto *a = new_file(kFilename);
        auto *b = new_file(kFilename);
        auto *c = new_file(kFilename);
        ASSERT_OK(a->file_lock(kLockShared));
        ASSERT_OK(b->file_lock(kLockShared));
        ASSERT_OK(c->file_lock(kLockShared));
        c->file_unlock();
        b->file_unlock();
        a->file_unlock();
    }

    auto test_exclusive() -> void
    {
        auto *a = new_file(kFilename);
        auto *b = new_file(kFilename);

        ASSERT_OK(a->file_lock(kLockShared));
        ASSERT_OK(a->file_lock(kLockExclusive));

        // Try to take a shared file_lock on "b", but fail due to "a"'s exclusive
        // file_lock.
        ASSERT_TRUE(b->file_lock(kLockShared).is_busy());

        // Unlock "a" and let "b" get the exclusive file_lock.
        a->file_unlock();
        ASSERT_OK(b->file_lock(kLockShared));
        ASSERT_OK(b->file_lock(kLockExclusive));
        b->file_unlock();
    }

    template <class Test>
    auto run_test(const Test &test)
    {
        for (std::size_t i = 0; i < kReplicates; ++i) {
            test();
        }
    }

protected:
    EnvWithFiles m_helper;
    // Pointer to an object owned by m_helper.
    Env *m_env;
};

TEST_P(EnvLockStateTests, Sequence)
{
    run_test([this] { test_sequence(false); });
    run_test([this] { test_sequence(true); });
}

TEST_P(EnvLockStateTests, Shared)
{
    run_test([this] { test_shared(); });
}

TEST_P(EnvLockStateTests, Exclusive)
{
    run_test([this] { test_exclusive(); });
}

TEST_P(EnvLockStateTests, NOOPs)
{
    auto *f = new_file(kFilename);

    ASSERT_OK(f->file_lock(kLockShared));
    ASSERT_OK(f->file_lock(kLockShared));

    ASSERT_OK(f->file_lock(kLockShared));

    ASSERT_OK(f->file_lock(kLockExclusive));
    ASSERT_OK(f->file_lock(kLockExclusive));
    ASSERT_OK(f->file_lock(kLockShared));

    f->file_unlock();
    f->file_unlock();
}

#ifndef NDEBUG
TEST_P(EnvLockStateTests, InvalidRequestDeathTest)
{
    auto *f = new_file(kFilename);
    // kUnlocked -> kShared is the only allowed transition out of kUnlocked.
    ASSERT_DEATH((void)f->file_lock(kLockExclusive), kExpectationMatcher);
}
#endif // NDEBUG

INSTANTIATE_TEST_SUITE_P(
    EnvLockStateTests,
    EnvLockStateTests,
    ::testing::Values(1, 2, 5, 10, 100));

class EnvShmTests : public testing::Test
{
public:
    explicit EnvShmTests()
    {
        m_helper.env = Env::default_env();
    }

    ~EnvShmTests() override = default;

    auto get_same_file(bool is_readonly = false) -> File *
    {
        return m_helper.open_unowned_file(
            EnvWithFiles::kSameName,
            (is_readonly ? Env::kReadOnly : Env::kCreate));
    }

protected:
    EnvWithFiles m_helper;
};

TEST_F(EnvShmTests, MemoryIsShared)
{
    auto *file_a = m_helper.open_unowned_file(EnvWithFiles::kSameName, Env::kCreate);
    auto *file_b = m_helper.open_unowned_file(EnvWithFiles::kSameName, Env::kCreate);

    SharedBuffer a(*file_a);
    SharedBuffer b(*file_b);

    // Start of the shared mapping.
    a.write(0, "foo");
    ASSERT_EQ("foo", b.read(0, 3));

    // In-between the 1st and 2nd regions.
    b.write(File::kShmRegionSize - 1, "bar");
    ASSERT_EQ("bar", b.read(File::kShmRegionSize - 1, 3));
}

TEST_F(EnvShmTests, ShmIsTruncated)
{
    auto *shm = m_helper.open_file(EnvWithFiles::kSameName, Env::kCreate);
    {
        SharedBuffer sh(*shm);
        sh.write(0, "hello");
    }
    shm->shm_unmap(false);
    delete shm;
    shm = m_helper.open_file(EnvWithFiles::kSameName, Env::kCreate);

    SharedBuffer sh(*shm);
    ASSERT_EQ(sh.read(0, 5), std::string("\0\0\0\0\0", 5));
    shm->shm_unmap(true);
    delete shm;
}

TEST_F(EnvShmTests, LockCompatibility)
{
    auto *a = m_helper.open_file(EnvWithFiles::kSameName, Env::kCreate);
    auto *b = m_helper.open_file(EnvWithFiles::kSameName, Env::kCreate);
    auto *c = m_helper.open_file(EnvWithFiles::kSameName, Env::kCreate);

    // Shm must be created before locks can be taken.
    volatile void *ptr;
    ASSERT_OK(a->shm_map(0, ptr));
    ASSERT_OK(b->shm_map(0, ptr));
    ASSERT_OK(c->shm_map(0, ptr));

    // Shared locks can overlap, but they can only be 1 byte long.
    for (std::size_t i = 0; i < 8; ++i) {
        ASSERT_OK(a->shm_lock(i, 1, kShmLock | kShmReader));
        if (i < 4) {
            ASSERT_OK(b->shm_lock(i, 1, kShmLock | kShmReader));
        }
    }

    ASSERT_TRUE(c->shm_lock(0, 1, kShmLock | kShmWriter).is_busy());

    // Unlock half of "a"'s locked bytes.
    ASSERT_OK(a->shm_lock(0, 1, kShmUnlock | kShmReader));
    ASSERT_OK(a->shm_lock(1, 1, kShmUnlock | kShmReader));
    ASSERT_OK(a->shm_lock(2, 1, kShmUnlock | kShmReader));
    ASSERT_OK(a->shm_lock(3, 1, kShmUnlock | kShmReader));

    // "b" still has shared locks.
    ASSERT_TRUE(c->shm_lock(0, 1, kShmLock | kShmWriter).is_busy());

    ASSERT_OK(b->shm_lock(0, 1, kShmUnlock | kShmReader));
    ASSERT_OK(b->shm_lock(1, 1, kShmUnlock | kShmReader));
    ASSERT_OK(b->shm_lock(2, 1, kShmUnlock | kShmReader));
    ASSERT_OK(b->shm_lock(3, 1, kShmUnlock | kShmReader));

    ASSERT_TRUE(c->shm_lock(0, 5, kShmLock | kShmWriter).is_busy());
    ASSERT_OK(c->shm_lock(0, 4, kShmLock | kShmWriter));

    a->shm_unmap(true);
    b->shm_unmap(true);
    c->shm_unmap(true);
}

static auto busy_wait_file_lock(File &file, bool is_writer) -> void
{
    Status s;
    do {
        s = file.file_lock(kLockShared);
        if (s.is_ok()) {
            if (is_writer) {
                s = file.file_lock(kLockExclusive);
                if (s.is_ok()) {
                    return;
                }
                std::this_thread::yield();
                file.file_unlock();
            } else {
                return;
            }
        }
        std::this_thread::yield();
    } while (s.is_busy());
    ASSERT_OK(s);
}
static auto busy_wait_shm_lock(File &file, std::size_t r, std::size_t n, ShmLockFlag flags) -> void
{
    CALICODB_EXPECT_LE(r + n, File::kShmLockCount);
    for (;;) {
        const auto s = file.shm_lock(r, n, flags);
        if (s.is_ok()) {
            return;
        } else if (!s.is_busy()) {
            ADD_FAILURE() << s.to_string();
        }
        std::this_thread::yield();
    }
}
static auto file_reader_writer_test_routine(Env &env, File &file, bool is_writer) -> void
{
    Status s;
    if (is_writer) {
        busy_wait_file_lock(file, kLockExclusive);
        write_file_version(file, read_file_version(file) + 1);
        file.file_unlock();
    } else {
        busy_wait_file_lock(file, kLockShared);
        read_file_version(file); // Could be anything...
        file.file_unlock();
    }
}
static auto shm_lifetime_test_routine(Env &env, const std::string &filename, bool unlink) -> void
{
    File *file;
    ASSERT_OK(env.new_file(filename, Env::kCreate, file));

    Status s;
    volatile void *ptr;
    while (!(s = file->shm_map(0, ptr)).is_ok()) {
        // NOTE: This call may return either Status::busy() or Status::not_found() on failure.
        // Status::not_found() means someone else unlinked the shm before we could get the DMS
        // lock.
    }
    ASSERT_OK(s);
    file->shm_unmap(unlink);

    delete file;
}
static auto shm_reader_writer_test_routine(File &file, std::size_t r, std::size_t n, bool is_writer) -> void
{
    ASSERT_TRUE(is_writer || n == 1);
    const auto lock_flag = is_writer ? kShmWriter : kShmReader;
    busy_wait_shm_lock(file, r, n, kShmLock | lock_flag);

    for (std::size_t i = r; i < r + n; ++i) {
        const auto version = read_shm_version(file, i);
        if (is_writer) {
            write_shm_version(file, version + 1, i);
        }
    }
    ASSERT_OK(file.shm_lock(r, n, kShmUnlock | lock_flag));
}

// Env multithreading tests
//
// Each Env instance created in a given process communicates with the same global
// "inode info manager". This is to overcome some shortcomings of POSIX advisory
// locks. Examples include (a) closing a file descriptor to an inode with locks
// held on it can cause all locks to be dropped, and (b) POSIX locks don't work
// between threads in the same process.
//
// This test fixture uses multiple processes/threads to access a one or more Envs.
// The process is forked kNumEnvs times. The Env is not created until after the
// fork(), so there are kNumEnvs independent Envs, each managing its own inode list.
// Locking between processes must take place through the actual POSIX advisory locks.
// Locking between threads in the same process must be coordinated through the
// global inode list.
struct EnvConcurrencyTestsParam {
    std::size_t num_envs = 0;
    std::size_t num_threads = 0;
};
class EnvConcurrencyTests : public testing::TestWithParam<EnvConcurrencyTestsParam>
{
public:
    const std::size_t kNumEnvs = GetParam().num_envs;
    const std::size_t kNumThreads = GetParam().num_threads;
    static constexpr std::size_t kNumRounds = 500;

    ~EnvConcurrencyTests() override = default;

    auto SetUp() -> void override
    {
        // Create the file and zero out the version.
        auto *tempenv = Env::default_env();
        File *tempfile;
        ASSERT_OK(tempenv->new_file("./testdir/0000000000", Env::kCreate, tempfile));
        write_file_version(*tempfile, 0);
        delete tempfile;
        delete tempenv;
    }

    auto set_up() -> void
    {
        if (m_env == nullptr) {
            m_env = Env::default_env();
            m_helper.env = m_env;
        }

        ASSERT_GT(kNumEnvs, 0) << "REQUIRES: kNumEnvs > 0";
        m_helper.open_unowned_file(
            EnvWithFiles::kSameName,
            Env::kCreate);
    }

    template <class Test>
    auto run_test(const Test &test)
    {
        for (std::size_t n = 0; n < kNumEnvs; ++n) {
            const auto pid = fork();
            ASSERT_NE(-1, pid) << strerror(errno);
            if (pid) {
                continue;
            }

            test(n);
            std::exit(testing::Test::HasFailure());
        }
        for (std::size_t n = 0; n < kNumEnvs; ++n) {
            int s;
            const auto pid = wait(&s);
            ASSERT_NE(pid, -1)
                << "wait failed: " << strerror(errno);
            ASSERT_TRUE(WIFEXITED(s) && WEXITSTATUS(s) == 0)
                << "exited " << (WIFEXITED(s) ? "" : "ab")
                << "normally with exit status "
                << WEXITSTATUS(s);
        }
    }

    template <class IsWriter>
    auto run_reader_writer_test(std::size_t writers_per_thread, const IsWriter &is_writer) -> void
    {
        run_test([&is_writer, this](auto) {
            for (std::size_t i = 0; i < kNumThreads; ++i) {
                set_up();
            }
            std::vector<std::thread> threads;
            while (threads.size() < kNumThreads) {
                const auto t = threads.size();
                threads.emplace_back([&is_writer, t, this] {
                    for (std::size_t r = 0; r < kNumRounds; ++r) {
                        file_reader_writer_test_routine(*m_env, *m_helper.files[t], is_writer(r));
                    }
                });
            }
            for (auto &thread : threads) {
                thread.join();
            }
        });
        set_up();
        ASSERT_EQ(writers_per_thread * kNumThreads, read_file_version(*m_helper.files.front()));
    }

    auto run_shm_lifetime_test(bool unlink) -> void
    {
        run_test([this, unlink](auto) {
            for (std::size_t i = 0; i < kNumThreads; ++i) {
                set_up();
            }
            std::vector<std::thread> threads;
            while (threads.size() < kNumThreads) {
                const auto t = threads.size();
                threads.emplace_back([this, unlink] {
                    for (std::size_t r = 0; r < kNumRounds; ++r) {
                        shm_lifetime_test_routine(
                            *m_helper.env, m_helper.testdir.as_child(make_filename(0)), unlink);
                    }
                });
            }
            for (auto &thread : threads) {
                thread.join();
            }
        });
    }

    auto run_shm_reader_writer_test(std::size_t writer_n, std::size_t num_writers) -> void
    {
        std::vector<std::size_t> indices(kNumRounds);
        std::iota(begin(indices), end(indices), 0);
        std::default_random_engine rng(42);
        std::shuffle(begin(indices), end(indices), rng);
        indices.resize(num_writers);

        std::vector<bool> flags(kNumRounds);
        for (std::size_t i = 0; i < num_writers; ++i) {
            flags[indices[i]] = true;
        }

        auto *env = Env::default_env();
        File *file; // Keep this shm open to read from at the end...
        ASSERT_OK(env->new_file("./testdir/0000000000", Env::kCreate, file));
        volatile void *ptr;
        ASSERT_OK(file->shm_map(0, ptr));
        run_test([&](auto) {
            for (std::size_t i = 0; i < kNumThreads; ++i) {
                set_up();
                m_helper.open_unowned_file(EnvWithFiles::kSameName, Env::kCreate);
                volatile void *ptr;
                ASSERT_OK(m_helper.files[i]->shm_map(0, ptr));
            }
            std::vector<std::thread> threads;
            while (threads.size() < kNumThreads) {
                const auto t = threads.size();
                threads.emplace_back([t, this, &flags, writer_n] {
                    for (std::size_t r = 0; r < kNumRounds; ++r) {
                        shm_reader_writer_test_routine(
                            *m_helper.files[t],
                            r % (File::kShmLockCount - flags[r] * (writer_n - 1)),
                            flags[r] * (writer_n - 1) + 1, flags[r]);
                    }
                });
            }
            for (auto &thread : threads) {
                thread.join();
            }
        });

        ASSERT_EQ(num_writers * writer_n * kNumThreads * kNumEnvs, sum_shm_versions(*file));
        file->shm_unmap(true);
        delete file;
        // Get rid of the shm for the next round.
        m_helper.cleanup_files();
        delete env;
    }

protected:
    EnvWithFiles m_helper;
    Env *m_env = nullptr;
};
TEST_P(EnvConcurrencyTests, SingleWriter)
{
    run_reader_writer_test(kNumEnvs, [](auto r) {
        return r == kNumRounds / 2;
    });
}
TEST_P(EnvConcurrencyTests, MultipleWriters)
{
    run_reader_writer_test(kNumEnvs * kNumRounds / 2, [](auto r) {
        return r & 1;
    });
}
TEST_P(EnvConcurrencyTests, Contention)
{
    run_reader_writer_test(kNumEnvs * kNumRounds, [](auto) {
        return true;
    });
}
TEST_P(EnvConcurrencyTests, ShmLifetime1)
{
    run_shm_lifetime_test(false);
}
TEST_P(EnvConcurrencyTests, ShmLifetime2)
{
    run_shm_lifetime_test(true);
}
TEST_P(EnvConcurrencyTests, SingleShmWriter1)
{
    run_shm_reader_writer_test(1, 1);
    run_shm_reader_writer_test(1, 1);
}
TEST_P(EnvConcurrencyTests, SingleShmWriter2)
{
    run_shm_reader_writer_test(2, 1);
    run_shm_reader_writer_test(3, 1);
    run_shm_reader_writer_test(4, 1);
}
TEST_P(EnvConcurrencyTests, MultipleShmWriters)
{
    run_shm_reader_writer_test(1, 5);
    run_shm_reader_writer_test(2, 5);
    run_shm_reader_writer_test(3, 5);

    run_shm_reader_writer_test(1, 10);
    run_shm_reader_writer_test(2, 10);
    run_shm_reader_writer_test(3, 10);

    run_shm_reader_writer_test(1, 15);
    run_shm_reader_writer_test(2, 15);
    run_shm_reader_writer_test(3, 15);
}
INSTANTIATE_TEST_SUITE_P(
    EnvConcurrencyTests,
    EnvConcurrencyTests,
    ::testing::Values(
        // Sanity check: single thread/process
        EnvConcurrencyTestsParam{1, 1},

        // Multiple threads
        EnvConcurrencyTestsParam{1, 5},
        EnvConcurrencyTestsParam{1, 10},

        // Multiple processes
        EnvConcurrencyTestsParam{5, 1},
        EnvConcurrencyTestsParam{10, 1},

        // Multiple threads in multiple processes
        EnvConcurrencyTestsParam{2, 2},
        EnvConcurrencyTestsParam{2, 4},
        EnvConcurrencyTestsParam{4, 2}));

} // namespace calicodb