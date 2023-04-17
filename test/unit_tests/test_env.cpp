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
    ~EnvWithFiles()
    {
        for (std::size_t i = 0; i < files.size(); ++i) {
            (void)env->remove_file(make_filename(i));
            delete files[i];
        }
        delete env;
    }

    [[nodiscard]] auto open_file(std::size_t id, Env::OpenMode mode) const -> File *
    {
        File *file;
        EXPECT_OK(env->new_file(
            make_filename(id),
            mode,
            file));
        return file;
    }

    enum NextFileName {
        kSameName,
        kDifferentName,
    };

    auto open_unowned_file(NextFileName name, Env::OpenMode mode) -> File *
    {
        if (name == kDifferentName) {
            ++m_last_id;
        }
        const auto id = m_last_id;
        auto *file = open_file(id, mode);
        files.emplace_back(file);
        return file;
    }

    std::vector<File *> files;
    Env *env = nullptr;

private:
    std::size_t m_last_id = 0;
};

static constexpr std::size_t kVersionOffset = 1024;
static constexpr std::size_t kVersionLengthInU32 = 128;
static constexpr auto kVersionLength = kVersionLengthInU32 * sizeof(U32);

// REQUIRES: kShared or greater lock is held on "file"
static auto read_version(File &file) -> U32
{
    std::string version_string(kVersionLength, '\0');
    EXPECT_OK(file.read_exact(
        kVersionOffset,
        kVersionLength,
        version_string.data()));
    const auto version = get_u32(version_string.data());
    for (std::size_t i = 1; i < kVersionLengthInU32; ++i) {
        EXPECT_EQ(version, get_u32(version_string.data() + sizeof(U32) * i));
    }
    return version;
}

// REQUIRES: kExclusive lock is held on "file"
static auto write_version(File &file, U32 version) -> void
{
    std::string version_string(kVersionLength, '\0');
    for (std::size_t i = 0; i < kVersionLengthInU32; ++i) {
        put_u32(version_string.data() + sizeof(U32) * i, version);
    }
    EXPECT_OK(file.write(
        kVersionOffset,
        version_string));
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
        auto *original = m_helper.open_unowned_file(EnvWithFiles::kDifferentName, Env::kCreate | Env::kReadWrite);
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
            Env::kCreate | Env::kReadWrite,
            file));
        m_helper.files.emplace_back(file);
        return file;
    }

    auto test_sequence(bool reserve) -> void
    {
        auto *f = new_file(kFilename);
        ASSERT_OK(m_env->set_lock(*f, Env::kShared));
        ASSERT_EQ(m_env->get_lock(*f), Env::kShared);
        if (reserve) {
            ASSERT_OK(m_env->set_lock(*f, Env::kReserved));
            ASSERT_EQ(m_env->get_lock(*f), Env::kReserved);
        }
        ASSERT_OK(m_env->set_lock(*f, Env::kExclusive));
        ASSERT_EQ(m_env->get_lock(*f), Env::kExclusive);
        ASSERT_OK(m_env->unlock(*f, Env::kShared));
        ASSERT_EQ(m_env->get_lock(*f), Env::kShared);
        ASSERT_OK(m_env->unlock(*f, Env::kUnlocked));
        ASSERT_EQ(m_env->get_lock(*f), Env::kUnlocked);
    }

    auto test_shared() -> void
    {
        auto *a = new_file(kFilename);
        auto *b = new_file(kFilename);
        auto *c = new_file(kFilename);
        ASSERT_OK(m_env->set_lock(*a, Env::kShared));
        ASSERT_OK(m_env->set_lock(*b, Env::kShared));
        ASSERT_OK(m_env->set_lock(*c, Env::kShared));
        ASSERT_OK(m_env->unlock(*c, Env::kUnlocked));
        ASSERT_OK(m_env->unlock(*b, Env::kUnlocked));
        ASSERT_OK(m_env->unlock(*a, Env::kUnlocked));
    }

    auto test_exclusive() -> void
    {
        auto *a = new_file(kFilename);
        auto *b = new_file(kFilename);

        ASSERT_OK(m_env->set_lock(*a, Env::kShared));
        ASSERT_OK(m_env->set_lock(*a, Env::kExclusive));

        // Try to take a shared lock on "b", but fail due to "a"'s exclusive
        // lock.
        ASSERT_TRUE(m_env->set_lock(*b, Env::kShared).is_busy());

        // Unlock "a" and let "b" get the exclusive lock.
        ASSERT_OK(m_env->unlock(*a, Env::kUnlocked));
        ASSERT_OK(m_env->set_lock(*b, Env::kShared));
        ASSERT_OK(m_env->set_lock(*b, Env::kExclusive));
        ASSERT_OK(m_env->unlock(*b, Env::kUnlocked));
    }

    auto test_reserved(bool shared) -> void
    {
        auto *a = new_file(kFilename);
        auto *b = new_file(kFilename);
        auto *c = new_file(kFilename);

        if (shared) {
            ASSERT_OK(m_env->set_lock(*a, Env::kShared));
            ASSERT_OK(m_env->set_lock(*b, Env::kShared));
            ASSERT_OK(m_env->set_lock(*c, Env::kShared));
        }

        // Take a reserved lock on 1 of the files and make sure that the
        // other file descriptors cannot be locked in a mode greater than
        // kShared.
        File *files[] = {a, b, c};
        for (std::size_t i = 0; i < 3; ++i) {
            auto *p = files[i];
            auto *x = files[(i + 1) % 3];
            auto *y = files[(i + 2) % 3];

            ASSERT_OK(m_env->set_lock(*p, Env::kShared));
            ASSERT_OK(m_env->set_lock(*p, Env::kReserved));

            ASSERT_OK(m_env->set_lock(*x, Env::kShared));
            ASSERT_TRUE(m_env->set_lock(*x, Env::kReserved).is_busy());
            ASSERT_TRUE(m_env->set_lock(*x, Env::kExclusive).is_busy());

            ASSERT_OK(m_env->set_lock(*y, Env::kShared));
            ASSERT_TRUE(m_env->set_lock(*y, Env::kReserved).is_busy());
            ASSERT_TRUE(m_env->set_lock(*y, Env::kExclusive).is_busy());

            ASSERT_OK(m_env->unlock(*p, shared ? Env::kShared : Env::kUnlocked));
            ASSERT_OK(m_env->unlock(*x, shared ? Env::kShared : Env::kUnlocked));
            ASSERT_OK(m_env->unlock(*y, shared ? Env::kShared : Env::kUnlocked));
        }
    }

    auto test_pending(bool reserved) -> void
    {
        auto *a = new_file(kFilename);
        auto *b = new_file(kFilename);
        auto *c = new_file(kFilename);
        auto *extra = new_file(kFilename);

        // Used to prevent "p" below from getting an exclusive lock.
        ASSERT_OK(m_env->set_lock(*extra, Env::kShared));

        // Fail to take an exclusive lock on 1 of the files, leaving it in
        // pending mode, and make sure that the other file descriptors cannot
        // be locked.
        File *files[] = {a, b, c};
        for (std::size_t i = 0; i < 3; ++i) {
            auto *p = files[i];
            auto *x = files[(i + 1) % 3];
            auto *y = files[(i + 2) % 3];

            ASSERT_OK(m_env->set_lock(*p, Env::kShared));
            if (reserved) {
                ASSERT_OK(m_env->set_lock(*p, Env::kReserved));
            }

            ASSERT_TRUE(m_env->set_lock(*p, Env::kExclusive).is_busy());

            if (reserved) {
                ASSERT_EQ(m_env->get_lock(*p), Env::kPending);
                ASSERT_TRUE(m_env->set_lock(*x, Env::kShared).is_busy());
                ASSERT_TRUE(m_env->set_lock(*y, Env::kShared).is_busy());
            } else {
                ASSERT_EQ(m_env->get_lock(*p), Env::kShared);
                ASSERT_OK(m_env->set_lock(*x, Env::kShared));
                ASSERT_OK(m_env->set_lock(*y, Env::kShared));
            }

            ASSERT_OK(m_env->unlock(*p, Env::kUnlocked));
            ASSERT_OK(m_env->unlock(*x, Env::kUnlocked));
            ASSERT_OK(m_env->unlock(*y, Env::kUnlocked));
        }
    }

    template<class Test>
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
    run_test([this] {test_sequence(false);});
    run_test([this] {test_sequence(true);});
}

TEST_P(EnvLockStateTests, Shared)
{
    run_test([this] {test_shared();});
}

TEST_P(EnvLockStateTests, Exclusive)
{
    run_test([this] {test_exclusive();});
}

TEST_P(EnvLockStateTests, Reserved)
{
    run_test([this] {test_reserved(false);});
    run_test([this] {test_reserved(true);});
}

TEST_P(EnvLockStateTests, Pending)
{
    run_test([this] {test_pending(false);});
    run_test([this] {test_pending(true);});
}

TEST_P(EnvLockStateTests, NOOPs)
{
    auto *f = new_file(kFilename);

    ASSERT_OK(m_env->set_lock(*f, Env::kShared));
    ASSERT_OK(m_env->set_lock(*f, Env::kShared));
    ASSERT_OK(m_env->set_lock(*f, Env::kUnlocked));
    ASSERT_EQ(m_env->get_lock(*f), Env::kShared);

    ASSERT_OK(m_env->set_lock(*f, Env::kReserved));
    ASSERT_OK(m_env->set_lock(*f, Env::kReserved));
    ASSERT_OK(m_env->set_lock(*f, Env::kShared));
    ASSERT_OK(m_env->set_lock(*f, Env::kUnlocked));
    ASSERT_EQ(m_env->get_lock(*f), Env::kReserved);

    ASSERT_OK(m_env->set_lock(*f, Env::kExclusive));
    ASSERT_OK(m_env->set_lock(*f, Env::kExclusive));
    ASSERT_OK(m_env->set_lock(*f, Env::kReserved));
    ASSERT_OK(m_env->set_lock(*f, Env::kShared));
    ASSERT_OK(m_env->set_lock(*f, Env::kUnlocked));
    ASSERT_EQ(m_env->get_lock(*f), Env::kExclusive);

    ASSERT_OK(m_env->unlock(*f, Env::kShared));
    ASSERT_OK(m_env->unlock(*f, Env::kShared));
    ASSERT_EQ(m_env->get_lock(*f), Env::kShared);
    ASSERT_OK(m_env->unlock(*f, Env::kUnlocked));
    ASSERT_OK(m_env->unlock(*f, Env::kUnlocked));
    ASSERT_EQ(m_env->get_lock(*f), Env::kUnlocked);
    ASSERT_OK(m_env->unlock(*f, Env::kShared));
}

#ifndef NDEBUG
TEST_P(EnvLockStateTests, InvalidRequestDeathTest)
{
    auto *f = new_file(kFilename);
    // kPending cannot be requested directly.
    ASSERT_DEATH((void)m_env->set_lock(*f, Env::kPending), kExpectationMatcher);
    // kUnlocked -> kShared is the only allowed transition out of kUnlocked.
    ASSERT_DEATH((void)m_env->set_lock(*f, Env::kReserved), kExpectationMatcher);
    ASSERT_DEATH((void)m_env->set_lock(*f, Env::kExclusive), kExpectationMatcher);
    // unlock() can only be called with kShared or kUnlocked.
    ASSERT_DEATH((void)m_env->unlock(*f, Env::kReserved), kExpectationMatcher);
    ASSERT_DEATH((void)m_env->unlock(*f, Env::kPending), kExpectationMatcher);
    ASSERT_DEATH((void)m_env->unlock(*f, Env::kExclusive), kExpectationMatcher);
}
#endif // NDEBUG

INSTANTIATE_TEST_SUITE_P(
    EnvLockStateTests,
    EnvLockStateTests,
    ::testing::Values(1, 2, 5, 10, 100));

static auto busy_wait_lock(Env &env, File &file, bool is_writer) -> void
{
    for (auto m = Env::kShared; m <= (is_writer ? Env::kExclusive : Env::kShared); ) {
        if (m == Env::kPending) {
            m = Env::kExclusive;
            continue;
        }
        auto s = env.set_lock(file, m);
        if (s.is_ok()) {
            m = Env::LockMode{m + 1};
            continue;
        } else if (!s.is_busy()) {
            ADD_FAILURE() << s.to_string();
        } else {
            // Give up and let some other thread/process try to get an exclusive lock.
            ASSERT_OK(env.unlock(file, Env::kUnlocked));
            m = Env::kShared;
        }
        std::this_thread::yield();
    }
}
static auto reader_writer_test_routine(Env &env, File &file, bool is_writer) -> void
{
    Status s;
    if (is_writer) {
        busy_wait_lock(env, file, Env::kExclusive);
        write_version(file, read_version(file) + 1);
        ASSERT_OK(env.unlock(file, Env::kUnlocked));
    } else {
        busy_wait_lock(env, file, Env::kShared);
        read_version(file); // Could be anything...
        ASSERT_OK(env.unlock(file, Env::kUnlocked));
    }
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
struct MultiEnvMultiProcessTestsParam {
    std::size_t num_envs = 0;
    std::size_t num_threads = 0;
};
class MultiEnvMultiProcessTests : public testing::TestWithParam<MultiEnvMultiProcessTestsParam> {
public:
    const std::size_t kNumEnvs = GetParam().num_envs;
    const std::size_t kNumThreads = GetParam().num_threads;
    static constexpr std::size_t kNumRounds = 500;

    ~MultiEnvMultiProcessTests() override = default;

    auto SetUp() -> void override
    {
        // Create the file and zero out the version.
        auto *tempenv = Env::default_env();
        File *tempfile;
        ASSERT_OK(tempenv->new_file(make_filename(0), Env::kCreate | Env::kReadWrite, tempfile));
        write_version(*tempfile, 0);
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
            Env::kCreate | Env::kReadWrite);
    }

    template<class Test>
    auto run_test(const Test &test)
    {
        for (std::size_t n = 0; n < kNumEnvs; ++n) {
            if (fork()) {
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

    template<class IsWriter>
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
                        reader_writer_test_routine(*m_env, *m_helper.files[t], is_writer(r));
                    }
                });
            }
            for (auto &thread : threads) {
                thread.join();
            }
        });
        set_up();
        ASSERT_EQ(writers_per_thread * kNumThreads, read_version(*m_helper.files.front()));
    }

protected:
    EnvWithFiles m_helper;
    Env *m_env = nullptr;
};
TEST_P(MultiEnvMultiProcessTests, SingleWriter)
{
    run_reader_writer_test(kNumEnvs, [](auto r) {
        return r == kNumRounds / 2;
    });
}
TEST_P(MultiEnvMultiProcessTests, MultipleWriters)
{
    run_reader_writer_test(kNumEnvs * kNumRounds / 2, [](auto r) {
        return r & 1;
    });
}
TEST_P(MultiEnvMultiProcessTests, Contention)
{
    run_reader_writer_test(kNumEnvs * kNumRounds, [](auto) {
        return true;
    });
}
INSTANTIATE_TEST_SUITE_P(
    MultiEnvMultiProcessTests,
    MultiEnvMultiProcessTests,
    ::testing::Values(
        MultiEnvMultiProcessTestsParam {1, 1},
        MultiEnvMultiProcessTestsParam {1, 5},
        MultiEnvMultiProcessTestsParam {5, 5},
        MultiEnvMultiProcessTestsParam {5, 5}));

struct MultiEnvSingleProcessTestsParam {
    std::size_t num_threads = 0;
};
class MultiEnvSingleProcessTests : public testing::TestWithParam<MultiEnvSingleProcessTestsParam>
{
protected:
    const std::size_t kNumThreads = GetParam().num_threads;
    static constexpr std::size_t kNumRounds = 500;

    ~MultiEnvSingleProcessTests() override = default;

    auto SetUp() -> void override
    {
        m_helpers.resize(kNumThreads);
        for (auto &h : m_helpers) {
            h.env = Env::default_env();
            h.open_unowned_file(
                EnvWithFiles::kDifferentName,
                Env::kCreate | Env::kReadWrite);
        }
        auto *file = m_helpers.front().files.front();
        write_version(*file, 0);
    }
    
    template<class IsWriter>
    auto run_reader_writer_test(std::size_t writers_per_thread, const IsWriter &is_writer) -> void 
    {
        for (std::size_t i = 0; i < kNumThreads; ++i) {
            auto &env = *m_helpers[i].env;
            auto &file = *m_helpers[i].files.front();
            m_threads.emplace_back([&env, &file, &is_writer] {
                for (std::size_t r = 0; r < kNumRounds; ++r) {
                    reader_writer_test_routine(env, file, is_writer(r));
                }
            });
        }
        for (auto &thread : m_threads) {
            thread.join();
        }
        auto *file = m_helpers.front().files.front();
        ASSERT_EQ(writers_per_thread * kNumThreads, read_version(*file));
    }

    mutable std::mutex m_mutex;
    std::vector<std::thread> m_threads;
    std::vector<EnvWithFiles> m_helpers;
};
TEST_P(MultiEnvSingleProcessTests, SingleWriter)
{
    run_reader_writer_test(1, [](auto r) {
        return r == kNumRounds / 2;
    });
}
TEST_P(MultiEnvSingleProcessTests, MultipleWriters)
{
    run_reader_writer_test(kNumRounds / 2, [](auto r) {
        return r & 1;
    });
}
TEST_P(MultiEnvSingleProcessTests, Contention)
{
    run_reader_writer_test(kNumRounds, [](auto) {
        return true;
    });
}
INSTANTIATE_TEST_SUITE_P(
    MultiEnvSingleProcessTests,
    MultiEnvSingleProcessTests,
    ::testing::Values(
        MultiEnvSingleProcessTestsParam {1},
        MultiEnvSingleProcessTestsParam {2},
        MultiEnvSingleProcessTestsParam {3},
        MultiEnvSingleProcessTestsParam {4},
        MultiEnvSingleProcessTestsParam {5},
        MultiEnvSingleProcessTestsParam {10},
        MultiEnvSingleProcessTestsParam {15}));

} // namespace calicodb