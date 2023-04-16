// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#include "calicodb/env.h"
#include "encoding.h"
#include "tools.h"
#include "unit_tests.h"
#include "utils.h"
#include <filesystem>
#include <fstream>
#include <gtest/gtest.h>
#include <mutex>
#include <thread>

#include <sys/fcntl.h>
#include <sys/types.h>

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

struct EnvWithFiles final {
    ~EnvWithFiles()
    {
        for (const auto *file : files) {
            delete file;
        }
        delete env;
    }

    std::vector<File *> files;
    Env *env = nullptr;
};

// Helpers for testing files and locking.
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
static auto make_filename(std::size_t n)
{
    return tools::integral_key<10>(n);
}
class WorkDelegator
{
public:
    explicit WorkDelegator(std::mutex &mutex, std::size_t n)
        : m_rng(42),
          m_mutex(&mutex),
          m_indices(n)
    {
        std::iota(begin(m_indices), end(m_indices), 0);
        m_idx = m_indices.size();
    }

    auto operator()() -> std::size_t
    {
        std::lock_guard lock(*m_mutex);
        if (m_idx == m_indices.size()) {
            std::shuffle(begin(m_indices), end(m_indices), m_rng);
            m_idx = 0;
        }
        return m_indices[m_idx++];
    }

private:
    std::vector<std::size_t> m_indices;
    std::default_random_engine m_rng;
    std::size_t m_idx = 0;

    mutable std::mutex *m_mutex = nullptr;
};

static constexpr auto kFilename = "./__testfile";

// Env multithreading tests
//
// Each Env instance created in a given process communicates with the same global
// "inode info manager". This is to overcome some shortcomings of POSIX advisory
// locks. Examples include (a) closing a file descriptor to an inode with locks
// held on it can cause all locks to be dropped, and (b) POSIX locks don't work
// between threads in the same process.
//
class EnvLockStateTests : public testing::Test
{
public:
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

protected:
    EnvWithFiles m_helper;
    // Pointer that gets delete'd in ~EnvWithFiles().
    Env *m_env;
};

TEST_F(EnvLockStateTests, LockingSequence)
{
    auto *f = new_file(kFilename);
    ASSERT_OK(m_env->lock(*f, Env::kShared));
    ASSERT_OK(m_env->lock(*f, Env::kReserved));
    ASSERT_OK(m_env->lock(*f, Env::kExclusive));
    ASSERT_OK(m_env->unlock(*f, Env::kShared));
    ASSERT_OK(m_env->unlock(*f, Env::kUnlocked));
}

TEST_F(EnvLockStateTests, MultipleSharedLocksAreAllowed)
{
    auto *a = new_file(kFilename);
    auto *b = new_file(kFilename);
    auto *c = new_file(kFilename);
    ASSERT_OK(m_env->lock(*a, Env::kShared));
    ASSERT_OK(m_env->lock(*b, Env::kShared));
    ASSERT_OK(m_env->lock(*c, Env::kShared));
    ASSERT_OK(m_env->unlock(*c, Env::kUnlocked));
    ASSERT_OK(m_env->unlock(*b, Env::kUnlocked));
    ASSERT_OK(m_env->unlock(*a, Env::kUnlocked));
}

TEST_F(EnvLockStateTests, SingleExclusiveLockIsAllowed)
{
    auto *a = new_file(kFilename);
    auto *b = new_file(kFilename);

    ASSERT_OK(m_env->lock(*a, Env::kShared));
    ASSERT_OK(m_env->lock(*a, Env::kExclusive));
    ASSERT_TRUE(m_env->lock(*b, Env::kShared).is_busy());
    ASSERT_OK(m_env->unlock(*a, Env::kUnlocked));

    ASSERT_OK(m_env->lock(*b, Env::kShared));
    ASSERT_OK(m_env->lock(*b, Env::kExclusive));
    ASSERT_OK(m_env->unlock(*b, Env::kUnlocked));
}

TEST_F(EnvLockStateTests, OnlySharedLocksAllowedWhileReserved)
{
    auto *a = new_file(kFilename);
    auto *b = new_file(kFilename);
    auto *c = new_file(kFilename);

    ASSERT_OK(m_env->lock(*a, Env::kShared));
    ASSERT_OK(m_env->lock(*a, Env::kReserved));

    ASSERT_OK(m_env->lock(*b, Env::kShared));
    ASSERT_TRUE(m_env->lock(*b, Env::kReserved).is_busy());
    ASSERT_TRUE(m_env->lock(*b, Env::kExclusive).is_busy());
    ASSERT_OK(m_env->lock(*c, Env::kShared));
    ASSERT_TRUE(m_env->lock(*c, Env::kReserved).is_busy());
    ASSERT_TRUE(m_env->lock(*c, Env::kExclusive).is_busy());

    ASSERT_OK(m_env->unlock(*a, Env::kUnlocked));
}

TEST_F(EnvLockStateTests, SharedLocksNotAllowedWhilePending)
{
    auto *a = new_file(kFilename);
    auto *b = new_file(kFilename);
    auto *c = new_file(kFilename);

    ASSERT_OK(m_env->lock(*a, Env::kShared));
    ASSERT_OK(m_env->lock(*b, Env::kShared));
    ASSERT_OK(m_env->lock(*a, Env::kReserved));

    // Fail to get the exclusive lock, leaving the state as kPending.
    ASSERT_TRUE(m_env->lock(*a, Env::kExclusive).is_busy());
    ASSERT_TRUE(m_env->lock(*c, Env::kShared).is_busy());

    ASSERT_OK(m_env->unlock(*b, Env::kUnlocked));
    ASSERT_OK(m_env->lock(*a, Env::kExclusive));
}

// This first test fixture accesses a given number of files (kNumFiles) through
// 1 Env instance using a given number of threads (kNumThreads).
struct SingleEnvTestParam {
    std::size_t num_threads = 0;
    std::size_t num_files = 0;
};
class SingleEnvTests : public testing::TestWithParam<SingleEnvTestParam>
{
public:
    const std::size_t kNumThreads = GetParam().num_threads;
    const std::size_t kNumFiles = GetParam().num_files;

    explicit SingleEnvTests()
        : m_delegator(m_mutex, kNumFiles)
    {
        m_env.env = Env::default_env();
    }

    ~SingleEnvTests() override
    {
        for (auto &thread : m_threads) {
            thread.join();
        }
    }

    auto SetUp() -> void override
    {
        ASSERT_GT(kNumThreads, 0) << "REQUIRES: kNumThreads > 0";
        ASSERT_GT(kNumFiles, 0) << "REQUIRES: kNumFiles > 0";
        for (std::size_t i = 0; i < kNumFiles; ++i) {
            const auto filename = make_filename(i);
            (void)m_env.env->remove_file(filename);

            File *file;
            ASSERT_OK(m_env.env->new_file(
                filename,
                Env::kCreate | Env::kReadWrite,
                file));
            write_version(*file, 0);
            m_env.files.emplace_back(file);
        }
    }

protected:
    mutable std::mutex m_mutex;
    WorkDelegator m_delegator;
    std::vector<std::thread> m_threads;
    EnvWithFiles m_env;
};

TEST_P(SingleEnvTests, 1)
{
    const auto work = [](auto &e, auto n) -> void {
        auto *file = e.files[n];

        Status s;
        for (;;) {
            s = e.env->lock(*file, Env::kShared);
            if (!s.is_busy()) {
                ASSERT_OK(s);
                break;
            }
            std::this_thread::yield();
        }

        const auto version = read_version(*file) + 1;

        for (;;) {
            s = e.env->lock(*file, Env::kExclusive);
            if (!s.is_busy()) {
                ASSERT_OK(s);
                break;
            }
            std::this_thread::yield();
        }

        write_version(*file, version);

        ASSERT_OK(e.env->unlock(*file, Env::kUnlocked));
    };
    static constexpr std::size_t kNumRounds = 5;
    const auto work_size = kNumFiles * kNumRounds;
    for (std::size_t i = 0; i < kNumThreads; ++i) {
        m_threads.emplace_back([&] {
            for (std::size_t r = 0; r < work_size; ++r) {
                work(m_env, r % kNumFiles);
            }
        });
    }

    for (auto &thread : m_threads) {
        thread.join();
    }
    m_threads.clear();

    for (auto *file : m_env.files) {
        ASSERT_EQ(work_size * kNumThreads, read_version(*file));
    }
}

INSTANTIATE_TEST_SUITE_P(
    SingleEnvTests,
    SingleEnvTests,
    ::testing::Values(
        SingleEnvTestParam {1, 1},

        SingleEnvTestParam {1, 2},
        SingleEnvTestParam {2, 1},
        //        SingleEnvTestParam {3, 1},
        //        SingleEnvTestParam {3, 2},
        //        SingleEnvTestParam {2, 3},
        //        SingleEnvTestParam {1, 3},

        //        SingleEnvTestParam {5, 1},
        //        SingleEnvTestParam {5, 2},
        //        SingleEnvTestParam {5, 3},
        //        SingleEnvTestParam {5, 4},
        //        SingleEnvTestParam {5, 5},
        //        SingleEnvTestParam {5, 6},
        //        SingleEnvTestParam {5, 7},
        SingleEnvTestParam {1, 1}));

struct MultiEnvTestParam {
    std::size_t num_threads = 0;
    std::size_t num_files = 0;
    std::size_t num_envs = 0;
};
class MultiEnvTest : public testing::TestWithParam<MultiEnvTestParam>
{
protected:
    ~MultiEnvTest() override
    {
        for (auto &thread : m_threads) {
            thread.join();
        }
    }

    auto SetUp() -> void override
    {
    }

    const std::size_t kNumThreads = GetParam().num_threads;
    const std::size_t kNumFiles = GetParam().num_files;
    const std::size_t kNumEnvs = GetParam().num_envs;

    mutable std::mutex m_mutex;
    std::vector<std::thread> m_threads;
    std::vector<EnvWithFiles> m_envs;
};

TEST_P(MultiEnvTest, 1)
{
    for (std::size_t i = 0; i < kNumThreads; ++i) {
        m_threads.emplace_back();
    }
}

INSTANTIATE_TEST_SUITE_P(
    MultiEnvTest,
    MultiEnvTest,
    ::testing::Values(
        MultiEnvTestParam {0, 0, 0},
        MultiEnvTestParam {0, 0, 0},
        MultiEnvTestParam {0, 0, 0},
        MultiEnvTestParam {0, 0, 0}));

template <class EnvType>
[[nodiscard]] auto open_file(EnvType &env, const std::string &filename) -> std::unique_ptr<File>
{
    File *temp = nullptr;
    EXPECT_OK(env.new_file(filename, Env::kCreate | Env::kReadWrite, temp));
    return std::unique_ptr<File>(temp);
}

// TODO: Just use tools::assign_file_contents() declared in tools.h.
auto write_whole_file(const std::string &path, const Slice &message) -> void
{
    std::ofstream ofs(path, std::ios::trunc);
    ofs << message.to_string();
}

// TODO: Just use tools::read_file_to_string() declared in tools.h.
[[nodiscard]] auto read_whole_file(const std::string &path) -> std::string
{
    std::string message;
    std::ifstream ifs(path);
    ifs.seekg(0, std::ios::end);
    message.resize(ifs.tellg());
    ifs.seekg(0, std::ios::beg);
    ifs.read(message.data(), message.size());
    return message;
}

auto write_out_randomly(tools::RandomGenerator &random, File &writer, const Slice &message) -> void
{
    constexpr std::size_t num_chunks = 20;
    ASSERT_GT(message.size(), num_chunks) << "File is too small for this test";
    Slice in(message);
    std::size_t counter = 0;

    while (!in.is_empty()) {
        const auto chunk_size = std::min<std::size_t>(in.size(), random.Next(message.size() / num_chunks));
        auto chunk = in.range(0, chunk_size);

        ASSERT_TRUE(writer.write(counter, chunk).is_ok());
        counter += chunk_size;
        in.advance(chunk_size);
    }
    ASSERT_TRUE(in.is_empty());
}

[[nodiscard]] auto read_back_randomly(tools::RandomGenerator &random, File &reader, std::size_t size) -> std::string
{
    static constexpr std::size_t num_chunks = 20;
    EXPECT_GT(size, num_chunks) << "File is too small for this test";
    std::string backing(size, '\x00');
    auto *out_data = backing.data();
    std::size_t counter = 0;

    while (counter < size) {
        const auto chunk_size = std::min<std::size_t>(size - counter, random.Next(size / num_chunks));
        const auto s = reader.read_exact(counter, chunk_size, out_data);
        EXPECT_TRUE(s.is_ok()) << "Error: " << s.to_string().data();
        out_data += chunk_size;
        counter += chunk_size;
    }
    return backing;
}

class FileTests
    : public EnvTestHarness<PosixEnv>,
      public testing::Test
{
public:
    ~FileTests() override = default;

    tools::RandomGenerator random;
};

class PosixReaderTests : public FileTests
{
public:
    PosixReaderTests()
    {
        write_whole_file(kDBFilename, "");
        file = open_file(env(), kDBFilename);
    }

    std::unique_ptr<File> file;
};

TEST_F(PosixReaderTests, NewFileIsEmpty)
{
    std::string backing(8, '\x00');
    ASSERT_TRUE(file->read_exact(0, 8, backing.data()).is_io_error());
}

TEST_F(PosixReaderTests, ReadsBackContents)
{
    auto data = random.Generate(500);
    write_whole_file(kDBFilename, data);
    ASSERT_EQ(read_back_randomly(random, *file, data.size()), data);
}

class PosixEditorTests : public FileTests
{
public:
    PosixEditorTests()
        : file(open_file(env(), kDBFilename))
    {
        write_whole_file(kDBFilename, "");
    }

    std::unique_ptr<File> file;
};

TEST_F(PosixEditorTests, NewFileIsEmpty)
{
    std::string backing(8, '\x00');
    ASSERT_TRUE(file->read_exact(0, 8, backing.data()).is_io_error());
}

TEST_F(PosixEditorTests, WritesOutAndReadsBackData)
{
    auto data = random.Generate(500);
    write_out_randomly(random, *file, data);
    ASSERT_EQ(read_back_randomly(random, *file, data.size()), data);
}

class FakeEnvTests
    : public EnvTestHarness<tools::FakeEnv>,
      public testing::Test
{
public:
    ~FakeEnvTests() override = default;

    tools::RandomGenerator random;
};

TEST_F(FakeEnvTests, ReaderStopsAtEOF)
{
    auto ra_editor = open_file(env(), kDBFilename);
    auto ra_reader = open_file(env(), kDBFilename);

    const auto data = random.Generate(500);
    write_out_randomly(random, *ra_editor, data);

    Slice slice;
    std::string buffer(data.size() * 2, '\x00');
    ASSERT_OK(ra_reader->read(0, buffer.size(), buffer.data(), &slice));
    ASSERT_EQ(slice.size(), data.size());
}

class TestEnvTests
    : public EnvTestHarness<tools::TestEnv>,
      public testing::Test
{
protected:
    ~TestEnvTests() override = default;
};

TEST_F(TestEnvTests, OperationsOnUnlinkedFiles)
{
    File *file;
    ASSERT_OK(env().new_file("test", Env::kCreate | Env::kReadWrite, file));
    ASSERT_OK(env().remove_file("test"));
    ASSERT_FALSE(env().file_exists("test"));

    std::size_t file_size;
    ASSERT_TRUE(env().file_size("test", file_size).is_not_found());

    // Read, write, and sync should still work.
    const Slice message("Hello, world!", 13);
    ASSERT_OK(file->write(0, message));
    ASSERT_OK(file->sync());
    char buffer[13];
    ASSERT_OK(file->read_exact(0, sizeof(buffer), buffer));
    ASSERT_EQ(message, Slice(buffer, sizeof(buffer)));

    // Interceptors should work.
    QUICK_INTERCEPTOR("test", tools::Interceptor::kSync);
    assert_special_error(file->sync());
    env().clear_interceptors();

    // The file was unlinked, so it should be empty next time it is opened.
    delete file;
    ASSERT_OK(env().new_file("test", Env::kCreate | Env::kReadWrite, file));
    Slice slice;
    ASSERT_OK(file->read(0, sizeof(buffer), buffer, &slice));
    ASSERT_TRUE(slice.is_empty());
    delete file;
}

} // namespace calicodb