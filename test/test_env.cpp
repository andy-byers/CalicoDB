// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#include "alloc.h"
#include "calicodb/env.h"
#include "common.h"
#include "encoding.h"
#include "fake_env.h"
#include "temp.h"
#include "test.h"
#include <filesystem>
#include <gtest/gtest.h>
#include <thread>

namespace calicodb::test
{

static auto make_filename(size_t n)
{
    return numeric_key<10>(n);
}
static auto write_out_randomly(RandomGenerator &random, File &writer, const Slice &message) -> void
{
    constexpr size_t kChunks = 20;
    ASSERT_GT(message.size(), kChunks) << "File is too small for this test";
    Slice in(message);
    size_t counter = 0;

    while (!in.is_empty()) {
        const auto chunk_size = minval<size_t>(in.size(), random.Next(message.size() / kChunks));
        auto chunk = in.range(0, chunk_size);

        ASSERT_TRUE(writer.write(counter, chunk).is_ok());
        counter += chunk_size;
        in.advance(chunk_size);
    }
    ASSERT_TRUE(in.is_empty());
}
[[nodiscard]] static auto read_back_randomly(RandomGenerator &random, File &reader, size_t size) -> std::string
{
    static constexpr size_t kChunks = 20;
    EXPECT_GT(size, kChunks) << "File is too small for this test";
    std::string backing(size, '\x00');
    auto *out_data = backing.data();
    size_t counter = 0;

    while (counter < size) {
        const auto chunk_size = minval<size_t>(size - counter, random.Next(size / kChunks));
        const auto s = reader.read_exact(counter, chunk_size, out_data);
        EXPECT_TRUE(s.is_ok()) << "Error: " << s.message();
        out_data += chunk_size;
        counter += chunk_size;
    }
    return backing;
}
struct EnvWithFiles final {
    explicit EnvWithFiles()
        : m_dirname(testing::TempDir())
    {
    }

    ~EnvWithFiles()
    {
        cleanup_files();
        if (env != &Env::default_env()) {
            delete env;
        }
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

    [[nodiscard]] auto open_file(size_t id, Env::OpenMode mode, bool clear = false) const -> File *
    {
        File *file;
        const auto filename = m_dirname + make_filename(id);
        EXPECT_TRUE(env->new_file(filename.c_str(), mode, file).is_ok())
            << "failed to " << ((mode & Env::kCreate) ? "create" : "open") << " file \"" << filename << '"';
        if (clear) {
            EXPECT_OK(file->resize(0));
        }
        return file;
    }

    auto open_unowned_file(NextFileName name, Env::OpenMode mode, bool clear = false) -> File *
    {
        std::lock_guard lock(mutex);
        if (name == kDifferentName) {
            ++last_id;
        }
        const auto id = last_id;
        auto *file = open_file(id, mode, clear);
        files.emplace_back(file);
        return file;
    }

    mutable std::mutex mutex;
    std::string m_dirname;
    std::vector<File *> files;
    Env *env = nullptr;
    size_t last_id = 0;
};

// Helper for testing shared memory
class SharedBuffer final
{
public:
    explicit SharedBuffer(File &file)
        : m_file(&file)
    {
    }

    auto read(size_t offset, size_t size) -> std::string
    {
        std::string out(size, '\0');
        auto *ptr = out.data();
        for (auto r = offset / File::kShmRegionSize; size; ++r) {
            volatile void *mem;
            EXPECT_OK(m_file->shm_map(r, true, mem));
            const volatile char *begin = reinterpret_cast<volatile char *>(mem);
            size_t copy_offset = 0;
            if (ptr == out.data()) {
                copy_offset = offset % File::kShmRegionSize;
            }
            auto copy_size = minval(size, File::kShmRegionSize - copy_offset);
            std::memcpy(ptr, const_cast<const char *>(begin) + copy_offset, copy_size);
            ptr += copy_size;
            size -= copy_size;
        }
        return out;
    }

    auto write(size_t offset, const Slice &in) -> void
    {
        const auto r1 = offset / File::kShmRegionSize;
        Slice copy(in);
        for (auto r = r1; !copy.is_empty(); ++r) {
            volatile void *mem;
            EXPECT_OK(m_file->shm_map(r, true, mem));
            EXPECT_TRUE(mem);
            volatile char *begin = reinterpret_cast<volatile char *>(mem);
            size_t copy_offset = 0;
            if (r == r1) {
                copy_offset = offset % File::kShmRegionSize;
            }
            auto copy_size = minval(copy.size(), File::kShmRegionSize - copy_offset);
            std::memcpy(const_cast<char *>(begin) + copy_offset, copy.data(), copy_size);
            copy.advance(copy_size);
        }
    }

private:
    File *m_file;
};

class FileTests : public testing::TestWithParam<size_t>
{
public:
    const size_t kCount = GetParam();

    explicit FileTests()
    {
        m_env = &Env::default_env();
        m_helper.env = m_env;
    }

    ~FileTests() override = default;

    auto test_same_inode() -> void
    {
        const auto message = m_random.Generate(1'024);
        auto *original = m_helper.open_unowned_file(EnvWithFiles::kDifferentName, Env::kCreate);
        write_out_randomly(m_random, *original, message);
        for (size_t i = 0; i < kCount; ++i) {
            auto *file = m_helper.open_unowned_file(EnvWithFiles::kSameName, Env::kReadOnly);
            ASSERT_EQ(to_string(message), read_back_randomly(m_random, *file, message.size()));
        }
    }

protected:
    RandomGenerator m_random;
    EnvWithFiles m_helper;
    // Pointer to an object owned by m_helper.
    Env *m_env;
};

TEST_P(FileTests, OpenAndClose)
{
    for (size_t i = 0; i < 2; ++i) {
        auto *file = m_helper.open_unowned_file(
            EnvWithFiles::kSameName,
            Env::kCreate);
        for (size_t j = 0; j < 2; ++j) {
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

class LoggerTests : public testing::Test
{
protected:
    static constexpr auto kHdrLen = std::char_traits<char>::length(
        "0000/00/00-00:00:00.000000 ");

    explicit LoggerTests()
        : m_log_filename(testing::TempDir() + "logger")
    {
    }

    ~LoggerTests() override
    {
        delete m_logger;
    }

    auto SetUp() -> void override
    {
        reset();
    }

    auto reset() -> void
    {
        delete m_logger;
        m_logger = nullptr;
        std::filesystem::remove_all(m_log_filename);
        ASSERT_OK(Env::default_env().new_logger(m_log_filename.c_str(), m_logger));
    }

    const std::string m_log_filename;
    Logger *m_logger = nullptr;
};

TEST_F(LoggerTests, LogNullptrIsNOOP)
{
    log(nullptr, "nothing %d", 42);
    ASSERT_TRUE(read_file_to_string(Env::default_env(), m_log_filename.c_str()).empty());
}

TEST_F(LoggerTests, LogsFormattedText)
{
    log(m_logger, "%u foo", 123);
    const auto msg1 = read_file_to_string(Env::default_env(), m_log_filename.c_str());
    log(m_logger, "bar %d", 42);
    const auto msg2 = read_file_to_string(Env::default_env(), m_log_filename.c_str());

    // Make sure both text and header info were written.
    ASSERT_EQ(kHdrLen, msg1.find("123 foo\n"));
    ASSERT_EQ(kHdrLen * 2 + 8, msg2.find("bar 42\n"));
    ASSERT_EQ(msg1.size(), kHdrLen + 8);
    ASSERT_EQ(msg2.size(), kHdrLen * 2 + 15);
}

TEST_F(LoggerTests, HandlesMessages)
{
    std::string msg;
    for (size_t n = 0; n < 512; ++n) {
        reset();

        msg.resize(n, '$');
        log(m_logger, "%s", msg.c_str());

        const auto res = read_file_to_string(Env::default_env(), m_log_filename.c_str());
        ASSERT_EQ(msg + '\n', res.substr(kHdrLen)); // Account for the datetime header and trailing newline.
    }
}

TEST_F(LoggerTests, HandlesLongMessages)
{
    std::string msg;
    for (size_t n = 1'000; n < 10'000; n *= 10) {
        reset();

        msg.resize(n, '$');
        log(m_logger, "%s", msg.c_str());

        const auto res = read_file_to_string(Env::default_env(), m_log_filename.c_str());
        ASSERT_EQ(msg + '\n', res.substr(kHdrLen)); // Account for the datetime header and trailing newline.
    }
}

class EnvLockStateTests : public testing::TestWithParam<size_t>
{
public:
    const size_t kReplicates = GetParam();
    std::string m_filename;

    explicit EnvLockStateTests()
        : m_filename(testing::TempDir() + "filename")
    {
        m_env = &Env::default_env();
        m_helper.env = m_env;
    }

    ~EnvLockStateTests() override
    {
        (void)m_env->remove_file(m_filename.c_str());
    }

    auto new_file(const char *filename) -> File *
    {
        File *file;
        EXPECT_OK(m_env->new_file(
            filename,
            Env::kCreate,
            file));
        m_helper.files.emplace_back(file);
        return file;
    }

    auto test_sequence(bool) -> void
    {
        auto *f = new_file(m_filename.c_str());
        ASSERT_OK(f->file_lock(kFileShared));
        ASSERT_OK(f->file_lock(kFileExclusive));
        f->file_unlock();
    }

    auto test_shared() -> void
    {
        auto *a = new_file(m_filename.c_str());
        auto *b = new_file(m_filename.c_str());
        auto *c = new_file(m_filename.c_str());
        ASSERT_OK(a->file_lock(kFileShared));
        ASSERT_OK(b->file_lock(kFileShared));
        ASSERT_OK(c->file_lock(kFileShared));
        c->file_unlock();
        b->file_unlock();
        a->file_unlock();
    }

    auto test_exclusive() -> void
    {
        auto *a = new_file(m_filename.c_str());
        auto *b = new_file(m_filename.c_str());

        ASSERT_OK(a->file_lock(kFileShared));
        ASSERT_OK(a->file_lock(kFileExclusive));

        // Try to take a shared file_lock on "b", but fail due to "a"'s exclusive
        // file_lock.
        ASSERT_TRUE(b->file_lock(kFileShared).is_busy());

        // Unlock "a" and let "b" get the exclusive file_lock.
        a->file_unlock();
        ASSERT_OK(b->file_lock(kFileShared));
        ASSERT_OK(b->file_lock(kFileExclusive));
        b->file_unlock();
    }

    template <class Test>
    auto run_test(const Test &&test)
    {
        for (size_t i = 0; i < kReplicates; ++i) {
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
    auto *f = new_file(m_filename.c_str());

    ASSERT_OK(f->file_lock(kFileShared));
    ASSERT_OK(f->file_lock(kFileShared));

    ASSERT_OK(f->file_lock(kFileShared));

    ASSERT_OK(f->file_lock(kFileExclusive));
    ASSERT_OK(f->file_lock(kFileExclusive));
    ASSERT_OK(f->file_lock(kFileShared));

    f->file_unlock();
    f->file_unlock();
}

#ifndef NDEBUG
TEST_P(EnvLockStateTests, InvalidRequestDeathTest)
{
    auto *f = new_file(m_filename.c_str());
    // kUnlocked -> kShared is the only allowed transition out of kUnlocked.
    ASSERT_DEATH((void)f->file_lock(kFileExclusive), "");
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
        m_helper.env = &Env::default_env();
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
    ASSERT_OK(a->shm_map(0, true, ptr));
    ASSERT_OK(b->shm_map(0, true, ptr));
    ASSERT_OK(c->shm_map(0, true, ptr));

    // Shared locks can overlap, but they can only be 1 byte long.
    for (size_t i = 0; i < 8; ++i) {
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

    delete a;
    delete b;
    delete c;
}

static auto busy_wait_file_lock(File &file, bool is_writer) -> void
{
    Status s;
    do {
        s = file.file_lock(kFileShared);
        if (s.is_ok()) {
            if (is_writer) {
                s = file.file_lock(kFileExclusive);
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
static auto busy_wait_shm_lock(File &file, size_t r, size_t n, ShmLockFlag flags) -> void
{
    ASSERT_LE(r + n, File::kShmLockCount);
    for (;;) {
        const auto s = file.shm_lock(r, n, flags);
        if (s.is_ok()) {
            return;
        } else if (!s.is_busy()) {
            ADD_FAILURE() << s.message();
        }
        std::this_thread::yield();
    }
}

TEST(EnvWrappers, WrapperEnvWorksAsExpected)
{
    FakeEnv env;
    EnvWrapper w_env(env);
    ASSERT_EQ(&env, w_env.target());
    ASSERT_EQ(&env, const_cast<const EnvWrapper &>(w_env).target());
    File *file;
    Logger *sink;
    ASSERT_OK(w_env.new_file("file", Env::kCreate, file));
    ASSERT_TRUE(w_env.new_logger("sink", sink).is_not_supported());
    ASSERT_TRUE(w_env.file_exists("file"));
    delete file;
    size_t size;
    ASSERT_OK(w_env.file_size("file", size));
    ASSERT_EQ(size, 0);
    w_env.srand(123);
    (void)w_env.rand();
    w_env.sleep(0);
    ASSERT_OK(w_env.remove_file("file"));
}

class TempEnvTests : public testing::TestWithParam<size_t>
{
public:
    const size_t m_sector_size;
    std::unique_ptr<Env> m_env;
    std::unique_ptr<File> m_file;
    std::string m_result;
    std::string m_buffer;
    RandomGenerator m_random;

    explicit TempEnvTests()
        : m_sector_size(GetParam()),
          m_env(new_temp_env(m_sector_size)),
          m_result(m_sector_size * 3, '\0'),
          m_buffer(m_result),
          m_random(m_result.size())
    {
    }

    ~TempEnvTests() override = default;

    auto SetUp() -> void override
    {
        File *file;
        ASSERT_OK(m_env->new_file("temp", Env::OpenMode(), file));
        m_file.reset(file);
    }

    auto random_size(size_t offset) -> size_t
    {
        return m_random.Next(1, minval(m_result.size() - offset, m_sector_size));
    }

    auto random_data(size_t offset)
    {
        return m_random.Generate(random_size(offset));
    }

    auto write_file(size_t offset, const Slice &data) -> void
    {
        ASSERT_OK(m_file->write(offset, data));
        ASSERT_LE(offset + data.size(), m_result.size());
        std::memcpy(m_result.data() + offset, data.data(), data.size());
    }

    auto check_file(size_t offset, size_t size) -> void
    {
        std::fill(begin(m_buffer), end(m_buffer), '\0');
        ASSERT_LT(offset, m_buffer.size());
        ASSERT_LE(offset + size, m_buffer.size());
        ASSERT_OK(m_file->read_exact(offset, size, m_buffer.data()));
        ASSERT_EQ(to_slice(m_buffer).range(0, size),
                  to_slice(m_result).range(offset, size));
    }
};

TEST_P(TempEnvTests, Operations)
{
    // NOOPs.
    EXPECT_OK(m_file->file_lock(FileLockMode()));
    m_file->file_unlock();

    // Not supported.
    EXPECT_NOK(m_file->shm_lock(0, 1, ShmLockFlag()));
    volatile void *ptr;
    EXPECT_NOK(m_file->shm_map(0, false, ptr));
    m_file->shm_unmap(true);

    // File should still be accessible through the pointer returned by new_file().
    EXPECT_TRUE(m_env->file_exists("temp"));
    EXPECT_OK(m_env->remove_file("temp"));
    EXPECT_FALSE(m_env->file_exists("temp"));

    m_file.reset();

    m_env->srand(42);
    m_env->rand();
    m_env->sleep(1);

    Logger *logger;
    ASSERT_OK(m_env->new_logger("NOOP", logger));
    ASSERT_EQ(logger, nullptr);
}

TEST_P(TempEnvTests, SequentialIO)
{
    size_t offset = 0;
    while (offset < m_result.size()) {
        const auto chunk = random_data(offset);
        write_file(offset, chunk);
        offset += chunk.size();
    }
    offset = 0;
    while (offset < m_result.size()) {
        const auto chunk_size = random_size(offset);
        check_file(offset, chunk_size);
        offset += chunk_size;
    }
    check_file(0, m_result.size());
}

TEST_P(TempEnvTests, RandomIO)
{
    size_t file_size = 0;
    RandomGenerator random;
    for (size_t i = 0; i < 100; ++i) {
        const auto chunk = random.Generate(random.Next(m_sector_size / 2));
        const auto offset = random.Next(m_result.size() - chunk.size());
        file_size = maxval<size_t>(file_size, offset + chunk.size());
        write_file(offset, chunk);
        check_file(offset, chunk.size());
    }
    check_file(0, file_size);
}

TEST_P(TempEnvTests, LargeIO)
{
    RandomGenerator random(m_buffer.size());
    const auto data = random.Generate(m_buffer.size());
    std::memcpy(m_buffer.data(), data.data(), data.size());

    write_file(100, data.range(0, data.size() - 200));
    check_file(0, m_result.size() - 200);
    check_file(100, m_result.size() - 200);

    write_file(0, data);
    check_file(0, m_result.size());
}

INSTANTIATE_TEST_SUITE_P(
    TempEnvTests,
    TempEnvTests,
    testing::Values(kMinPageSize / 2,
                    kMinPageSize,
                    kMinPageSize * 2,
                    kMaxPageSize / 2,
                    kMaxPageSize,
                    kMaxPageSize * 2));

class FileConcurrencyTests : public testing::TestWithParam<std::tuple<size_t, size_t>>
{
public:
    struct State {
        std::vector<int> output;
        const char *filename;
        int *resource;
        File *file;
    };

    const std::tuple<size_t, size_t> m_options;
    const std::string m_filename;
    std::vector<std::thread> m_threads;
    std::vector<State> m_states;
    int m_resource;

    explicit FileConcurrencyTests()
        : m_options(GetParam()),
          m_filename(testing::TempDir() + "calicodb_file_concurrency"),
          m_resource(0)
    {
    }

    ~FileConcurrencyTests() override
    {
        std::filesystem::remove_all(m_filename.c_str());
    }

    auto run_test() -> void
    {
        const size_t num_readers = std::get<0>(m_options);
        const size_t num_writers = std::get<1>(m_options);
        static constexpr size_t kNumRounds = 256;
        const auto num_threads = num_readers + num_writers;
        while (m_states.size() < num_threads) {
            m_states.emplace_back();
            m_states.back().filename = m_filename.c_str();
            m_states.back().resource = &m_resource;
            m_states.back().file = nullptr;
        }

        for (size_t n = 0; n < num_threads; ++n) {
            auto *action = n < num_readers ? reader : writer;
            m_threads.emplace_back([action, &s = m_states.at(n)] {
                for (size_t i = 0; i < kNumRounds; ++i) {
                    action(s);
                }
            });
        }
        for (auto &t : m_threads) {
            t.join();
        }

        for (size_t n = 0; n < num_threads; ++n) {
            const auto &s = m_states.at(n);
            ASSERT_EQ(s.file, nullptr);
            auto sorted = s.output;
            std::sort(begin(sorted), end(sorted));
            ASSERT_EQ(sorted, s.output);
        }

        ASSERT_EQ(m_resource, kNumRounds * num_writers);
    }

    static auto open_file(State &state) -> Status
    {
        return Env::default_env().new_file(
            state.filename,
            Env::kCreate | Env::kReadWrite,
            state.file);
    }

    static auto close_file(State &state) -> void
    {
        delete std::exchange(state.file, nullptr);
    }

    static auto reader(State &state) -> void
    {
        ASSERT_OK(open_file(state));
        busy_wait_file_lock(*state.file, false);
        state.output.push_back(*state.resource);
        close_file(state);
    }

    static auto writer(State &state) -> void
    {
        ASSERT_OK(open_file(state));
        busy_wait_file_lock(*state.file, true);
        state.output.push_back((*state.resource)++);
        close_file(state);
    }
};

TEST_P(FileConcurrencyTests, Run)
{
    run_test();
}

INSTANTIATE_TEST_SUITE_P(
    FileConcurrencyTests,
    FileConcurrencyTests,
    ::testing::Combine(
        testing::Values(1, 2, 5, 10),
        testing::Values(0, 1, 2, 5, 10)));

struct ShmLockPattern {
    size_t lock_ofs;
    size_t lock_len;
    int write;
};
class ShmConcurrencyTests : public testing::TestWithParam<std::vector<ShmLockPattern>>
{
public:
    struct SharedState {
        int resources[File::kShmLockCount];
        const std::string filename;
    } m_shared;

    struct State {
        std::vector<int> outputs[File::kShmLockCount];
        File *file = nullptr;
        size_t lock_ofs = 0;
        size_t lock_len = 0;
    };

    const std::vector<ShmLockPattern> m_options;
    std::vector<std::thread> m_threads;
    std::vector<State> m_states;
    File *m_file;

    explicit ShmConcurrencyTests()
        : m_shared{{}, testing::TempDir() + "calicodb_file_concurrency"},
          m_options(GetParam())
    {
    }

    ~ShmConcurrencyTests() override
    {
        std::filesystem::remove_all(m_shared.filename.c_str());
    }

    auto SetUp() -> void override
    {
        ASSERT_OK(Env::default_env().new_file(
            m_shared.filename.c_str(),
            Env::kCreate | Env::kReadWrite,
            m_file));
        volatile void *ptr;
        ASSERT_OK(m_file->shm_map(0, true, ptr));
    }

    auto TearDown() -> void override
    {
        m_file->shm_unmap(true);
        delete std::exchange(m_file, nullptr);
    }

    auto run_test() -> void
    {
        static constexpr size_t kNumRounds = 256;
        const auto num_threads = m_options.size();
        int expected[File::kShmLockCount] = {};
        for (auto [ofs, len, write] : m_options) {
            for (size_t i = 0; i < len; ++i) {
                expected[ofs + i] += write;
            }
        }

        m_states.resize(num_threads);
        for (size_t n = 0; n < num_threads; ++n) {
            const auto [ofs, len, write] = m_options.at(n);
            auto *action = write ? writer : reader;
            auto &s = m_states.at(n);
            s.lock_len = len;
            s.lock_ofs = ofs;
            m_threads.emplace_back([action, &s, &shared = m_shared] {
                for (size_t i = 0; i < kNumRounds; ++i) {
                    action(shared, s);
                }
            });
        }
        for (auto &t : m_threads) {
            t.join();
        }

        for (const auto &s : m_states) {
            for (size_t i = 0; i < s.lock_len; ++i) {
                const auto &output = s.outputs[i + s.lock_ofs];
                auto sorted = output;
                std::sort(begin(sorted), end(sorted));
                ASSERT_EQ(sorted, output);
            }
        }
        for (size_t i = 0; i < ARRAY_SIZE(expected); ++i) {
            ASSERT_EQ(m_shared.resources[i], static_cast<int>(kNumRounds) * expected[i]);
        }
    }

    static auto open_file(State &state, const char *filename) -> Status
    {
        return Env::default_env().new_file(
            filename,
            Env::kCreate | Env::kReadWrite,
            state.file);
    }

    static auto close_file(State &state) -> void
    {
        delete std::exchange(state.file, nullptr);
    }

    static auto reader(SharedState &shared, State &state) -> void
    {
        ASSERT_EQ(state.lock_len, 1);
        ASSERT_OK(open_file(state, shared.filename.c_str()));

        volatile void *ptr;
        ASSERT_OK(state.file->shm_map(state.lock_ofs, false, ptr));
        busy_wait_shm_lock(*state.file, state.lock_ofs, 1, kShmLock | kShmReader);
        state.outputs[state.lock_ofs].push_back(shared.resources[state.lock_ofs]);
        ASSERT_OK(state.file->shm_lock(state.lock_ofs, 1, kShmUnlock | kShmReader));
        state.file->shm_unmap(false);
        close_file(state);
    }

    static auto writer(SharedState &shared, State &state) -> void
    {
        ASSERT_OK(open_file(state, shared.filename.c_str()));

        volatile void *ptr;
        ASSERT_OK(state.file->shm_map(state.lock_ofs, true, ptr));
        busy_wait_shm_lock(*state.file, state.lock_ofs, state.lock_len, kShmLock | kShmWriter);
        for (size_t i = 0; i < state.lock_len; ++i) {
            const auto r = i + state.lock_ofs;
            state.outputs[r].push_back(shared.resources[r]++);
        }
        ASSERT_OK(state.file->shm_lock(state.lock_ofs, state.lock_len, kShmUnlock | kShmWriter));
        state.file->shm_unmap(false);
        close_file(state);
    }
};

TEST_P(ShmConcurrencyTests, Run)
{
    run_test();
}

INSTANTIATE_TEST_SUITE_P(
    SingleLock,
    ShmConcurrencyTests,
    ::testing::Values(                            // 01234567
        std::vector<ShmLockPattern>{{0, 1, 1}},   // w.......
        std::vector<ShmLockPattern>{{0, 1, 0},    // r.......
                                    {0, 1, 1}},   // w.......
        std::vector<ShmLockPattern>{{0, 1, 0},    // r.......
                                    {0, 1, 0},    // r.......
                                    {0, 1, 1}},   // w.......
        std::vector<ShmLockPattern>{{0, 1, 0},    // r.......
                                    {0, 1, 0},    // r.......
                                    {0, 1, 0},    // r.......
                                    {0, 1, 1},    // w.......
                                    {0, 1, 1}})); // w.......

INSTANTIATE_TEST_SUITE_P(
    MultiLock,
    ShmConcurrencyTests,
    ::testing::Values(                            // 01234567
        std::vector<ShmLockPattern>{{0, 2, 1}},   // ww......
        std::vector<ShmLockPattern>{{0, 1, 0},    // r.......
                                    {0, 2, 1}},   // ww......
        std::vector<ShmLockPattern>{{0, 1, 0},    // r.......
                                    {1, 1, 0},    // .r......
                                    {0, 2, 1}},   // ww......
        std::vector<ShmLockPattern>{{0, 1, 0},    // r.......
                                    {1, 1, 0},    // .r......
                                    {2, 1, 0},    // ..r.....
                                    {0, 2, 1},    // ww......
                                    {1, 3, 1}},   // .ww.....
        std::vector<ShmLockPattern>{{0, 1, 0},    // r.......
                                    {1, 1, 0},    // .r......
                                    {2, 1, 0},    // ..r.....
                                    {3, 1, 0},    // ...r....
                                    {4, 1, 0},    // ....r...
                                    {5, 1, 0},    // .....r..
                                    {6, 1, 0},    // ......r.
                                    {7, 1, 0},    // .......r
                                    {0, 8, 1}},   // wwwwwwww
        std::vector<ShmLockPattern>{{0, 1, 0},    // r.......
                                    {1, 1, 0},    // .r......
                                    {2, 1, 0},    // ..r.....
                                    {3, 1, 0},    // ...r....
                                    {0, 1, 0},    // r.......
                                    {1, 1, 0},    // .r......
                                    {2, 1, 0},    // ..r.....
                                    {3, 1, 0},    // ...r....
                                    {0, 8, 1}},   // wwwwwwww
        std::vector<ShmLockPattern>{{5, 1, 0},    // .....r..
                                    {7, 1, 0},    // .......r
                                    {6, 2, 1},    // ......ww
                                    {4, 2, 1},    // ....ww..
                                    {2, 4, 1},    // ..wwww..
                                    {0, 6, 1}})); // wwwwww..

} // namespace calicodb::test