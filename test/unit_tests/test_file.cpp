#include <filesystem>
#include <fstream>
#include <limits>

#include <gtest/gtest.h>

#include "cub/common.h"
#include "random.h"
#include "file/file.h"
#include "file/interface.h"
#include "file/system.h"
#include "fakes.h"
#include "unit_tests.h"

namespace {

using namespace cub;

const std::string TEST_STRING = "TEST_STRING";

template<class S> auto read_string(S &store, std::string &buffer) -> Size
{
    return store.read(_b(buffer));
}

template<class S> auto read_exact_string(S &store, std::string &buffer) -> void
{
    read_exact(store, _b(buffer));
}

template<class S> auto write_string(S &store, const std::string &buffer) -> Size
{
    return store.write(_b(buffer));
}

template<class S> auto write_exact_string(S &store, const std::string &buffer) -> void
{
    write_exact(store, _b(buffer));
}

auto test_random_reads_and_writes(IReadWriteFile &store) -> void
{
    static constexpr Size payload_size = 1'000;
    Random random {0};
    const auto payload_out = random.next_string(payload_size);
    auto out = _b(payload_out);

    // Write out the string in random-sized chunks.
    while (!out.is_empty()) {
        const auto chunk_size = random.next_int(out.size());
        store.write(out.range(0, chunk_size));
        out.advance(chunk_size);
    }
    ASSERT_EQ(store.seek(0, Seek::BEGIN), 0);

    std::string payload_in(payload_size, '\x00');
    auto in = _b(payload_in);

    // Read back the string in random-sized chunks.
    while (!in.is_empty()) {
        const auto chunk_size = random.next_int(in.size());
        store.read(in.range(0, chunk_size));
        in.advance(chunk_size);
    }
    ASSERT_EQ(payload_in, payload_out);
}

class FileTests: public testing::Test {
public:
    const std::string PATH = "/tmp/cub_test_file";

    FileTests()
        : test_buffer(TEST_STRING.size(), '\x00') {}

    ~FileTests() override = default;

    static auto open_ro(const std::string &name, Mode mode) -> std::unique_ptr<IReadOnlyFile>
    {
        return open_file<ReadOnlyFile>(name, mode);
    }

    static auto open_wo(const std::string &name, Mode mode) -> std::unique_ptr<IWriteOnlyFile>
    {
        return open_file<WriteOnlyFile>(name, mode);
    }

    static auto open_rw(const std::string &name, Mode mode) -> std::unique_ptr<IReadWriteFile>
    {
        return open_file<ReadWriteFile>(name, mode);
    }

    static auto open_log(const std::string &name, Mode mode) -> std::unique_ptr<ILogFile>
    {
        return open_file<LogFile>(name, mode);
    }

    std::string test_buffer;

private:
    template<class S> static auto open_file(const std::string &name, Mode mode) -> std::unique_ptr<S>
    {
        return std::make_unique<S>(name, mode, 0666);
    }
};

TEST_F(FileTests, ExistsAfterClose)
{
    // File is closed in the destructor.
    open_ro(PATH, Mode::CREATE);
    ASSERT_TRUE(std::filesystem::exists(PATH));
}

TEST_F(FileTests, ReadFromFile)
{
    {
        auto ofs = std::ofstream{PATH};
        ofs << TEST_STRING;
    }
    auto file = open_ro(PATH, {});
    read_exact_string(*file, test_buffer);
    ASSERT_EQ(test_buffer, TEST_STRING);
}

TEST_F(FileTests, WriteToFile)
{
    auto file = open_wo(PATH, Mode::CREATE | Mode::TRUNCATE);
    write_string(*file, TEST_STRING);
    auto ifs = std::ifstream{PATH};
    std::string result;
    ifs >> result;
    ASSERT_EQ(result, TEST_STRING);
}

TEST_F(FileTests, ReportsEOFDuringRead)
{
    auto file = open_rw("a", Mode::CREATE | Mode::TRUNCATE);
    write_exact_string(*file, TEST_STRING);
    ASSERT_EQ(file->seek(0, Seek::BEGIN), 0);
    test_buffer.resize(TEST_STRING.size() * 2);
    // Try to read past EOF.
    ASSERT_EQ(read_string(*file, test_buffer), TEST_STRING.size());
    test_buffer.resize(TEST_STRING.size());
    ASSERT_EQ(test_buffer, TEST_STRING);
}

TEST_F(FileTests, RandomReadsAndWrites)
{
    auto file = open_rw("a", Mode::CREATE | Mode::TRUNCATE);
    test_random_reads_and_writes(*file);
}

class FileFailureTests: public testing::Test {
public:
    static constexpr auto PATH = "/tmp/cub_file_failure";
    static constexpr Size OVERFLOW_SIZE {std::numeric_limits<Size>::max()};

    FileFailureTests()
        : file {std::make_unique<ReadWriteFile>(PATH, Mode::CREATE | Mode::TRUNCATE, 0666)} {}

    ~FileFailureTests() override = default;

    std::unique_ptr<IReadWriteFile> file;
    Byte *fake_ptr {reinterpret_cast<Byte*>(123)};
    Bytes large_slice {fake_ptr, OVERFLOW_SIZE};
};

TEST_F(FileFailureTests, FailsWhenFileExistsButShouldNot)
{
    ASSERT_THROW(ReadWriteFile(PATH, Mode::CREATE | Mode::EXCLUSIVE, 0666), SystemError);
}

TEST_F(FileFailureTests, FailsWhenFileDoesNotExistButShould)
{
    file.reset();
    std::filesystem::remove(PATH);
    ASSERT_THROW(ReadWriteFile(PATH, Mode {}, 0666), SystemError);
}

TEST_F(FileFailureTests, ThrowsWhenReadSizeIsTooLarge)
{
    ASSERT_THROW(file->read(large_slice), SystemError);
}

TEST_F(FileFailureTests, ThrowsWhenWriteSizeIsTooLarge)
{
    ASSERT_THROW(file->write(large_slice), SystemError);
}

TEST_F(FileFailureTests, ThrowsWhenSeekOffsetIsTooLarge)
{
    ASSERT_THROW(file->seek(static_cast<long>(OVERFLOW_SIZE), Seek::BEGIN), SystemError);
}

TEST_F(FileFailureTests, ThrowsWhenNewSizeIsTooLarge)
{
    ASSERT_THROW(file->resize(OVERFLOW_SIZE), SystemError);
}

class SystemTests: public testing::Test {
public:
    static constexpr auto PATH = "/tmp/cub_system";

    SystemTests()
    {
        closed_fd = system::open(PATH, O_CREAT | O_TRUNC, 0666);

        // system::open() should throw rather than returning -1.
        CUB_EXPECT_NE(closed_fd, -1);
        system::close(closed_fd);
    }

    ~SystemTests() override
    {
        try {
            system::unlink(PATH);
        } catch (const SystemError&) {
            // File may not exist after some tests.
        }
    }

    int closed_fd {-1};
};

#ifdef CUB_OSX

TEST_F(SystemTests, CannotCallUseDirectIoOnUnopenedFile)
{
    ASSERT_THROW(system::use_direct_io(closed_fd), SystemError);
}

#endif // CUB_OSX

TEST_F(SystemTests, CannotUseUnopenedFile)
{
    Bytes b;
    ASSERT_THROW(system::size(closed_fd), SystemError);
    ASSERT_THROW(system::read(closed_fd, b), SystemError);
    ASSERT_THROW(system::write(closed_fd, b), SystemError);
    ASSERT_THROW(system::seek(closed_fd, 1, SEEK_SET), SystemError);
    ASSERT_THROW(system::close(closed_fd), SystemError);
    ASSERT_THROW(system::sync(closed_fd), SystemError);
    ASSERT_THROW(system::resize(closed_fd, 1), SystemError);
}

TEST_F(SystemTests, CannotRenameNonexistentFile)
{
    puts(PATH);
    system::unlink(PATH);
    ASSERT_THROW(system::rename(PATH, "/tmp/should_have_failed"), SystemError);
}

class MemoryTests: public FileTests {
protected:
    ~MemoryTests() override = default;
};

TEST_F(MemoryTests, SeekDeathTest)
{
    auto file = std::make_unique<ReadOnlyMemory>();

    // We shouldn't be able to negative offsets.
    ASSERT_DEATH(file->seek(-1, Seek::BEGIN), EXPECTATION_MATCHER);
    ASSERT_DEATH(file->seek(-1, Seek::CURRENT), EXPECTATION_MATCHER);
    ASSERT_DEATH(file->seek(-1, Seek::END), EXPECTATION_MATCHER);

    // We also shouldn't be able to seek to an index not representable by an off_t (64-bits
    // on my platform). Unfortunately, testing this requires invoking signed integer overflow.
    // TODO: Testing on platforms where off_t is 32 bits wide (csgrads1.utdallas.edu for example).
}

TEST_F(MemoryTests, RandomReadsAndWrites)
{
    auto fake_file = std::make_unique<ReadWriteMemory>();
    test_random_reads_and_writes(*fake_file);
}

TEST_F(MemoryTests, SharesMemory)
{
    auto a = std::make_unique<WriteOnlyMemory>();
    auto b = std::make_unique<ReadOnlyMemory>(a->memory());
    ASSERT_EQ(a->write(_b(TEST_STRING)), TEST_STRING.size());
    ASSERT_EQ(b->read(_b(test_buffer)), test_buffer.size());
    ASSERT_EQ(test_buffer, TEST_STRING);
}

class FaultyMemoryTests: public FileTests {
protected:
    ~FaultyMemoryTests() override = default;
};

TEST_F(FaultyMemoryTests, CanReadNormally)
{
    auto mem = std::make_unique<FaultyReadOnlyMemory>();
    mem->read(_b(test_buffer));
}

TEST_F(FaultyMemoryTests, CanWriteNormally)
{
    auto mem = std::make_unique<FaultyWriteOnlyMemory>();
    mem->write(_b(TEST_STRING));
}

TEST_F(FaultyMemoryTests, GeneratesReadFault)
{
    auto mem = std::make_unique<FaultyReadOnlyMemory>();
    auto controls = mem->controls();
    controls.set_read_fault_rate(100);
    ASSERT_THROW(mem->read(_b(test_buffer)), IOError);
}

TEST_F(FaultyMemoryTests, GeneratesWriteFault)
{
    auto mem = std::make_unique<FaultyWriteOnlyMemory>();
    auto controls = mem->controls();
    controls.set_write_fault_rate(100);
    ASSERT_THROW(mem->write(_b(TEST_STRING)), IOError);
}

} // <anonymous>