#include <filesystem>
#include <fstream>
#include <limits>
#include <climits>
#include <gtest/gtest.h>

#include "calico/options.h"
#include "random.h"
#include "storage/file.h"
#include "storage/interface.h"
#include "storage/system.h"
#include "unit_tests.h"

namespace {

using namespace cco;

constexpr auto TEST_STRING = "TEST_STRING";

template<class Reader>
auto read_exact_string(Reader &&reader, std::string &buffer) -> Result<void>
{
    return read_exact(std::forward<Reader>(reader), stob(buffer));
}

template<class Reader>
auto read_exact_string(Reader &&reader, std::string &buffer, Index offset) -> Result<void>
{
    return read_exact_at(std::forward<Reader>(reader), stob(buffer), offset);
}

template<class Writer>
auto write_string(Writer &writer, const std::string &buffer) -> Result<Size>
{
    return writer.write(stob(buffer));
}

template<class Writer>
auto write_exact_string(Writer &&writer, const std::string &buffer) -> Result<void>
{
    return write_all(std::forward<Writer>(writer), stob(buffer));
}

template<class Writer>
auto write_exact_string(Writer &&writer, const std::string &buffer, Index offset) -> Result<void>
{
    return write_all(std::forward<Writer>(writer), stob(buffer), offset);
}

[[maybe_unused]] auto test_random_reads_and_writes(IFile &file) -> void
{
    static constexpr Size payload_size {1'000};
    Random random {0};
    const auto payload_out = random.next_string(payload_size);
    auto out = stob(payload_out);

    // Write out the string in random-sized chunks.
    while (!out.is_empty()) {
        const auto chunk_size = random.next_int(out.size());
        ASSERT_TRUE(write_all(file, out.range(0, chunk_size)));
        out.advance(chunk_size);
    }
    auto seek_result = file.seek(0, Seek::BEGIN);
    ASSERT_TRUE(seek_result && not seek_result.value());

    std::string payload_in(payload_size, '\x00');
    auto in = stob(payload_in);

    // Read back the string in random-sized chunks.
    while (!in.is_empty()) {
        const auto chunk_size = random.next_int(in.size());
        ASSERT_TRUE(read_exact(file, in.range(0, chunk_size)));
        in.advance(chunk_size);
    }
    ASSERT_EQ(payload_in, payload_out);
}

class FileTests: public testing::Test {
public:
    const std::string PATH = "/tmp/calico_test_file";

    FileTests()
        : test_buffer(strlen(TEST_STRING), '\x00') {}

    ~FileTests() override
    {
        std::filesystem::remove(PATH);
    }

    static auto open(const std::string &name, Mode mode) -> std::unique_ptr<IFile>
    {
        auto fd = *system::open(name, static_cast<int>(mode), 0666);
        return std::make_unique<File>(fd, mode, name);
    }

    std::string test_buffer;
};

TEST_F(FileTests, NewFileIsEmpty)
{
    ASSERT_EQ(open(PATH, Mode::CREATE)->size().value(), 0);
}

TEST_F(FileTests, StoresFileInformation)
{
    // File is closed in the destructor.
    const auto mode = Mode::CREATE | Mode::READ_WRITE | Mode::APPEND;
    auto file = open(PATH, mode);
    ASSERT_EQ(file->name(), PATH);
    ASSERT_EQ(file->mode(), mode);
}

TEST_F(FileTests, ExistsAfterClose)
{
    // File is closed in the destructor.
    open(PATH, Mode::CREATE);
    ASSERT_TRUE(std::filesystem::exists(PATH));
}

TEST_F(FileTests, ReadFromFile)
{
    {
        auto ofs = std::ofstream{PATH};
        ofs << TEST_STRING;
    }
    auto file = open(PATH, Mode::READ_ONLY);
    read_exact_string(*file, test_buffer);
    ASSERT_EQ(test_buffer, TEST_STRING);
}

TEST_F(FileTests, WriteToFile)
{
    auto file = open(PATH, Mode::WRITE_ONLY | Mode::CREATE | Mode::TRUNCATE);
    write_exact_string(*file, TEST_STRING);
    ASSERT_TRUE(file->sync());
    auto ifs = std::ifstream{PATH};
    std::string result;
    ifs >> result;
    ASSERT_EQ(result, TEST_STRING);
    ASSERT_EQ(file->size(), result.size());
}

TEST_F(FileTests, PositionedReadsAndWrites)
{
    auto file = open(PATH, Mode::READ_WRITE | Mode::CREATE);
    write_exact_string(*file, "!", 12);
    write_exact_string(*file, "world", 7);
    write_exact_string(*file, "Hello, ", 0);
    std::string buffer(13, '\x00');

    ASSERT_TRUE(read_exact_at(*file, stob(buffer).advance(12), 12));
    ASSERT_TRUE(read_exact_at(*file, stob(buffer).range(6, 6), 6));
    ASSERT_TRUE(read_exact_at(*file, stob(buffer).truncate(7), 0));
    ASSERT_EQ(buffer, "Hello, world!");
}

TEST_F(FileTests, ExactReadsFailIfNotEnoughData)
{
    auto file = open(PATH, Mode::READ_WRITE | Mode::CREATE);
    write_exact_string(*file, "Hello, world!");
    std::string buffer(100, '\x00');
    ASSERT_FALSE(read_exact(*file, stob(buffer)));
}

TEST_F(FileTests, ReportsEOFDuringRead)
{
    auto file = open(PATH, Mode::CREATE | Mode::READ_WRITE | Mode::TRUNCATE);
    write_exact_string(*file, TEST_STRING);
    ASSERT_TRUE(file->seek(0, Seek::BEGIN));
    test_buffer.resize(test_buffer.size() * 2);
    // Try to read past EOF.
    auto result = file->read(stob(test_buffer));
    ASSERT_TRUE(result && result.value() == strlen(TEST_STRING));
    test_buffer.resize(strlen(TEST_STRING));
    ASSERT_EQ(test_buffer, TEST_STRING);
}

TEST_F(FileTests, RandomReadsAndWrites)
{
    auto file = open(PATH, Mode::READ_WRITE | Mode::CREATE | Mode::TRUNCATE);
    test_random_reads_and_writes(*file);
}

class FileFailureTests: public testing::Test {
public:
    static constexpr auto PATH = "/tmp/calico_file_failure";
    static constexpr Size OVERFLOW_SIZE {std::numeric_limits<Size>::max()};

    FileFailureTests()
    {
        const auto mode = Mode::READ_WRITE | Mode::CREATE | Mode::TRUNCATE;
        const auto fd = *system::open(PATH, static_cast<int>(mode), 0666);
        file = std::make_unique<File>(fd, mode, PATH);
    }

    ~FileFailureTests() override
    {
        std::filesystem::remove(PATH);
    }

    std::unique_ptr<IFile> file;
    Byte *fake_ptr {reinterpret_cast<Byte*>(123)};
    Bytes large_slice {fake_ptr, OVERFLOW_SIZE};
};

TEST_F(FileFailureTests, FailsWhenFileExistsButShouldNot)
{
    ASSERT_FALSE(system::open(PATH, static_cast<int>(Mode::CREATE | Mode::EXCLUSIVE), 0666));
}

TEST_F(FileFailureTests, FailsWhenFileDoesNotExistButShould)
{
    ASSERT_TRUE(system::unlink(PATH));
    ASSERT_TRUE(file->close());

    ASSERT_FALSE(system::open(PATH, O_RDONLY, 0666));
}

TEST_F(FileFailureTests, FailsWhenReadSizeIsTooLarge)
{
    ASSERT_TRUE(file->read(large_slice).error().is_system_error());
}

TEST_F(FileFailureTests, FailsWhenWriteSizeIsTooLarge)
{
    ASSERT_TRUE(file->write(large_slice).error().is_system_error());
}

TEST_F(FileFailureTests, FailsWhenSeekOffsetIsTooLarge)
{
    ASSERT_TRUE(file->seek(static_cast<long>(OVERFLOW_SIZE), Seek::BEGIN).error().is_system_error());
}

TEST_F(FileFailureTests, FailsWhenNewSizeIsTooLarge)
{
    ASSERT_TRUE(file->resize(OVERFLOW_SIZE).error().is_system_error());
}

TEST_F(FileTests, CannotCloseFileTwice)
{
    auto file = open(PATH, Mode::CREATE);
    ASSERT_TRUE(file->close());
    ASSERT_FALSE(file->close());
}

TEST(SystemTests, OperationsFailOnInvalidHandle)
{
    static constexpr auto fd {123'456'789};

    std::string buffer(13, '\x00');
    ASSERT_FALSE(system::read(fd, stob(buffer)));
    ASSERT_FALSE(system::write(fd, stob(buffer)));
    ASSERT_FALSE(system::seek(fd, 123, static_cast<int>(Seek::BEGIN)));
    ASSERT_FALSE(system::seek(fd, 123, static_cast<int>(Seek::BEGIN)));
    ASSERT_FALSE(system::sync(fd));
}

TEST(SystemTests, CannotUnlinkNonexistentFile)
{
    static constexpr auto nonexistent = "/tmp/calico_should_not_exist__";
    std::error_code error;
    std::filesystem::remove(nonexistent, error);
    ASSERT_FALSE(system::unlink(nonexistent));
    ASSERT_FALSE(system::unlink(""));
}

} // <anonymous>