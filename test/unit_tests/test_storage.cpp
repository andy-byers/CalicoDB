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

using namespace calico;

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
    auto writer = file.open_writer();

    // Write out the string in random-sized chunks.
    while (!out.is_empty()) {
        const auto chunk_size = random.next_int(out.size());
        ASSERT_TRUE(write_all(*writer, out.range(0, chunk_size)));
        out.advance(chunk_size);
    }
    auto seek_result = writer->seek(0, Seek::BEGIN);
    ASSERT_TRUE(seek_result && not seek_result.value());

    std::string payload_in(payload_size, '\x00');
    auto in = stob(payload_in);
    auto reader = file.open_reader();

    // Read back the string in random-sized chunks.
    while (!in.is_empty()) {
        const auto chunk_size = random.next_int(in.size());
        ASSERT_TRUE(read_exact(*reader, in.range(0, chunk_size)));
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
        auto file = std::make_unique<File>();
        EXPECT_TRUE(file->open(name, mode, 0666));
        return file;
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
    ASSERT_EQ(file->name(), std::filesystem::path {PATH}.filename());
    ASSERT_EQ(file->mode(), mode);
    ASSERT_EQ(file->permissions(), 0666);
}

TEST_F(FileTests, ExistsAfterClose)
{
    // File is closed in the destructor.
    open(PATH, Mode::CREATE);
    ASSERT_TRUE(std::filesystem::exists(PATH));
}

TEST_F(FileTests, RenameReplacesOldName)
{
    // File is closed in the destructor.
    auto file = open(PATH, Mode::CREATE);
    ASSERT_TRUE(file->rename(PATH + "_new"));
    ASSERT_FALSE(std::filesystem::exists(PATH));
    ASSERT_TRUE(std::filesystem::exists(PATH + "_new"));
}

TEST_F(FileTests, ReadFromFile)
{
    {
        auto ofs = std::ofstream{PATH};
        ofs << TEST_STRING;
    }
    auto file = open(PATH, Mode::READ_ONLY);
    read_exact_string(*file->open_reader(), test_buffer);
    ASSERT_EQ(test_buffer, TEST_STRING);
}

TEST_F(FileTests, WriteToFile)
{
    auto file = open(PATH, Mode::WRITE_ONLY | Mode::CREATE | Mode::TRUNCATE);
    auto writer = file->open_writer();
    write_exact_string(*writer, TEST_STRING);
    ASSERT_TRUE(writer->sync());
    auto ifs = std::ifstream{PATH};
    std::string result;
    ifs >> result;
    ASSERT_EQ(result, TEST_STRING);
    ASSERT_EQ(file->size(), result.size());
}

TEST_F(FileTests, PositionedReadsAndWrites)
{
    auto file = open(PATH, Mode::READ_WRITE | Mode::CREATE);
    write_exact_string(*file->open_writer(), "!", 12);
    write_exact_string(*file->open_writer(), "world", 7);
    write_exact_string(*file->open_writer(), "Hello, ", 0);
    std::string buffer(13, '\x00');

    auto reader = file->open_reader();
    ASSERT_TRUE(read_exact_at(*reader, stob(buffer).advance(12), 12));
    ASSERT_TRUE(read_exact_at(*reader, stob(buffer).range(6, 6), 6));
    ASSERT_TRUE(read_exact_at(*reader, stob(buffer).truncate(7), 0));
    ASSERT_EQ(buffer, "Hello, world!");
}

TEST_F(FileTests, ExactReadsFailIfNotEnoughData)
{
    auto file = open(PATH, Mode::READ_WRITE | Mode::CREATE);
    write_exact_string(*file->open_writer(), "Hello, world!");
    std::string buffer(100, '\x00');
    ASSERT_FALSE(read_exact(*file->open_reader(), stob(buffer)));
}

TEST_F(FileTests, ReportsEOFDuringRead)
{
    auto file = open(PATH, Mode::CREATE | Mode::READ_WRITE | Mode::TRUNCATE);
    write_exact_string(*file->open_writer(), TEST_STRING);
    auto reader = file->open_reader();
    ASSERT_TRUE(reader->seek(0, Seek::BEGIN));
    test_buffer.resize(test_buffer.size() * 2);
    // Try to read past EOF.
    auto result = reader->read(stob(test_buffer));
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
        : file {std::make_unique<File>()}
    {
        EXPECT_TRUE(file->open(PATH, Mode::READ_WRITE | Mode::CREATE | Mode::TRUNCATE, 0666));
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
    File file;
    ASSERT_FALSE(file.open(PATH, Mode::CREATE | Mode::EXCLUSIVE, 0666));
}

TEST_F(FileFailureTests, FailsWhenFileDoesNotExistButShould)
{
    ASSERT_TRUE(file->remove());
    ASSERT_TRUE(file->close());

    File file;
    ASSERT_FALSE(file.open(PATH, Mode {}, 0666));
}

TEST_F(FileFailureTests, FailsWhenReadSizeIsTooLarge)
{
    auto reader = file->open_reader();
    ASSERT_TRUE(reader->read(large_slice).error().is_system_error());
}

TEST_F(FileFailureTests, FailsWhenWriteSizeIsTooLarge)
{
    auto writer = file->open_writer();
    ASSERT_TRUE(writer->write(large_slice).error().is_system_error());
}

TEST_F(FileFailureTests, FailsWhenSeekOffsetIsTooLarge)
{
    auto reader = file->open_reader();
    ASSERT_TRUE(reader->seek(static_cast<long>(OVERFLOW_SIZE), Seek::BEGIN).error().is_system_error());
}

TEST_F(FileFailureTests, FailsWhenNewSizeIsTooLarge)
{
    auto writer = file->open_writer();
    ASSERT_TRUE(writer->resize(OVERFLOW_SIZE).error().is_system_error());
}

TEST_F(FileTests, FailsWhenNewNameIsTooLong)
{
    // File is closed in the destructor.
    auto file = open(PATH, Mode::CREATE);
    ASSERT_TRUE(file->rename(std::string(PATH_MAX + 1, 'x')).error().is_system_error());
}

TEST_F(FileTests, FailsWhenNewNameIsEmpty)
{
    auto file = open(PATH, Mode::CREATE);
    ASSERT_TRUE(file->rename("").error().is_system_error());
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