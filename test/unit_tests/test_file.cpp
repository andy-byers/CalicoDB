#include <filesystem>
#include <fstream>
#include <limits>

#include <gtest/gtest.h>

#include "calico/options.h"
#include "random.h"
#include "storage/file.h"
#include "storage/interface.h"
#include "unit_tests.h"

namespace {

using namespace calico;

constexpr auto TEST_STRING = "TEST_STRING";

template<class Reader>
auto read_string(Reader &&reader, std::string &buffer) -> Result<Size>
{
    return reader.noex_read(stob(buffer));
}

template<class Reader>
auto read_exact_string(Reader &&reader, std::string &buffer) -> Result<void>
{
    return noex_read_exact(std::forward<Reader>(reader), stob(buffer));
}

template<class Writer>
auto write_string(Writer &writer, const std::string &buffer) -> Result<Size>
{
    return writer.noex_write(stob(buffer));
}

template<class Writer>
auto write_exact_string(Writer &&writer, const std::string &buffer) -> Result<void>
{
    return noex_write_all(std::forward<Writer>(writer), stob(buffer));
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
        ASSERT_TRUE(noex_write_all(*writer, out.range(0, chunk_size)));
        out.advance(chunk_size);
    }
    auto seek_result = writer->noex_seek(0, Seek::BEGIN);
    ASSERT_TRUE(seek_result && not seek_result.value());

    std::string payload_in(payload_size, '\x00');
    auto in = stob(payload_in);
    auto reader = file.open_reader();

    // Read back the string in random-sized chunks.
    while (!in.is_empty()) {
        const auto chunk_size = random.next_int(in.size());
        ASSERT_TRUE(noex_read_exact(*reader, in.range(0, chunk_size)));
        in.advance(chunk_size);
    }
    ASSERT_EQ(payload_in, payload_out);
}

class FileTests: public testing::Test {
public:
    const std::string PATH = "/tmp/calico_test_file";

    FileTests()
        : test_buffer(strlen(TEST_STRING), '\x00') {}

    ~FileTests() override = default;

    static auto open(const std::string &name, Mode mode) -> std::unique_ptr<IFile>
    {
        auto file = std::make_unique<File>();
        EXPECT_TRUE(file->noex_open(name, mode, 0666));
        return file;
    }

    std::string test_buffer;
};

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
    read_exact_string(*file->open_reader(), test_buffer);
    ASSERT_EQ(test_buffer, TEST_STRING);
}

TEST_F(FileTests, WriteToFile)
{
    auto file = open(PATH, Mode::WRITE_ONLY | Mode::CREATE | Mode::TRUNCATE);
    write_string(*file->open_writer(), TEST_STRING);
    auto ifs = std::ifstream{PATH};
    std::string result;
    ifs >> result;
    ASSERT_EQ(result, TEST_STRING);
}

TEST_F(FileTests, ReportsEOFDuringRead)
{
    auto file = open(PATH, Mode::CREATE | Mode::READ_WRITE | Mode::TRUNCATE);
    write_exact_string(*file->open_writer(), TEST_STRING);
    auto reader = file->open_reader();
    ASSERT_TRUE(reader->noex_seek(0, Seek::BEGIN));
    test_buffer.resize(test_buffer.size() * 2);
    // Try to read past EOF.
    auto result = read_string(*reader, test_buffer);
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
        EXPECT_TRUE(file->noex_open(PATH, Mode::READ_WRITE | Mode::CREATE | Mode::TRUNCATE, 0666));
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
    ASSERT_FALSE(file.noex_open(PATH, Mode::CREATE | Mode::EXCLUSIVE, 0666));
}

TEST_F(FileFailureTests, FailsWhenFileDoesNotExistButShould)
{
    ASSERT_TRUE(file->noex_remove());
    ASSERT_TRUE(file->noex_close());

    File file;
    ASSERT_FALSE(file.noex_open(PATH, Mode {}, 0666));
}

TEST_F(FileFailureTests, FailsWhenReadSizeIsTooLarge)
{
    auto reader = file->open_reader();
    ASSERT_FALSE(reader->noex_read(large_slice));
}

TEST_F(FileFailureTests, FailsWhenWriteSizeIsTooLarge)
{
    auto writer = file->open_writer();
    ASSERT_FALSE(writer->noex_write(large_slice));
}

TEST_F(FileFailureTests, FailsWhenSeekOffsetIsTooLarge)
{
    auto reader = file->open_reader();
    ASSERT_FALSE(reader->noex_seek(static_cast<long>(OVERFLOW_SIZE), Seek::BEGIN));
}

TEST_F(FileFailureTests, FailsWhenNewSizeIsTooLarge)
{
    auto writer = file->open_writer();
    ASSERT_FALSE(writer->noex_resize(OVERFLOW_SIZE));
}

} // <anonymous>