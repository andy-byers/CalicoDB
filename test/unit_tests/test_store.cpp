#include <filesystem>
#include <fstream>
#include <fcntl.h>
#include <unistd.h>
#include <gtest/gtest.h>
#include "calico/storage.h"
#include "random.h"
#include "storage/posix_storage.h"
#include "storage/posix_system.h"
#include "fakes.h"
#include "unit_tests.h"

namespace calico {

namespace internal {
    extern std::uint32_t random_seed;
} // namespace internal

template<class Base, class Store>
[[nodiscard]]
auto open_blob(Store &store, const std::string &name) -> std::unique_ptr<Base>
{
    auto s = Status::ok();
    Base *temp {};

    if constexpr (std::is_same_v<RandomReader, Base>) {
        s = store.open_random_reader(name, &temp);
    } else if constexpr (std::is_same_v<RandomEditor, Base>) {
        s = store.open_random_editor(name, &temp);
    } else if constexpr (std::is_same_v<AppendWriter, Base>) {
        s = store.open_append_writer(name, &temp);
    } else {
        ADD_FAILURE() << "Error: Unexpected blob type";
    }
    EXPECT_TRUE(s.is_ok()) << "Error: " << s.what();
    return std::unique_ptr<Base> {temp};
}

auto write_whole_file(const std::string &path, const std::string &message) -> void
{
    std::ofstream ofs {path, std::ios::trunc};
    ofs << message;
}

[[nodiscard]]
auto read_whole_file(const std::string &path) -> std::string
{
    std::string message;
    std::ifstream ifs {path};
    ifs >> message;
    return message;
}

template<class Writer>
constexpr auto write_out_randomly(Random &random, Writer &writer, const std::string &message) -> void
{
    constexpr Size num_chunks {20};
    ASSERT_GT(message.size(), num_chunks) << "File is too small for this test";
    auto in = stob(message);
    Size counter {};

    while (!in.is_empty()) {
        const auto chunk_size = std::min(in.size(), random.get(message.size() / num_chunks));
        auto chunk = in.copy().truncate(chunk_size);

        if constexpr (std::is_same_v<AppendWriter, Writer>) {
            ASSERT_TRUE(writer.write(chunk).is_ok());
        } else {
            ASSERT_TRUE(writer.write(chunk, counter).is_ok());
            counter += chunk_size;
        }
        in.advance(chunk_size);
    }
    ASSERT_TRUE(in.is_empty());
}

template<class Reader>
[[nodiscard]]
auto read_back_randomly(Random &random, Reader &reader, Size size) -> std::string
{
    static constexpr Size num_chunks {20};
    EXPECT_GT(size, num_chunks) << "File is too small for this test";
    std::string backing(size, '\x00');
    Bytes out {backing};
    Size counter {};

    while (!out.is_empty()) {
        const auto chunk_size = std::min(out.size(), random.get(size / num_chunks));
        auto chunk = out.copy().truncate(chunk_size);
        const auto s = reader.read(chunk, counter);

        if (chunk.size() < chunk_size)
            return backing;

        EXPECT_TRUE(s.is_ok()) << "Error: " << s.what();
        EXPECT_EQ(chunk.size(), chunk_size);
        out.advance(chunk_size);
        counter += chunk_size;
    }
    EXPECT_TRUE(out.is_empty());
    return backing;
}

constexpr auto HOME = "/tmp/calico_test_files";
constexpr auto PATH = "/tmp/calico_test_files/name";

class FileTests: public testing::Test {
public:
    FileTests()
        : storage {std::make_unique<PosixStorage>()}
    {
        std::error_code ignore;
        std::filesystem::remove_all(HOME, ignore);

        const auto s = storage->create_directory(HOME);
        EXPECT_TRUE(s.is_ok());
    }

    ~FileTests() override
    {
        std::error_code ignore;
        std::filesystem::remove_all(HOME, ignore);
    }

    std::unique_ptr<Storage> storage;
    Random random {internal::random_seed};
};

class RandomFileReaderTests: public FileTests {
public:
    RandomFileReaderTests()
    {
        write_whole_file(PATH, "");
        file = open_blob<RandomReader>(*storage, PATH);
    }

    std::unique_ptr<RandomReader> file;
};

TEST_F(RandomFileReaderTests, NewFileIsEmpty)
{
    std::string backing(8, '\x00');
    auto bytes = stob(backing);
    ASSERT_TRUE(file->read(bytes, 0).is_ok());
    ASSERT_TRUE(bytes.is_empty());
}

TEST_F(RandomFileReaderTests, ReadsBackContents)
{
    auto data = random.get<std::string>('a', 'z', 500);
    write_whole_file(PATH, data);
    ASSERT_EQ(read_back_randomly(random, *file, data.size()), data);
}

class RandomFileEditorTests: public FileTests {
public:
    RandomFileEditorTests()
        : file {open_blob<RandomEditor>(*storage, PATH)}
    {}

    std::unique_ptr<RandomEditor> file;
};

TEST_F(RandomFileEditorTests, NewFileIsEmpty)
{
    std::string backing(8, '\x00');
    auto bytes = stob(backing);
    ASSERT_TRUE(file->read(bytes, 0).is_ok());
    ASSERT_TRUE(bytes.is_empty());
}

TEST_F(RandomFileEditorTests, WritesOutAndReadsBackData)
{
    auto data = random.get<std::string>('a', 'z', 500);
    write_out_randomly(random, *file, data);
    ASSERT_EQ(read_back_randomly(random, *file, data.size()), data);
}

class AppendFileWriterTests: public FileTests {
public:
    AppendFileWriterTests()
        : file {open_blob<AppendWriter>(*storage, PATH)}
    {}

    std::unique_ptr<AppendWriter> file;
};

TEST_F(AppendFileWriterTests, WritesOutData)
{
    auto data = random.get<std::string>('a', 'z', 500);
    write_out_randomly<AppendWriter>(random, *file, data);
    ASSERT_EQ(read_whole_file(PATH), data);
}

class PosixStorageTests: public testing::Test {
public:
    PosixStorageTests() = default;

    ~PosixStorageTests() override = default;

    PosixStorage storage;
    Random random {internal::random_seed};
};

class HeapTests: public testing::Test {
public:
    HeapTests()
        : storage {std::make_unique<HeapStorage>()}
    {
        const auto s = storage->create_directory(HOME);
        EXPECT_TRUE(s.is_ok());
    }

    ~HeapTests() override = default;

    std::unique_ptr<Storage> storage;
    Random random {internal::random_seed};
};

TEST_F(HeapTests, ReaderCannotCreateBlob)
{
    RandomReader *temp {};
    const auto s = storage->open_random_reader("nonexistent", &temp);
    ASSERT_TRUE(s.is_not_found()) << "Error: " << s.what();
}

TEST_F(HeapTests, ReadsAndWrites)
{
    auto ra_editor = open_blob<RandomEditor>(*storage, PATH);
    auto ra_reader = open_blob<RandomReader>(*storage, PATH);
    auto ap_writer = open_blob<AppendWriter>(*storage, PATH);

    const auto first_input = random.get<std::string>('a', 'z', 500);
    const auto second_input = random.get<std::string>('a', 'z', 500);
    write_out_randomly(random, *ra_editor, first_input);
    write_out_randomly(random, *ap_writer, second_input);
    const auto output_1 = read_back_randomly(random, *ra_reader, 1'000);
    const auto output_2 = read_back_randomly(random, *ra_editor, 1'000);
    ASSERT_EQ(output_1, output_2);
    ASSERT_EQ(output_1, first_input + second_input);
}

TEST_F(HeapTests, ReaderStopsAtEOF)
{
    auto ra_editor = open_blob<RandomEditor>(*storage, PATH);
    auto ra_reader = open_blob<RandomReader>(*storage, PATH);

    const auto data = random.get<std::string>('a', 'z', 500);
    write_out_randomly(random, *ra_editor, data);

    std::string buffer(data.size() * 2, '\x00');
    auto bytes = stob(buffer);
    ASSERT_OK(ra_reader->read(bytes, 0));
    ASSERT_EQ(bytes.to_string(), data);
}

TEST(SystemTests, SystemErrorBehavior)
{
    errno = ENOENT;
    ASSERT_TRUE(system::error().is_system_error());
    ASSERT_EQ(errno, 0);

    ASSERT_TRUE(system::error(std::errc::no_such_file_or_directory).is_system_error());
    assert_error_42(system::error("42"));
}

TEST(SystemTests, ClosedFileErrors)
{
    char backing[1];
    Bytes bytes {backing, sizeof(backing)};
    ASSERT_TRUE(system::file_read(-1, bytes).error().is_system_error());
    ASSERT_TRUE(system::file_write(-1, bytes).error().is_system_error());
    ASSERT_TRUE(system::file_seek(-1, 0, SEEK_CUR).error().is_system_error());
    ASSERT_TRUE(system::file_close(-1).is_system_error());
    ASSERT_TRUE(system::file_sync(-1).is_system_error());
}

TEST(SystemTests, NonexistentResourceErrors)
{
    ASSERT_TRUE(system::file_size("__does_not_exist__").error().is_system_error());
    ASSERT_TRUE(system::file_remove("__does_not_exist__").is_system_error());
    ASSERT_TRUE(system::file_resize("__does_not_exist__", 0).is_system_error());
    ASSERT_TRUE(system::dir_remove("__does_not_exist__").is_system_error());
}

TEST(SystemTests, OpenAndClose)
{
    static constexpr auto PATH = "/tmp/__calico_system_tests";
    auto fd = system::file_open(PATH, O_CREAT | O_RDWR, 0666).value();
    ASSERT_OK(system::file_close(fd));
    ASSERT_OK(system::file_exists(PATH));
    ASSERT_OK(system::file_remove(PATH));
    ASSERT_TRUE(system::file_exists(PATH).is_not_found());
}

} // namespace calico