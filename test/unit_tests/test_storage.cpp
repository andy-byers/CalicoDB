#include "calico/storage.h"
#include "tools.h"
#include "unit_tests.h"
#include "utils/types.h"
#include "utils/utils.h"
#include <filesystem>
#include <fstream>
#include <gtest/gtest.h>

namespace Calico {

template<class Base, class Store>
[[nodiscard]]
auto open_blob(Store &storage, const std::string &name) -> std::unique_ptr<Base>
{
    auto s = Status::ok();
    Base *temp {};

    if constexpr (std::is_same_v<RandomReader, Base>) {
        s = storage.open_random_reader(name, &temp);
    } else if constexpr (std::is_same_v<RandomEditor, Base>) {
        s = storage.open_random_editor(name, &temp);
    } else if constexpr (std::is_same_v<Logger, Base>) {
        s = storage.open_logger(name, &temp);
    } else {
        ADD_FAILURE() << "Error: Unexpected blob type";
    }
    EXPECT_TRUE(s.is_ok()) << "Error: " << s.what().data();
    return std::unique_ptr<Base> {temp};
}

auto write_whole_file(const std::string &path, const Slice &message) -> void
{
    std::ofstream ofs {path, std::ios::trunc};
    ofs << message.to_string();
}

[[nodiscard]]
auto read_whole_file(const std::string &path) -> std::string
{
    std::string message;
    std::ifstream ifs {path};
    ifs.seekg(0, std::ios::end);
    message.resize(ifs.tellg());
    ifs.seekg(0, std::ios::beg);
    ifs.read(message.data(), message.size());
    return message;
}

template<class Writer>
constexpr auto write_out_randomly(Tools::RandomGenerator &random, Writer &writer, const Slice &message) -> void
{
    constexpr Size num_chunks {20};
    ASSERT_GT(message.size(), num_chunks) << "File is too small for this test";
    Slice in {message};
    Size counter {};

    while (!in.is_empty()) {
        const auto chunk_size = std::min(in.size(), random.Next<Size>(message.size() / num_chunks));
        auto chunk = in.range(0, chunk_size);

        if constexpr (std::is_base_of_v<Logger, Writer>) {
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
auto read_back_randomly(Tools::RandomGenerator &random, Reader &reader, Size size) -> std::string
{
    static constexpr Size num_chunks {20};
    EXPECT_GT(size, num_chunks) << "File is too small for this test";
    std::string backing(size, '\x00');
    Span out {backing};
    Size counter {};

    while (!out.is_empty()) {
        const auto chunk_size = std::min(out.size(), random.Next<Size>(size / num_chunks));
        auto chunk = out.range(0, chunk_size);
        Size read_size = chunk.size();
        const auto s = reader.read(chunk.data(), read_size, counter);

        if (read_size != chunk_size) {
            return backing;
        }

        EXPECT_TRUE(s.is_ok()) << "Error: " << s.what().data();
        EXPECT_EQ(chunk.size(), chunk_size);
        out.advance(chunk_size);
        counter += chunk_size;
    }
    EXPECT_TRUE(out.is_empty());
    return backing;
}

class FileTests: public OnDiskTest {
public:
    FileTests()
        : filename {PREFIX + std::string {"file"}}
    {}

    ~FileTests() override = default;

    std::string filename;
    Tools::RandomGenerator random;
};

class RandomFileReaderTests: public FileTests {
public:
    RandomFileReaderTests()
    {
        write_whole_file(filename, "");
        file = open_blob<RandomReader>(*storage, filename);
    }

    std::unique_ptr<RandomReader> file;
};

TEST_F(RandomFileReaderTests, NewFileIsEmpty)
{
    std::string backing(8, '\x00');
    Span bytes {backing};
    auto read_size = bytes.size();
    ASSERT_TRUE(file->read(bytes.data(), read_size, 0).is_ok());
    ASSERT_EQ(read_size, 0);
}

TEST_F(RandomFileReaderTests, ReadsBackContents)
{
    auto data = random.Generate(500);
    write_whole_file(filename, data);
    ASSERT_EQ(read_back_randomly(random, *file, data.size()), data);
}

class RandomFileEditorTests: public FileTests {
public:
    RandomFileEditorTests()
        : file {open_blob<RandomEditor>(*storage, filename)}
    {}

    std::unique_ptr<RandomEditor> file;
};

TEST_F(RandomFileEditorTests, NewFileIsEmpty)
{
    std::string backing(8, '\x00');
    Span bytes {backing};
    auto read_size = bytes.size();
    ASSERT_TRUE(file->read(bytes.data(), read_size, 0).is_ok());
    ASSERT_EQ(read_size, 0);
}

TEST_F(RandomFileEditorTests, WritesOutAndReadsBackData)
{
    auto data = random.Generate(500);
    write_out_randomly(random, *file, data);
    ASSERT_EQ(read_back_randomly(random, *file, data.size()), data);
}

class AppendFileWriterTests: public FileTests {
public:
    AppendFileWriterTests()
        : file {open_blob<Logger>(*storage, filename)}
    {}

    std::unique_ptr<Logger> file;
};

TEST_F(AppendFileWriterTests, WritesOutData)
{
    auto data = random.Generate(500);
    write_out_randomly<Logger>(random, *file, data);
    ASSERT_EQ(read_whole_file(filename), data.to_string());
}

class PosixStorageTests: public OnDiskTest {
public:
    PosixStorageTests()
        : filename {PREFIX + std::string {"file"}}
    {}

    ~PosixStorageTests() override = default;

    std::string filename;
    Tools::RandomGenerator random;
};

class DynamicStorageTests : public InMemoryTest {
public:
    DynamicStorageTests()
        : filename {PREFIX + std::string {"file"}}
    {}

    ~DynamicStorageTests() override = default;

    std::string filename;
    Tools::RandomGenerator random;
};

TEST_F(DynamicStorageTests, ReaderCannotCreateFile)
{
    RandomReader *temp;
    const auto s = storage->open_random_reader("nonexistent", &temp);
    ASSERT_TRUE(s.is_not_found()) << "Error: " << s.what().data();
}

TEST_F(DynamicStorageTests, ReadsAndWrites)
{
    auto ra_editor = open_blob<RandomEditor>(*storage, filename);
    auto ra_reader = open_blob<RandomReader>(*storage, filename);
    auto ap_writer = open_blob<Logger>(*storage, filename);

    const auto first_input = random.Generate(500);
    const auto second_input = random.Generate(500);
    write_out_randomly(random, *ra_editor, first_input);
    write_out_randomly(random, *ap_writer, second_input);
    const auto output_1 = read_back_randomly(random, *ra_reader, 1'000);
    const auto output_2 = read_back_randomly(random, *ra_editor, 1'000);
    ASSERT_EQ(output_1, output_2);
    ASSERT_EQ(output_1, first_input.to_string() + second_input.to_string());
}

TEST_F(DynamicStorageTests, ReaderStopsAtEOF)
{
    auto ra_editor = open_blob<RandomEditor>(*storage, filename);
    auto ra_reader = open_blob<RandomReader>(*storage, filename);

    const auto data = random.Generate(500);
    write_out_randomly(random, *ra_editor, data);

    std::string buffer(data.size() * 2, '\x00');
    Span bytes {buffer};
    auto read_size = bytes.size();
    ASSERT_OK(ra_reader->read(bytes.data(), read_size, 0));
    ASSERT_EQ(bytes.truncate(read_size), data);
}

} // namespace Calico