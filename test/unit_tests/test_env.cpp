// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#include "calicodb/env.h"
#include "tools.h"
#include "unit_tests.h"
#include "utils.h"
#include <filesystem>
#include <fstream>
#include <gtest/gtest.h>

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

template <class Base, class Store>
[[nodiscard]] auto open_blob(Store &env, const std::string &name) -> std::unique_ptr<Base>
{
    auto s = Status::ok();
    Base *temp = nullptr;

    if constexpr (std::is_same_v<Reader, Base>) {
        s = env.new_reader(name, temp);
    } else if constexpr (std::is_same_v<Editor, Base>) {
        s = env.new_editor(name, temp);
    } else if constexpr (std::is_same_v<Logger, Base>) {
        s = env.new_logger(name, temp);
    } else {
        ADD_FAILURE() << "Error: Unexpected blob type";
    }
    EXPECT_TRUE(s.is_ok()) << "Error: " << s.to_string().data();
    return std::unique_ptr<Base> {temp};
}

auto write_whole_file(const std::string &path, const Slice &message) -> void
{
    std::ofstream ofs {path, std::ios::trunc};
    ofs << message.to_string();
}

[[nodiscard]] auto read_whole_file(const std::string &path) -> std::string
{
    std::string message;
    std::ifstream ifs {path};
    ifs.seekg(0, std::ios::end);
    message.resize(ifs.tellg());
    ifs.seekg(0, std::ios::beg);
    ifs.read(message.data(), message.size());
    return message;
}

template <class Writer>
constexpr auto write_out_randomly(tools::RandomGenerator &random, Writer &writer, const Slice &message) -> void
{
    constexpr std::size_t num_chunks = 20;
    ASSERT_GT(message.size(), num_chunks) << "File is too small for this test";
    Slice in {message};
    std::size_t counter = 0;

    while (!in.is_empty()) {
        const auto chunk_size = std::min<std::size_t>(in.size(), random.Next(message.size() / num_chunks));
        auto chunk = in.range(0, chunk_size);

        if constexpr (std::is_base_of_v<Logger, Writer>) {
            ASSERT_TRUE(writer.write(chunk).is_ok());
        } else {
            ASSERT_TRUE(writer.write(counter, chunk).is_ok());
            counter += chunk_size;
        }
        in.advance(chunk_size);
    }
    ASSERT_TRUE(in.is_empty());
}

template <class Reader>
[[nodiscard]] auto read_back_randomly(tools::RandomGenerator &random, Reader &reader, std::size_t size) -> std::string
{
    static constexpr std::size_t num_chunks = 20;
    EXPECT_GT(size, num_chunks) << "File is too small for this test";
    std::string backing(size, '\x00');
    auto *out_data = backing.data();
    std::size_t counter = 0;

    while (counter < size) {
        const auto chunk_size = std::min<std::size_t>(size - counter, random.Next(size / num_chunks));
        Slice slice;
        const auto s = reader.read(counter, chunk_size, out_data, &slice);

        if (slice.size() != chunk_size) {
            return backing;
        }

        EXPECT_TRUE(s.is_ok()) << "Error: " << s.to_string().data();
        EXPECT_EQ(chunk_size, chunk_size);
        out_data += chunk_size;
        counter += chunk_size;
    }
    return backing;
}

class FileTests
    : public OnDiskTest,
      public testing::Test
{
public:
    ~FileTests() override = default;

    tools::RandomGenerator random;
};

class PosixInfoLoggerTests : public FileTests
{
public:
    PosixInfoLoggerTests()
    {
        std::filesystem::remove_all(filename);

        InfoLogger *temp;
        EXPECT_OK(env->new_info_logger(filename, temp));
        file.reset(temp);
    }

    std::string filename {"__test_info_logger"};
    std::unique_ptr<InfoLogger> file;
};

TEST_F(PosixInfoLoggerTests, WritesFormattedText)
{
    file->logv("test %03d %.03f %s\n", 12, 0.21f, "abc");
    ASSERT_EQ("test 012 0.210 abc\n", read_whole_file(filename));
}

TEST_F(PosixInfoLoggerTests, AddsNewline)
{
    file->logv("test");
    ASSERT_EQ("test\n", read_whole_file(filename));
}

TEST_F(PosixInfoLoggerTests, ResizesBuffer)
{
    const std::string message(512 * 10, 'x');
    file->logv("%s", message.c_str());
    ASSERT_EQ(message + '\n', read_whole_file(filename));
}

class PosixReaderTests : public FileTests
{
public:
    PosixReaderTests()
    {
        write_whole_file(kFilename, "");
        file = open_blob<Reader>(*env, kFilename);
    }

    std::unique_ptr<Reader> file;
};

TEST_F(PosixReaderTests, NewFileIsEmpty)
{
    std::string backing(8, '\x00');
    Slice slice;
    ASSERT_TRUE(file->read(0, 8, backing.data(), &slice).is_ok());
    ASSERT_EQ(slice.size(), 0);
}

TEST_F(PosixReaderTests, ReadsBackContents)
{
    auto data = random.Generate(500);
    write_whole_file(kFilename, data);
    ASSERT_EQ(read_back_randomly(random, *file, data.size()), data);
}

class PosixEditorTests : public FileTests
{
public:
    PosixEditorTests()
        : file {open_blob<Editor>(*env, kFilename)}
    {
    }

    std::unique_ptr<Editor> file;
};

TEST_F(PosixEditorTests, NewFileIsEmpty)
{
    std::string backing(8, '\x00');
    Slice slice;
    ASSERT_TRUE(file->read(0, 8, backing.data(), &slice).is_ok());
    ASSERT_TRUE(slice.is_empty());
}

TEST_F(PosixEditorTests, WritesOutAndReadsBackData)
{
    auto data = random.Generate(500);
    write_out_randomly(random, *file, data);
    ASSERT_EQ(read_back_randomly(random, *file, data.size()), data);
}

class PosixLoggerTests : public FileTests
{
public:
    PosixLoggerTests()
        : file {open_blob<Logger>(*env, kFilename)}
    {
    }

    std::unique_ptr<Logger> file;
};

TEST_F(PosixLoggerTests, WritesOutData)
{
    auto data = random.Generate(500);
    write_out_randomly<Logger>(random, *file, data);
    ASSERT_EQ(read_whole_file(kFilename), data.to_string());
}

class EnvPosixTests : public OnDiskTest
{
public:
    EnvPosixTests()
    {
    }

    ~EnvPosixTests() override = default;

    tools::RandomGenerator random;
};

class FakeEnvTests
    : public InMemoryTest,
      public testing::Test
{
public:
    ~FakeEnvTests() override = default;

    tools::RandomGenerator random;
};

TEST_F(FakeEnvTests, ReaderCannotCreateFile)
{
    Reader *temp;
    const auto s = env->new_reader("nonexistent", temp);
    ASSERT_TRUE(s.is_not_found()) << "Error: " << s.to_string().data();
}

TEST_F(FakeEnvTests, ReadsAndWrites)
{
    auto ra_editor = open_blob<Editor>(*env, kFilename);
    auto ra_reader = open_blob<Reader>(*env, kFilename);
    auto ap_writer = open_blob<Logger>(*env, kFilename);

    const auto first_input = random.Generate(500);
    const auto second_input = random.Generate(500);
    write_out_randomly(random, *ra_editor, first_input);
    write_out_randomly(random, *ap_writer, second_input);
    const auto output_1 = read_back_randomly(random, *ra_reader, 1'000);
    const auto output_2 = read_back_randomly(random, *ra_editor, 1'000);
    ASSERT_EQ(output_1, output_2);
    ASSERT_EQ(output_1, first_input.to_string() + second_input.to_string());
}

TEST_F(FakeEnvTests, ReaderStopsAtEOF)
{
    auto ra_editor = open_blob<Editor>(*env, kFilename);
    auto ra_reader = open_blob<Reader>(*env, kFilename);

    const auto data = random.Generate(500);
    write_out_randomly(random, *ra_editor, data);

    std::string buffer(data.size() * 2, '\x00');
    Slice slice;
    ASSERT_OK(ra_reader->read(0, buffer.size(), buffer.data(), &slice));
    ASSERT_EQ(slice, data);
}

} // namespace calicodb