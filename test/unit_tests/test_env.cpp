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

template <class EnvType>
[[nodiscard]] auto open_file(EnvType &env, const std::string &filename) -> std::unique_ptr<File>
{
    File *temp = nullptr;
    EXPECT_OK(env.new_file(filename, temp));
    return std::unique_ptr<File>(temp);
}

auto write_whole_file(const std::string &path, const Slice &message) -> void
{
    std::ofstream ofs(path, std::ios::trunc);
    ofs << message.to_string();
}

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
    : public EnvTestHarness<EnvPosix>,
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

        LogFile *temp;
        EXPECT_OK(env().new_log_file(filename, temp));
        file.reset(temp);
    }

    std::string filename {"__test_info_logger"};
    std::unique_ptr<LogFile> file;
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

} // namespace calicodb