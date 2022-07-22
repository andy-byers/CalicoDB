#include <limits>
#include "fakes.h"
#include "utils/logging.h"
#include "utils/utils.h"

namespace cco {

static auto maybe_emit_read_error(Random &random, FaultControls::Controls &controls) -> Result<void>
{
    // Once the counter reaches 0, we return an error. Additional calls will also return errors, until
    // the read fault counter is reset to -1.
    auto &counter = controls.read_fault_counter;
    if (!counter || random.next_int(99U) < controls.read_fault_rate)
        return Err {Status::system_error(std::make_error_code(std::errc::io_error).message())};
    // Counter should settle on -1.
    counter -= counter >= 0;
    return {};
}

static auto maybe_emit_write_error(Random &random, FaultControls::Controls &controls) -> Result<void>
{
    auto &counter = controls.write_fault_counter;
    if (!counter || random.next_int(99U) < controls.write_fault_rate)
        return Err {Status::system_error(std::make_error_code(std::errc::io_error).message())};
    counter -= counter >= 0;
    return {};
}

FakeDirectory::FakeDirectory(const std::string &path)
    : m_path {path} {}

auto FakeDirectory::path() const -> std::string
{
    return m_path;
}

auto FakeDirectory::name() const -> std::string
{
    return m_path.filename();
}

auto FakeDirectory::open_fake_file(const std::string &name, Mode mode, int permissions) -> std::unique_ptr<FakeFile>
{
    auto file = open_file(name, mode, permissions);
    return std::unique_ptr<FakeFile> {dynamic_cast<FakeFile*>(file->release())};
}

static auto do_seek(Index cursor, Size file_size, long offset, Seek whence)
{
    CCO_EXPECT_BOUNDED_BY(long, cursor);
    const auto position = static_cast<long>(cursor);

    if (whence == Seek::BEGIN) {
        CCO_EXPECT_GE(offset, 0);
        cursor = static_cast<Size>(offset);
    } else if (whence == Seek::CURRENT) {
        CCO_EXPECT_GE(position + offset, 0);
        cursor = static_cast<Size>(position + offset);
    } else {
        const auto end = static_cast<long>(file_size);
        CCO_EXPECT_EQ(whence, Seek::END);
        CCO_EXPECT_GE(end + offset, 0);
        cursor = static_cast<Size>(end + offset);
    }
    CCO_EXPECT_GE(static_cast<off_t>(cursor), 0);
    return cursor;
}

struct IOResult {
    Index new_cursor{};
    Size transfer_size{};
};

auto do_read(const std::string &memory, Index cursor, Bytes out)
{
    auto read_size = Size{};
    if (auto buffer = stob(memory); cursor < buffer.size()) {
        const auto diff = buffer.size() - cursor;
        read_size = std::min(out.size(), diff);
        buffer.advance(cursor);
        utils::mem_copy(out, buffer, read_size);
        cursor += read_size;
    }
    return IOResult {cursor, read_size};
}

static auto do_write(std::string &memory, Index cursor, BytesView in)
{
    if (const auto write_end = cursor + in.size(); memory.size() < write_end)
        memory.resize(write_end);
    utils::mem_copy(stob(memory).range(cursor), in);
    return IOResult {cursor + in.size(), in.size()};
}

auto FakeDirectory::children() const -> Result<std::vector<std::string>>
{
    std::vector<std::string> result;
    std::transform(begin(m_shared), end(m_shared), begin(result), [](auto entry) {
        return entry.first;
    });
    return result;
}

auto FakeDirectory::open_file(const std::string &name, Mode mode, int permissions) -> Result<std::unique_ptr<IFile>>
{
    std::unique_ptr<IFile> file;
    if (auto itr = m_shared.find(name); itr != std::end(m_shared)) {
        if ((static_cast<unsigned>(mode) & static_cast<unsigned>(Mode::TRUNCATE)) == static_cast<unsigned>(Mode::TRUNCATE))
            itr->second.memory().clear();
        file = std::make_unique<FakeFile>(name, itr->second, m_faults[name]);
    } else {
        FaultControls faults;
        SharedMemory shared;
        m_shared.emplace(name, shared);
        m_faults.emplace(name, faults);
        file = std::make_unique<FakeFile>(name, shared, faults);
    }
    if (auto opened = file->open(name, mode, permissions)) {
        return file;
    } else {
        return Err {opened.error()};
    }
}

auto FakeDirectory::remove() -> Result<void>
{
    m_path.clear();
    return {};
}

auto FakeDirectory::sync() -> Result<void>
{
    return {};
}

auto FakeDirectory::close() -> Result<void>
{
    CCO_EXPECT_TRUE(m_is_open);
    m_is_open = false;
    return {};
}

auto FakeFile::seek(long offset, Seek whence) -> Result<Index>
{
    m_cursor = do_seek(m_cursor, size().value(), offset, whence);
    return m_cursor;
}

auto FakeFile::read(Bytes out) -> Result<Size>
{
    if (const auto result = maybe_emit_read_error(m_random, m_faults.controls()); !result)
        return Err {result.error()};

    const auto [cursor, read_size] = do_read(m_memory.memory(), m_cursor, out);
    m_cursor = cursor;
    return read_size;
}

auto FakeFile::read(Bytes out, Index offset) -> Result<Size>
{
    return seek(static_cast<long>(offset), Seek::BEGIN)
        .and_then([out, this](Index) -> Result<Size> {
            return read(out);
        });
}

auto FakeFile::write(BytesView in) -> Result<Size>
{
    if (const auto result = maybe_emit_write_error(m_random, m_faults.controls()); !result)
        return Err {result.error()};
    if (m_is_append)
        m_cursor = m_memory.memory().size();
    const auto [cursor, write_size] = do_write(m_memory.memory(), m_cursor, in);
    m_cursor = cursor;
    return write_size;
}

auto FakeFile::write(BytesView in, Index offset) -> Result<Size>
{
    return seek(static_cast<long>(offset), Seek::BEGIN)
        .and_then([in, this](Index) -> Result<Size> {
            return write(in);
        });
}

auto FakeFile::sync() -> Result<void>
{
    return {};
}

auto FakeFile::resize(Size size) -> Result<void>
{
    m_memory.memory().resize(size);
    return {};
}

//auto MockDirectory::open_and_register_mock_file(const std::string &name, Mode mode, int permissions) -> std::unique_ptr<IFile>
//{
//    auto *mock = new testing::NiceMock<MockFile> {m_fake.open_file(name, mode, permissions).value()};
//    mock->delegate_to_fake();
//    m_files.emplace(mock_name(name, mode), mock);
//    return std::unique_ptr<IFile> {mock};
//}

} // cco
