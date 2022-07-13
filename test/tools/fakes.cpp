#include <limits>
#include "fakes.h"
#include "utils/logging.h"
#include "utils/utils.h"

namespace calico {

static auto maybe_emit_read_error(Random &random, FaultControls::Controls &controls) -> Result<void>
{
    // If the counter is positive, we tick down until we hit 0, at which point we throw an exception.
    // Afterward, further reads will be subject to the normal read fault rate mechanism. If the counter
    // is never set, it stays at -1 and doesn't contribute to the faults generated.
    auto &counter = controls.read_fault_counter;
    if (!counter || random.next_int(99U) < controls.read_fault_rate)
        return ErrorResult {Error::system_error(std::make_error_code(std::errc::io_error).message())};
    // Counter should settle on -1.
    counter -= counter >= 0;
    return {};
}

static auto maybe_emit_write_error(Random &random, FaultControls::Controls &controls) -> Result<void>
{
    auto &counter = controls.write_fault_counter;
    if (!counter || random.next_int(99U) < controls.write_fault_rate)
        return ErrorResult {Error::system_error(std::make_error_code(std::errc::io_error).message())};
    counter -= counter >= 0;
    return {};
}

MemoryBank::MemoryBank(const std::string &path)
    : m_path {path} {}

auto MemoryBank::path() const -> std::string
{
    return m_path;
}

auto MemoryBank::name() const -> std::string
{
    return m_path.filename();
}

auto MemoryBank::open_memory_bank(const std::string &name) -> std::unique_ptr<MemoryBank>
{
    return std::unique_ptr<MemoryBank> {dynamic_cast<MemoryBank*>(open_directory(name)->release())};
}

auto MemoryBank::open_memory(const std::string &name, Mode mode, int permissions) -> std::unique_ptr<Memory>
{
    auto file = open_file(name, mode, permissions);
    return std::unique_ptr<Memory> {dynamic_cast<Memory*>(file->release())};
}

auto Memory::open_reader() -> std::unique_ptr<IFileReader>
{
    return std::make_unique<MemoryReader>(*this);
}

auto Memory::open_writer() -> std::unique_ptr<IFileWriter>
{
    return std::make_unique<MemoryWriter>(*this);
}

static auto do_seek(Index cursor, Size file_size, long offset, Seek whence)
{
    CALICO_EXPECT_BOUNDED_BY(long, cursor);
    const auto position = static_cast<long>(cursor);

    if (whence == Seek::BEGIN) {
        CALICO_EXPECT_GE(offset, 0);
        cursor = static_cast<Size>(offset);
    } else if (whence == Seek::CURRENT) {
        CALICO_EXPECT_GE(position + offset, 0);
        cursor = static_cast<Size>(position + offset);
    } else {
        const auto end = static_cast<long>(file_size);
        CALICO_EXPECT_EQ(whence, Seek::END);
        CALICO_EXPECT_GE(end + offset, 0);
        cursor = static_cast<Size>(end + offset);
    }
    CALICO_EXPECT_GE(static_cast<off_t>(cursor), 0);
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
    if (const auto write_end = in.size() + cursor; memory.size() < write_end)
        memory.resize(write_end);
    utils::mem_copy(stob(memory).range(cursor), in);
    return IOResult {cursor + in.size(), in.size()};
}

MemoryReader::MemoryReader(Memory &memory)
    : m_memory {&memory}
{
    assert(memory.is_open());
}

MemoryWriter::MemoryWriter(Memory &memory)
    : m_memory {&memory}
{
    assert(memory.is_open());
}

auto MemoryBank::children() const -> Result<std::vector<std::string>>
{
    std::vector<std::string> result;
    std::transform(begin(m_shared), end(m_shared), begin(result), [](auto entry) {
        return entry.first;
    });
    return result;
}

auto MemoryBank::open_directory(const std::string &name) -> Result<std::unique_ptr<IDirectory>>
{
    return std::make_unique<MemoryBank>(m_path / name);
}

auto MemoryBank::open_file(const std::string &name, Mode mode, int permissions) -> Result<std::unique_ptr<IFile>>
{
    std::unique_ptr<IFile> memory;
    if (auto itr = m_shared.find(name); itr != std::end(m_shared)) {
        if ((static_cast<unsigned>(mode) & static_cast<unsigned>(Mode::TRUNCATE)) == static_cast<unsigned>(Mode::TRUNCATE))
            itr->second.memory().clear();
        memory = std::make_unique<Memory>(name, itr->second, m_faults[name]);
    } else {
        FaultControls faults;
        SharedMemory shared;
        m_shared.emplace(name, shared);
        m_faults.emplace(name, faults);
        memory = std::make_unique<Memory>(name, shared, faults);
    }
    if (auto opened = memory->open(name, mode, permissions)) {
        return memory;
    } else {
        return ErrorResult {opened.error()};
    }
}

auto MemoryBank::remove() -> Result<void>
{
    m_path.clear();
    return {};
}

auto MemoryBank::sync() -> Result<void>
{
    return {};
}

auto MemoryReader::seek(long offset, Seek whence) -> Result<Index>
{
    m_memory->cursor() = do_seek(m_memory->cursor(), *m_memory->size(), offset, whence);
    return m_memory->cursor();
}

auto MemoryReader::read(Bytes out) -> Result<Size>
{
    if (const auto result = maybe_emit_read_error(m_memory->random(), m_memory->faults().controls()); !result)
        return ErrorResult {result.error()};

    const auto [cursor, read_size] = do_read(m_memory->memory(), m_memory->cursor(), out);
    m_memory->cursor() = cursor;
    return read_size;
}

auto MemoryReader::read(Bytes out, Index offset) -> Result<Size>
{
    return seek(static_cast<long>(offset), Seek::BEGIN)
        .and_then([out, this](Index) -> Result<Size> {
            return read(out);
        });
}

auto MemoryWriter::seek(long offset, Seek whence) -> Result<Index>
{
    // TODO: Actually simulate system call errors rather than just tripping assertions.
    m_memory->cursor() = do_seek(m_memory->cursor(), *m_memory->size(), offset, whence);
    return m_memory->cursor();
}

auto MemoryWriter::write(BytesView in) -> Result<Size>
{
    if (const auto result = maybe_emit_write_error(m_memory->random(), m_memory->faults().controls()); !result)
        return ErrorResult {result.error()};

    const auto [cursor, write_size] = do_write(m_memory->memory(), m_memory->cursor(), in);
    m_memory->cursor() = cursor;
    return write_size;
}

auto MemoryWriter::write(BytesView in, Index offset) -> Result<Size>
{
    return seek(static_cast<long>(offset), Seek::BEGIN)
        .and_then([in, this](Index) -> Result<Size> {
            return write(in);
        });
}

auto MemoryWriter::sync() -> Result<void>
{
    return {};
}

auto MemoryWriter::resize(Size size) -> Result<void>
{
    m_memory->memory().resize(size);
    return {};
}

} // calico
