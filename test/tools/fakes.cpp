#include <limits>
#include "fakes.h"

namespace cub {

namespace {

    auto do_seek(Index cursor, Size file_size, long offset, Seek whence)
    {
        CUB_EXPECT_BOUNDED_BY(long, cursor);
        const auto position = static_cast<long>(cursor);

        if (whence == Seek::BEGIN) {
            CUB_EXPECT_GE(offset, 0);
            cursor = static_cast<Size>(offset);
        } else if (whence == Seek::CURRENT) {
            CUB_EXPECT_GE(position + offset, 0);
            cursor = static_cast<Size>(position + offset);
        } else {
            const auto end = static_cast<long>(file_size);
            CUB_EXPECT_EQ(whence, Seek::END);
            CUB_EXPECT_GE(end + offset, 0);
            cursor = static_cast<Size>(end + offset);
        }
        CUB_EXPECT_GE(static_cast<off_t>(cursor), 0);
        return cursor;
    }

    struct IOResult {
        Index new_cursor{};
        Size transfer_size{};
    };

    auto do_read(const std::string &memory, Index cursor, Bytes out)
    {
        auto read_size = Size{};
        if (auto buffer = _b(memory); cursor < buffer.size()) {
            const auto diff = buffer.size() - cursor;
            read_size = std::min(out.size(), diff);
            buffer.advance(cursor);
            mem_copy(out, buffer, read_size);
            cursor += read_size;
        }
        return IOResult {cursor, read_size};
    }

    auto do_write(std::string &memory, Index cursor, BytesView in)
    {
        if (const auto write_end = in.size() + cursor; memory.size() < write_end)
            memory.resize(write_end);
        mem_copy(_b(memory).range(cursor), in);
        return IOResult {cursor + in.size(), in.size()};
    }

} // <anonymous>

auto ReadOnlyMemory::seek(long offset, Seek whence) -> Index
{
    m_cursor = do_seek(m_cursor, size(), offset, whence);
    return m_cursor;
}

auto ReadOnlyMemory::read(Bytes out) -> Size
{
    const auto [cursor, read_size] = do_read(m_memory.memory(), m_cursor, out);
    m_cursor = cursor;
    return read_size;
}

auto WriteOnlyMemory::seek(long offset, Seek whence) -> Index
{
    m_cursor = do_seek(m_cursor, size(), offset, whence);
    return m_cursor;
}

auto WriteOnlyMemory::write(BytesView in) -> Size
{
    const auto [cursor, write_size] = do_write(m_memory.memory(), m_cursor, in);
    m_cursor = cursor;
    return write_size;
}

auto ReadWriteMemory::seek(long offset, Seek whence) -> Index
{
    m_cursor = do_seek(m_cursor, size(), offset, whence);
    return m_cursor;
}

auto ReadWriteMemory::read(Bytes out) -> Size
{
    const auto [cursor, read_size] = do_read(m_memory.memory(), m_cursor, out);
    m_cursor = cursor;
    return read_size;
}

auto ReadWriteMemory::write(BytesView in) -> Size
{
    const auto [cursor, write_size] = do_write(m_memory.memory(), m_cursor, in);
    m_cursor = cursor;
    return write_size;
}

auto LogMemory::write(BytesView in) -> Size
{
    const auto [cursor, write_size] = do_write(m_memory.memory(), m_cursor, in);
    m_cursor = cursor;
    return write_size;
}

auto FaultyReadOnlyMemory::read(Bytes out) -> Size
{
    maybe_throw_read_error();
    return ReadOnlyMemory::read(out);
}

auto FaultyWriteOnlyMemory::write(BytesView in) -> Size
{
    maybe_throw_write_error();
    return WriteOnlyMemory::write(in);
}

auto FaultyReadWriteMemory::read(Bytes out) -> Size
{
    maybe_throw_read_error();
    return ReadWriteMemory::read(out);
}

auto FaultyReadWriteMemory::write(BytesView in) -> Size
{
    maybe_throw_write_error();
    return ReadWriteMemory::write(in);
}

auto FaultyLogMemory::write(BytesView in) -> Size
{
    maybe_throw_write_error();
    return LogMemory::write(in);
}

WALHarness::WALHarness(Size block_size)
{
    auto writer_file = std::make_unique<FaultyLogMemory>();
    backing = writer_file->memory();
    auto reader_file = std::make_unique<FaultyReadOnlyMemory>(backing);
    writer = std::make_unique<WALWriter>(std::move(writer_file), block_size);
    reader = std::make_unique<WALReader>(std::move(reader_file), block_size);
}

WALHarness::~WALHarness() = default;

FakeFilesHarness::FakeFilesHarness(Options) // TODO
{
    tree_file = std::make_unique<FaultyReadWriteMemory>();
    tree_backing = tree_file->memory();
    wal_reader_file = std::make_unique<FaultyReadOnlyMemory>();
    wal_backing = wal_reader_file->memory();
    wal_writer_file = std::make_unique<FaultyLogMemory>(wal_backing);
    tree_faults = tree_file->controls();
    wal_reader_faults = wal_reader_file->controls();
    wal_writer_faults = wal_writer_file->controls();
}

auto FaultyDatabase::create(Size page_size) -> FaultyDatabase
{
    std::string header_backing(page_size, '\x00');
    FileHeader header {_b(header_backing)};
    FaultyDatabase db;

    header.set_page_size(page_size);
    header.set_block_size(page_size);

    auto tree_file = std::make_unique<FaultyReadWriteMemory>();
    db.tree_backing = tree_file->memory();
    auto wal_reader_file = std::make_unique<FaultyReadOnlyMemory>();
    db.wal_backing = wal_reader_file->memory();
    auto wal_writer_file = std::make_unique<FaultyLogMemory>(db.wal_backing);

    db.tree_faults = tree_file->controls();
    db.wal_reader_faults = wal_reader_file->controls();
    db.wal_writer_faults = wal_writer_file->controls();
    db.db = std::make_unique<Database::Impl>(Database::Impl::Parameters{
        "FaultyDatabase",
        std::move(tree_file),
        std::move(wal_reader_file),
        std::move(wal_writer_file),
        header,
        0x10, // Use a small number of frames to force a lot of I/O.
    });
    db.page_size = page_size;
    return db;
}

auto FaultyDatabase::clone() -> FaultyDatabase
{
    auto root_page = _b(tree_backing.memory()).truncate(page_size);
    FileHeader header {root_page};
    FaultyDatabase new_db;

    auto tree_file = std::make_unique<FaultyReadWriteMemory>(tree_backing);
    auto wal_reader_file = std::make_unique<FaultyReadOnlyMemory>(wal_backing);
    auto wal_writer_file = std::make_unique<FaultyLogMemory>(wal_backing);
    new_db.tree_backing = tree_backing;
    new_db.wal_backing = wal_backing;
    new_db.tree_faults = tree_file->controls();
    new_db.wal_reader_faults = wal_reader_file->controls();
    new_db.wal_writer_faults = wal_writer_file->controls();
    new_db.db = std::make_unique<Database::Impl>(Database::Impl::Parameters{
        "FaultyDatabase",
        std::move(tree_file),
        std::move(wal_reader_file),
        std::move(wal_writer_file),
        header,
        DEFAULT_FRAME_COUNT,
    });
    new_db.page_size = page_size;
    return new_db;
}

} // cub

