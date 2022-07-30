#include "file_header.h"

#include "node.h"
#include "utils/crc.h"
#include "utils/encoding.h"
#include "utils/identifier.h"
#include "utils/layout.h"

namespace cco {

FileHeaderReader::FileHeaderReader()
    : m_backing(FileLayout::HEADER_SIZE, '\x00'),
      m_header {stob(m_backing)}
{}

FileHeaderReader::FileHeaderReader(BytesView view)
    : m_header {view}
{
    CCO_EXPECT_EQ(view.size(), FileLayout::HEADER_SIZE);
}

auto FileHeaderReader::magic_code() const -> Index
{
    return get_u32(m_header.range(FileLayout::MAGIC_CODE_OFFSET));
}

auto FileHeaderReader::header_crc() const -> Index
{
    return get_u32(m_header.range(FileLayout::HEADER_CRC_OFFSET));
}

auto FileHeaderReader::page_count() const -> Size
{
    return get_u32(m_header.range(FileLayout::PAGE_COUNT_OFFSET));
}

auto FileHeaderReader::node_count() const -> Size
{
    return get_u32(m_header.range(FileLayout::NODE_COUNT_OFFSET));
}

auto FileHeaderReader::free_count() const -> Size
{
    return get_u32(m_header.range(FileLayout::FREE_COUNT_OFFSET));
}

auto FileHeaderReader::free_start() const -> PID
{
    return PID {get_u32(m_header.range(FileLayout::FREE_START_OFFSET))};
}

auto FileHeaderReader::page_size() const -> Size
{
    return get_u16(m_header.range(FileLayout::PAGE_SIZE_OFFSET));
}

auto FileHeaderReader::record_count() const -> Size
{
    return get_u32(m_header.range(FileLayout::KEY_COUNT_OFFSET));
}

auto FileHeaderReader::flushed_lsn() const -> LSN
{
    return LSN {get_u32(m_header.range(FileLayout::FLUSHED_LSN_OFFSET))};
}

FileHeaderWriter::FileHeaderWriter(Bytes bytes)
    : m_header {bytes}
{
    CCO_EXPECT_EQ(bytes.size(), FileLayout::HEADER_SIZE);
}

auto FileHeaderWriter::update_magic_code() -> void
{
    put_u32(m_header.range(FileLayout::MAGIC_CODE_OFFSET), MAGIC_CODE);
}

auto FileHeaderWriter::update_header_crc() -> void
{
    const auto offset = FileLayout::HEADER_CRC_OFFSET;
    put_u32(m_header.range(offset), crc_32(m_header.range(offset + sizeof(uint32_t))));
}

auto FileHeaderWriter::set_page_count(Size page_count) -> void
{
    put_u32(m_header.range(FileLayout::PAGE_COUNT_OFFSET), static_cast<uint32_t>(page_count));
}

auto FileHeaderWriter::set_node_count(Size node_count) -> void
{
    put_u32(m_header.range(FileLayout::NODE_COUNT_OFFSET), static_cast<uint32_t>(node_count));
}

auto FileHeaderWriter::set_free_count(Size free_count) -> void
{
    put_u32(m_header.range(FileLayout::FREE_COUNT_OFFSET), static_cast<uint32_t>(free_count));
}

auto FileHeaderWriter::set_free_start(PID free_start) -> void
{
    put_u32(m_header.range(FileLayout::FREE_START_OFFSET), free_start.value);
}

auto FileHeaderWriter::set_page_size(Size page_size) -> void
{
    put_u16(m_header.range(FileLayout::PAGE_SIZE_OFFSET), static_cast<uint16_t>(page_size));
}

auto FileHeaderWriter::set_key_count(Size key_count) -> void
{
    put_u32(m_header.range(FileLayout::KEY_COUNT_OFFSET), static_cast<uint32_t>(key_count));
}

auto FileHeaderWriter::set_flushed_lsn(LSN flushed_lsn) -> void
{
    put_u32(m_header.range(FileLayout::FLUSHED_LSN_OFFSET), flushed_lsn.value);
}

auto FileHeaderReader::is_magic_code_consistent() const -> bool
{
    return magic_code() == MAGIC_CODE;
}

auto FileHeaderReader::is_header_crc_consistent() const -> bool
{
    const auto offset = FileLayout::HEADER_CRC_OFFSET + sizeof(uint32_t);
    return header_crc() == crc_32(m_header.range(offset));
}

} // namespace cco