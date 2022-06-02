#include "file_header.h"
#include "utils/crc.h"
#include "utils/encoding.h"
#include "utils/identifier.h"
#include "utils/layout.h"
#include "page/node.h"

namespace cub {

FileHeader::FileHeader(Bytes data)
    : m_header{data.range(FileLayout::header_offset(), FileLayout::HEADER_SIZE)} {}

FileHeader::FileHeader(Node &root)
    // Causes the whole file header region to be written to the WAL.
    : m_header{root.page().mut_range(FileLayout::header_offset(), FileLayout::HEADER_SIZE)}
{
    CUB_EXPECT_TRUE(root.id().is_root());
}

auto FileHeader::data() -> Bytes
{
    return m_header;
}

auto FileHeader::data() const -> BytesView
{
    return m_header;
}

auto FileHeader::magic_code() const -> Index
{
    return get_uint32(m_header.range(FileLayout::MAGIC_CODE_OFFSET));
}

auto FileHeader::header_crc() const -> Index
{
    return get_uint32(m_header.range(FileLayout::HEADER_CRC_OFFSET));
}

auto FileHeader::page_count() const -> Size
{
    return get_uint32(m_header.range(FileLayout::PAGE_COUNT_OFFSET));
}

auto FileHeader::node_count() const -> Size
{
    return get_uint32(m_header.range(FileLayout::NODE_COUNT_OFFSET));
}

auto FileHeader::free_count() const -> Size
{
    return get_uint32(m_header.range(FileLayout::FREE_COUNT_OFFSET));
}

auto FileHeader::free_start() const -> PID
{
    return PID {get_uint32(m_header.range(FileLayout::FREE_START_OFFSET))};
}

auto FileHeader::page_size() const -> Size
{
    return get_uint16(m_header.range(FileLayout::PAGE_SIZE_OFFSET));
}

auto FileHeader::block_size() const -> Size
{
    return get_uint16(m_header.range(FileLayout::BLOCK_SIZE_OFFSET));
}

auto FileHeader::key_count() const -> Size
{
    return get_uint32(m_header.range(FileLayout::KEY_COUNT_OFFSET));
}

auto FileHeader::flushed_lsn() const -> LSN
{
    return LSN {get_uint32(m_header.range(FileLayout::FLUSHED_LSN_OFFSET))};
}

auto FileHeader::update_magic_code() -> void
{
    put_uint32(m_header.range(FileLayout::MAGIC_CODE_OFFSET), MAGIC_CODE);
}

auto FileHeader::update_header_crc() -> void
{
    const auto offset = FileLayout::HEADER_CRC_OFFSET;
    put_uint32(m_header.range(offset), crc_32(m_header.range(offset + sizeof(uint32_t))));
}

auto FileHeader::set_page_count(Size page_count) -> void
{
    CUB_EXPECT_BOUNDED_BY(uint32_t, page_count);
    put_uint32(m_header.range(FileLayout::PAGE_COUNT_OFFSET), static_cast<uint32_t>(page_count));
}

auto FileHeader::set_node_count(Size node_count) -> void
{
    CUB_EXPECT_BOUNDED_BY(uint32_t, node_count);
    put_uint32(m_header.range(FileLayout::NODE_COUNT_OFFSET), static_cast<uint32_t>(node_count));
}

auto FileHeader::set_free_count(Size free_count) -> void
{
    CUB_EXPECT_BOUNDED_BY(uint32_t, free_count);
    put_uint32(m_header.range(FileLayout::FREE_COUNT_OFFSET), static_cast<uint32_t>(free_count));
}

auto FileHeader::set_free_start(PID free_start) -> void
{
    put_uint32(m_header.range(FileLayout::FREE_START_OFFSET), free_start.value);
}

auto FileHeader::set_page_size(Size page_size) -> void
{
    CUB_EXPECT_BOUNDED_BY(uint16_t, page_size);
    put_uint16(m_header.range(FileLayout::PAGE_SIZE_OFFSET), static_cast<uint16_t>(page_size));
}

auto FileHeader::set_block_size(Size block_size) -> void
{
    CUB_EXPECT_BOUNDED_BY(uint16_t, block_size);
    put_uint16(m_header.range(FileLayout::BLOCK_SIZE_OFFSET), static_cast<uint16_t>(block_size));
}

auto FileHeader::set_key_count(Size key_count) -> void
{
    CUB_EXPECT_BOUNDED_BY(uint32_t, key_count);
    put_uint32(m_header.range(FileLayout::KEY_COUNT_OFFSET), static_cast<uint32_t>(key_count));
}

auto FileHeader::set_flushed_lsn(LSN flushed_lsn) -> void
{
    put_uint32(m_header.range(FileLayout::FLUSHED_LSN_OFFSET), flushed_lsn.value);
}

auto FileHeader::is_magic_code_consistent() const -> bool
{
    return magic_code() == MAGIC_CODE;
}

auto FileHeader::is_header_crc_consistent() const -> bool
{
    const auto offset = FileLayout::HEADER_CRC_OFFSET + sizeof(uint32_t);
    return header_crc() == crc_32(m_header.range(offset));
}

} // cub