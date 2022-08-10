#ifndef CCO_DB_HEADER_H
#define CCO_DB_HEADER_H

#include "calico/bytes.h"
#include "utils/identifier.h"
#include "page/page.h"

namespace cco {

class Page;

/// Identifies a file as a Calico DB database.
static constexpr std::uint32_t MAGIC_CODE {0xB11924E1};

struct FileHeader {
    std::uint32_t magic_code;
    std::uint32_t header_crc;
    std::uint64_t page_count;
    std::uint64_t freelist_head;
    std::uint64_t record_count;
    std::uint64_t flushed_lsn;
    std::uint16_t page_size;
    Byte reserved[6];
};

inline auto read_header(const Page &page) -> FileHeader
{
    FileHeader header {};
    std::memcpy(&header, page.view(0).data(), sizeof(header));
    return header;
}

inline auto write_header(Page &page, const FileHeader &header) -> void
{
    std::memcpy(page.bytes(0, sizeof(header)).data(), &header, sizeof(header));
}

} // namespace cco

#endif // CCO_DB_HEADER_H
