#ifndef CALICO_DB_HEADER_H
#define CALICO_DB_HEADER_H

#include "calico/bytes.h"
#include "utils/types.h"
#include "page/page.h"

namespace calico {

class Page;

/// Identifies a file as a Calico DB database.
static constexpr std::uint32_t MAGIC_CODE {0xB11924E1};

/**
 * Offset at which to begin computing the header CRC.
 */
static constexpr Size CRC_OFFSET {2 * sizeof(std::uint32_t)};

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

[[nodiscard]]
inline auto decode_page_size(std::uint16_t value) -> Size
{
    if (value == 0)
        return std::numeric_limits<std::uint16_t>::max();
    return value;
}

[[nodiscard]]
inline auto encode_page_size(Size page_size) -> std::uint16_t
{
    if (page_size == std::numeric_limits<std::uint16_t>::max())
        return 0;
    return static_cast<std::uint16_t>(page_size);
}

} // namespace calico

#endif // CALICO_DB_HEADER_H
