#ifndef CALICO_DB_HEADER_H
#define CALICO_DB_HEADER_H

#include "calico/bytes.h"
#include "utils/types.h"
#include "page/page.h"

namespace calico {

class Page;

static constexpr std::uint32_t MAGIC_CODE {0xB11924E1};
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
        return 1 << 16;
    return value;
}

[[nodiscard]]
inline auto encode_page_size(Size page_size) -> std::uint16_t
{
    if (page_size == 1 << 16)
        return 0;
    return static_cast<std::uint16_t>(page_size);
}

} // namespace calico

#endif // CALICO_DB_HEADER_H
