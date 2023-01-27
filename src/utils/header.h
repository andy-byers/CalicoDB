#ifndef CALICO_CORE_HEADER_H
#define CALICO_CORE_HEADER_H

#include "calico/slice.h"
#include "crc.h"
#include "page/page.h"
#include "types.h"

namespace Calico {

class Page_;

static constexpr std::uint32_t MAGIC_CODE__ {0xB11924E1};
static constexpr Size CRC_OFFSET {2 * sizeof(std::uint32_t)};

struct FileHeader__ {
    std::uint32_t magic_code;
    std::uint32_t header_crc;
    std::uint64_t page_count;
    std::uint64_t freelist_head;
    std::uint64_t record_count;
    std::uint64_t recovery_lsn;
    std::uint16_t page_size;
    Byte reserved[6];
};

static_assert(sizeof(FileHeader__) == 48);

inline auto read_header(const Page_ &page) -> FileHeader__
{
    FileHeader__ header {};
    Span bytes {reinterpret_cast<Byte*>(&header), sizeof(FileHeader__)};
    mem_copy(bytes, page.view(0), bytes.size());
    return header;
}

inline auto write_header(Page_ &page, const FileHeader__ &header) -> void
{
    Slice bytes {reinterpret_cast<const Byte*>(&header), sizeof(FileHeader__)};
    mem_copy(page.span(0, sizeof(header)), bytes, bytes.size());
}

[[nodiscard]]
inline auto compute_header_crc(const FileHeader__ &state)
{
    Slice bytes {reinterpret_cast<const Byte*>(&state), sizeof(state)};
    const auto range = bytes.range(CRC_OFFSET);
    return crc32c::Value(range.data(), range.size());
}

} // namespace Calico

#endif // CALICO_CORE_HEADER_H
