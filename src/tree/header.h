#ifndef CALICO_TREE_HEADER_H
#define CALICO_TREE_HEADER_H

#include "utils/types.h"

namespace Calico {

class Page;

/* File Header Format:
 *     Offset  Size  Name
 *     0       4     magic_code
 *     4       4     header_crc
 *     12      8     page_count
 *     20      8     record_count
 *     28      8     free_list_id
 *     36      8     recovery_lsn
 *     38      2     page_size
 */
struct FileHeader {
    static constexpr std::uint32_t MAGIC_CODE {0xB11924E1};
    static constexpr Size SIZE {42};
    explicit FileHeader() = default;
    explicit FileHeader(const Page &page);
    auto write(Page &page) const -> void;

    [[nodiscard]] auto compute_crc() const -> std::uint32_t;

    std::uint32_t magic_code {MAGIC_CODE};
    std::uint32_t header_crc {};
    std::uint64_t page_count {};
    std::uint64_t record_count {};
    Id freelist_head;
    Lsn recovery_lsn;
    std::uint16_t page_size {};
};

/* Node Header Format:
 *     Offset  Size  Name
 *     0       8     page_lsn
 *     8       1     flags
 *     9       8     next_id
 *     17      8     prev_id
 *     25      2     cell_count
 *     27      2     cell_start
 *     29      2     free_start
 *     31      2     free_total
 *     33      1     frag_count
 *
 * TODO: We don't need so many header fields. For instance, instead of caching the total free block bytes, we can just limit the
 *       list length and null out the last "next pointer". The "prev_id" field can be placed at the end
 *       and omitted in external nodes.
 */
struct NodeHeader {
    static constexpr Size SIZE {34};
    explicit NodeHeader() = default;
    explicit NodeHeader(const Page &page);
    auto write(Page &page) const -> void;

    Lsn page_lsn;
    Id next_id;
    Id prev_id;
    std::uint16_t cell_count {};
    std::uint16_t cell_start {};
    std::uint16_t free_start {};
    std::uint16_t free_total {};
    std::uint8_t frag_count {};
    bool is_external {true};

private:
    static constexpr Byte EXTERNAL_BITS {0b01'1};
    static constexpr Byte INTERNAL_BITS {0b10'1};
};

} // namespace Calico

#endif // CALICO_TREE_HEADER_H
