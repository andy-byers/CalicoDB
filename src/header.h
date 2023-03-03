#ifndef CALICODB_HEADER_H
#define CALICODB_HEADER_H

#include "types.h"

namespace calicodb
{

class Page;

/* The file header is located at offset 0 on the root page, which is always the first page in the database file. The root
 * page is also a node page, so it will have additional headers following the file header.
 *
 * File Header Format:
 *     Offset  std::size_t  Name
 *    ----------------------------
 *     0       4     magic_code
 *     4       4     header_crc
 *     8       8     page_count
 *     16      8     record_count
 *     24      8     free_list_id
 *     32      8     commit_lsn
 *     40      2     page_size
 */
struct FileHeader {
    static constexpr std::uint32_t kMagicCode {0xB11924E1};
    static constexpr std::size_t kSize {42};
    explicit FileHeader() = default;
    explicit FileHeader(const Page &page);
    explicit FileHeader(const char *data);
    auto write(Page &page) const -> void;

    [[nodiscard]] auto compute_crc() const -> std::uint32_t;

    std::uint32_t magic_code {kMagicCode};
    std::uint32_t header_crc {};
    std::uint64_t page_count {};
    std::uint64_t record_count {};
    Id freelist_head;
    Lsn commit_lsn;
    std::uint16_t page_size {};
};

/* Every page has a page header, which consists of just the page LSN.
 *
 * Page Header Format:
 *     Offset  Size  Name
 *    --------------------------
 *     0       8     page_lsn
 */

/* Node Header Format:
 *     Offset  Size  Name
 *    --------------------------
 *     0       1     flags
 *     1       8     next_id
 *     9       8     prev_id
 *     17      2     cell_count
 *     19      2     cell_start
 *     21      2     free_start
 *     23      2     free_total
 *     25      1     frag_count
 *
 * NOTE: The page_lsn from the page header is included in the node header size for convenience. Also, it should
 *       be noted that internal nodes do not use the prev_id field. In external nodes, the prev_id and next_id
 *       are used to refer to the left and right siblings, respectively. In internal nodes, the next_id field
 *       refers to the rightmost child ID.
 */
struct NodeHeader {
    static constexpr std::size_t kSize {34};
    explicit NodeHeader() = default;
    auto read(const Page &page) -> void;
    auto write(Page &page) const -> void;

    Id next_id;
    Id prev_id;
    std::uint16_t cell_count {};
    std::uint16_t cell_start {};
    std::uint16_t free_start {};
    std::uint16_t free_total {};
    std::uint8_t frag_count {};
    bool is_external {true};
};

} // namespace calicodb

#endif // CALICODB_HEADER_H
