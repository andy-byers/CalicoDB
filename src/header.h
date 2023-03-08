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
 *     Offset  Size  Name
 *    ----------------------------
 *     0       4     magic_code
 *     4       4     header_crc
 *     8       8     page_count
 *     16      8     record_count
 *     24      8     free_list_id
 *     32      8     last_table_id
 *     40      8     commit_lsn
 *     48      2     page_size
 */
struct FileHeader {
    static constexpr std::uint32_t kMagicCode {0xB11924E1};
    static constexpr std::size_t kSize {50};
    auto read(const char *data) -> void;
    auto write(char *data) const -> void;

    [[nodiscard]] auto compute_crc() const -> std::uint32_t;

    std::uint32_t magic_code {kMagicCode};
    std::uint32_t header_crc {};
    std::uint64_t page_count {};
    std::uint64_t record_count {};
    Id freelist_head;
    Id last_table_id;
    Lsn commit_lsn;
    std::uint16_t page_size {};
};

/* Every tree has a tree header on its root page, after the page header, but before the node header.
 *
 * Tree Header Format:
 *     Offset  Size  Name
 *    --------------------------
 *     0       8     checkpoint_lsn
 */
static constexpr auto kTreeHeaderSize = sizeof(Lsn);

/* Every page has a page header, after the file header, but before the tree header, if they are on this page.
 *
 * Page Header Format:
 *     Offset  Size  Name
 *    --------------------------
 *     0       8     page_lsn
 */
static constexpr auto kPageHeaderSize = sizeof(Lsn);

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
 */
struct NodeHeader {
    static constexpr std::size_t kSize {26};
    auto read(const char *data) -> void;
    auto write(char *data) const -> void;

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
