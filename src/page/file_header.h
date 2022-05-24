#ifndef CUB_PAGE_FILE_HEADER_H
#define CUB_PAGE_FILE_HEADER_H

#include "bytes.h"
#include "common.h"

namespace cub {

class Node;
struct LSN;
struct PID;

class FileHeader {
public:
    explicit FileHeader(Bytes);
    explicit FileHeader(Node&);
    auto data() const -> BytesView;
    auto magic_code() const -> Index;
    auto header_crc() const -> Index;
    auto page_count() const -> Size;
    auto node_count() const -> Size;
    auto free_count() const -> Size;
    auto free_start() const -> PID;
    auto page_size() const -> Size;
    auto block_size() const -> Size;
    auto key_count() const -> Size;
    auto flushed_lsn() const -> LSN;
    auto update_magic_code() -> void;
    auto update_header_crc() -> void;
    auto set_page_count(Size) -> void;
    auto set_node_count(Size) -> void;
    auto set_free_count(Size) -> void;
    auto set_free_start(PID) -> void;
    auto set_page_size(Size) -> void;
    auto set_block_size(Size) -> void;
    auto set_key_count(Size) -> void;
    auto set_flushed_lsn(LSN) -> void;

    auto is_magic_code_consistent() const -> bool;
    auto is_header_crc_consistent() const -> bool;

private:
    // Identifies the file as a CubDB_ database.
    static constexpr uint32_t MAGIC_CODE = 0xB11924E1;

    auto data() -> Bytes;

    Bytes m_header;
};

} // cub

#endif // CUB_PAGE_FILE_HEADER_H
