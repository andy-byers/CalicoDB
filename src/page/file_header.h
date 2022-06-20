#ifndef CUB_PAGE_FILE_HEADER_H
#define CUB_PAGE_FILE_HEADER_H

#include "cub/bytes.h"

namespace cub {

class Node;
struct LSN;
struct PID;

class FileHeader {
public:
    explicit FileHeader(Bytes);
    explicit FileHeader(Node&);
    [[nodiscard]] auto data() const -> BytesView;
    [[nodiscard]] auto magic_code() const -> Index;
    [[nodiscard]] auto header_crc() const -> Index;
    [[nodiscard]] auto page_count() const -> Size;
    [[nodiscard]] auto node_count() const -> Size;
    [[nodiscard]] auto free_count() const -> Size;
    [[nodiscard]] auto free_start() const -> PID;
    [[nodiscard]] auto page_size() const -> Size;
    [[nodiscard]] auto block_size() const -> Size;
    [[nodiscard]] auto record_count() const -> Size;
    [[nodiscard]] auto flushed_lsn() const -> LSN;
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

    [[nodiscard]] auto is_magic_code_consistent() const -> bool;
    [[nodiscard]] auto is_header_crc_consistent() const -> bool;

private:
    // Identifies the file as a Cub DB database.
    static constexpr uint32_t MAGIC_CODE = 0xB11924E1;

    auto data() -> Bytes;

    Bytes m_header;
};

} // cub

#endif // CUB_PAGE_FILE_HEADER_H
