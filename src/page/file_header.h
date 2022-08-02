#ifndef CCO_PAGE_FILE_HEADER_H
#define CCO_PAGE_FILE_HEADER_H

#include "calico/bytes.h"
#include "utils/identifier.h"

namespace cco {

class Page;

/// Identifies a file as a Calico DB database.
static constexpr uint32_t MAGIC_CODE {0xB11924E1};

class FileHeaderReader {
public:
    FileHeaderReader();
    explicit FileHeaderReader(BytesView);
    [[nodiscard]] auto magic_code() const -> Index;
    [[nodiscard]] auto header_crc() const -> Index;
    [[nodiscard]] auto page_count() const -> Size;
    [[nodiscard]] auto node_count() const -> Size;
    [[nodiscard]] auto free_count() const -> Size;
    [[nodiscard]] auto free_start() const -> PageId;
    [[nodiscard]] auto page_size() const -> Size;
    [[nodiscard]] auto record_count() const -> Size;
    [[nodiscard]] auto flushed_lsn() const -> SequenceNumber;
    [[nodiscard]] auto is_magic_code_consistent() const -> bool;
    [[nodiscard]] auto is_header_crc_consistent() const -> bool;

private:
    std::string m_backing;
    BytesView m_header;
};

class FileHeaderWriter {
public:
    explicit FileHeaderWriter(Bytes);
    auto update_magic_code() -> void;
    auto update_header_crc() -> void;
    auto set_page_count(Size) -> void;
    auto set_node_count(Size) -> void;
    auto set_free_count(Size) -> void;
    auto set_free_start(PageId) -> void;
    auto set_page_size(Size) -> void;
    auto set_key_count(Size) -> void;
    auto set_flushed_lsn(SequenceNumber) -> void;

private:
    std::string m_backing;
    Bytes m_header;
};

} // namespace cco

#endif // CCO_PAGE_FILE_HEADER_H
