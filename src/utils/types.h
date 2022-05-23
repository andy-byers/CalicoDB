/**
 * References
 * (1) https://en.cppreference.com/w/cpp/utility/exchange
 */

#ifndef CUB_TYPES_H
#define CUB_TYPES_H

#include <vector>

#include "common.h"
#include "layout.h"
#include "bytes.h"

namespace cub {

class Node;

// TODO: Put somewhere else!
class FileHeader {
public:
    // Identifies the file as a CubDB_ database.
    static constexpr auto MAGIC_CODE{0xB11924E1U};

    explicit FileHeader(Bytes);
    explicit FileHeader(Node&);
    auto data() const -> BytesView;
    auto magic_code() const -> Index;
    auto page_count() const -> Size;
    auto node_count() const -> Size;
    auto free_count() const -> Size;
    auto free_start() const -> PID;
    auto page_size() const -> Size;
    auto block_size() const -> Size;
    auto key_count() const -> Size;
    auto flushed_lsn() const -> LSN;
    auto set_magic_code() -> void;
    auto set_page_count(Size) -> void;
    auto set_node_count(Size) -> void;
    auto set_free_count(Size) -> void;
    auto set_free_start(PID) -> void;
    auto set_page_size(Size) -> void;
    auto set_block_size(Size) -> void;
    auto set_key_count(Size) -> void;
    auto set_flushed_lsn(LSN) -> void;

private:
    auto data() -> Bytes;

    Bytes m_header;
};

template<class Value> struct Unique {

    template<class V> explicit Unique(V v)
        : value{std::move(v)} {}

    Unique(const Unique &) = delete;
    auto operator=(const Unique &) -> Unique & = delete;

    Unique(Unique &&rhs) noexcept
    {
        *this = std::move(rhs);
    }

    auto operator=(Unique &&rhs) noexcept -> Unique &
    {
        // TODO: std::exchange() is not noexcept until C++23, but (1) doesn't specify
        //       any exceptions it could throw.
        value = std::exchange(rhs.value, {});
        return *this;
    }

    Value value;
};

enum class PageType: uint16_t {
    NULL_PAGE     = 0x0000,
    INTERNAL_NODE = 0x494E, // "IN"
    EXTERNAL_NODE = 0x4558, // "EX"
    OVERFLOW_LINK = 0x4F56, // "OV"
    FREELIST_LINK = 0x4652, // "FR"
};

} // Cub

#endif // CUB_TYPES_H
