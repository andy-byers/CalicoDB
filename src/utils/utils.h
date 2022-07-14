#ifndef CCO_UTILS_UTILS_H
#define CCO_UTILS_UTILS_H

#include <filesystem>
#include "calico/bytes.h"

namespace cco {

static constexpr Size PAGE_ID_SIZE{sizeof(uint32_t)};
static constexpr Size CELL_POINTER_SIZE{sizeof(uint16_t)};
static constexpr Index NULL_ID_VALUE{0};
static constexpr Index ROOT_ID_VALUE{1};

static constexpr Size MIN_CELL_HEADER_SIZE = sizeof(uint16_t) + // Key size       (2B)
                                             sizeof(uint32_t);  // Value size     (4B)

static constexpr Size MAX_CELL_HEADER_SIZE = MIN_CELL_HEADER_SIZE +
                                             PAGE_ID_SIZE +     // Left child ID  (4B)
                                             PAGE_ID_SIZE;      // Overflow ID    (4B)

enum class PageType : uint16_t {
    INTERNAL_NODE = 0x494E, // "IN"
    EXTERNAL_NODE = 0x4558, // "EX"
    OVERFLOW_LINK = 0x4F56, // "OV"
    FREELIST_LINK = 0x4652, // "FR"
};

inline auto Record::operator<(const Record &rhs) const -> bool
{
    return stob(key) < stob(rhs.key);
}

namespace utils {

    inline auto is_page_type_valid(PageType type) -> bool {
        return type == PageType::INTERNAL_NODE ||
               type == PageType::EXTERNAL_NODE ||
               type == PageType::OVERFLOW_LINK ||
               type == PageType::FREELIST_LINK;
    }

    // Source: http://graphics.stanford.edu/~seander/bithacks.html#DetermineIfPowerOf2
    template<class T>
    auto is_power_of_two(T v) noexcept -> bool {
        return v && !(v & (v - 1));
    }

    inline auto mem_copy(Bytes dst, BytesView src, size_t n) noexcept -> void * {
        assert(n <= src.size());
        assert(n <= dst.size());
        return std::memcpy(dst.data(), src.data(), n);
    }

    inline auto mem_copy(Bytes dst, BytesView src) noexcept -> void * {
        assert(src.size() <= dst.size());
        return mem_copy(dst, src, src.size());
    }

    inline auto mem_clear(Bytes mem, size_t n) noexcept -> void * {
        assert(n <= mem.size());
        return std::memset(mem.data(), 0, n);
    }

    inline auto mem_clear(Bytes mem) noexcept -> void * {
        return mem_clear(mem, mem.size());
    }

    inline auto mem_move(Bytes dst, BytesView src, Size n) noexcept -> void * {
        assert(n <= src.size());
        assert(n <= dst.size());
        return std::memmove(dst.data(), src.data(), n);
    }

    inline auto mem_move(Bytes dst, BytesView src) noexcept -> void * {
        assert(src.size() <= dst.size());
        return mem_move(dst, src, src.size());
    }

    inline auto mem_clear_safe(Bytes data, Size n) noexcept -> void * {
        assert(n <= data.size());
        volatile auto *p = data.data();
        for (Index i{}; i < n; ++i)
            *p++ = 0;
        return data.data();
    }

    inline auto mem_clear_safe(Bytes data) noexcept -> void * {
        return mem_clear_safe(data, data.size());
    }

} // utils
} // calico

#endif // CCO_UTILS_UTILS_H
