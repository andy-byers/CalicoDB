/*
 * References:
 * (1) https://github.com/gcc-mirror/gcc/blob/master/libstdc%2B%2B-v3/include/std/utility
 */

#ifndef CUB_COMMON_H
#define CUB_COMMON_H

#include <cstdlib>

#if !CUB_HAS_O_DIRECT
#   define O_DIRECT 0
#endif

namespace cub {

using Byte = char;
using Size = size_t;
using Index = size_t;

constexpr auto MIN_FRAME_COUNT{0x8};
constexpr auto MAX_FRAME_COUNT{0x400};
constexpr auto MIN_PAGE_SIZE{0x100};
constexpr auto MAX_PAGE_SIZE{1 << 15};
constexpr auto MIN_BLOCK_SIZE{MIN_PAGE_SIZE};
constexpr auto MAX_BLOCK_SIZE{MAX_PAGE_SIZE};

constexpr auto DEFAULT_FRAME_COUNT{0x80};
constexpr auto DEFAULT_PAGE_SIZE{0x4000};
constexpr auto DEFAULT_BLOCK_SIZE{0x8000};
constexpr auto DEFAULT_PERMISSIONS{0666};

struct Options {
    Size page_size{DEFAULT_PAGE_SIZE};
    Size block_size{DEFAULT_BLOCK_SIZE};
    Size frame_count{DEFAULT_FRAME_COUNT};
    int permissions{DEFAULT_PERMISSIONS};
};

} // cub

#endif // CUB_COMMON_H
