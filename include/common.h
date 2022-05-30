/*
 * References:
 * (1) https://github.com/gcc-mirror/gcc/blob/master/libstdc%2B%2B-v3/include/std/utility
 */

#ifndef CUB_COMMON_H
#define CUB_COMMON_H

#include <cstdlib>

#if (!defined(_WIN32) && !defined(_WIN64)) && (defined(__unix__) || defined(__unix) || defined(__APPLE__))
#    define CUB_UNIX
#    ifdef __APPLE__
#        define CUB_OSX
#    endif
#else
#    error "Error: This platform is currently unsupported"
#endif

namespace cub {

using Byte = char;
using Size = size_t;
using Index = size_t;

static constexpr Size MIN_FRAME_COUNT = 0x8;
static constexpr Size MAX_FRAME_COUNT = 0x400;
static constexpr Size MIN_PAGE_SIZE = 0x100;
static constexpr Size MAX_PAGE_SIZE = 1 << 15;
static constexpr Size MIN_BLOCK_SIZE = MIN_PAGE_SIZE;
static constexpr Size MAX_BLOCK_SIZE = MAX_PAGE_SIZE;
static constexpr Size DEFAULT_FRAME_COUNT = 0x80;
static constexpr Size DEFAULT_PAGE_SIZE = 0x4000;
static constexpr Size DEFAULT_BLOCK_SIZE = 0x8000;
static constexpr int DEFAULT_PERMISSIONS = 0666;

struct Options {
    Size page_size {DEFAULT_PAGE_SIZE};
    Size block_size {DEFAULT_BLOCK_SIZE};
    Size frame_count {DEFAULT_FRAME_COUNT};
    int permissions {DEFAULT_PERMISSIONS};
};

} // cub

#endif // CUB_COMMON_H
