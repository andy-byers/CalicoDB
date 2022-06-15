/*
 * References:
 * (1) https://github.com/gcc-mirror/gcc/blob/master/libstdc%2B%2B-v3/include/std/utility
 */

#ifndef CUB_COMMON_H
#define CUB_COMMON_H

#include <cstdint>
#include <cstdlib>
#include <string>

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
using Size = std::uint64_t;
using Index = std::uint64_t;

static constexpr Size MIN_FRAME_COUNT = 0x8;
static constexpr Size MAX_FRAME_COUNT = 0x1000;
static constexpr Size MIN_PAGE_SIZE = 0x100;
static constexpr Size MAX_PAGE_SIZE = 1 << 15;
static constexpr Size MIN_BLOCK_SIZE = MIN_PAGE_SIZE;
static constexpr Size MAX_BLOCK_SIZE = MAX_PAGE_SIZE;
static constexpr Size DEFAULT_FRAME_COUNT = 0x80;
static constexpr Size DEFAULT_PAGE_SIZE = 0x4000;
static constexpr Size DEFAULT_BLOCK_SIZE = 0x8000;
static constexpr int DEFAULT_PERMISSIONS = 0666;

/**
 * Options to use when opening a database.
 */
struct Options {
    Size page_size {DEFAULT_PAGE_SIZE}; ///< Size of a database page in bytes.
    Size block_size {DEFAULT_BLOCK_SIZE}; ///< Size of a WAL block in bytes.
    Size frame_count {DEFAULT_FRAME_COUNT}; ///< Number of frames to allow the buffer pool.
    int permissions {DEFAULT_PERMISSIONS}; ///< Permissions with which to open files.
    bool use_direct_io {}; ///< True if we should use direct I/O, false otherwise.
};

/**
 * Representation of a database record.
 */
struct Record {
    auto operator<(const cub::Record&) const -> bool;  // TODO: Seems to be necessary for std::sort()...
    std::string key; ///< The key by which records are ordered.
    std::string value; ///< The record value.
};

} // cub

auto operator<(const cub::Record&, const cub::Record&) -> bool;
auto operator>(const cub::Record&, const cub::Record&) -> bool;
auto operator<=(const cub::Record&, const cub::Record&) -> bool;
auto operator>=(const cub::Record&, const cub::Record&) -> bool;
auto operator==(const cub::Record&, const cub::Record&) -> bool;
auto operator!=(const cub::Record&, const cub::Record&) -> bool;

#endif // CUB_COMMON_H
