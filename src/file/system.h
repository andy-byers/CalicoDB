#ifndef CUB_STORAGE_SYSTEM_H
#define CUB_STORAGE_SYSTEM_H

#include "common.h"
#include "utils/slice.h"

namespace cub::system {

constexpr auto SUCCESS{0};
constexpr auto FAILURE{-1};

auto use_direct_io(int) -> void;
auto size(int) -> Size;
auto access(const std::string&, int) -> bool;
auto unlink(const std::string&) -> void;
auto read(int, MutBytes) -> Size;
auto write(int, RefBytes) -> Size;
auto seek(int, long, int) -> Index;
auto open(const std::string&, int, int) -> int;
auto close(int) -> void;
auto rename(const std::string&, const std::string&) -> void;
auto sync(int) -> void;
auto resize(int, Size) -> void;

} // cub::system

#endif // CUB_STORAGE_SYSTEM_H
