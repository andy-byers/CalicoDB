#ifndef CUB_FILE_SYSTEM_H
#define CUB_FILE_SYSTEM_H

#include "cub/bytes.h"

namespace cub::system {

static constexpr int SUCCESS = 0;
static constexpr int FAILURE = -1;

auto use_direct_io(int) -> void;
auto size(int) -> Size;
auto access(const std::string&, int) -> bool;
auto unlink(const std::string&) -> void;
auto read(int, Bytes) -> Size;
auto write(int, BytesView) -> Size;
auto seek(int, long, int) -> Index;
auto open(const std::string&, int, int) -> int;
auto close(int) -> void;
auto rename(const std::string&, const std::string&) -> void;
auto sync(int) -> void;
auto resize(int, Size) -> void;

} // cub::system

#endif // CUB_FILE_SYSTEM_H
