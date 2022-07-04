#ifndef CALICO_STORAGE_SYSTEM_H
#define CALICO_STORAGE_SYSTEM_H

#include "interface.h"

namespace calico::system {

static constexpr int SUCCESS = 0;
static constexpr int FAILURE = -1;

auto size(int) -> Size;
auto read(int, Bytes) -> Size;
auto write(int, BytesView) -> Size;
auto seek(int, long, int) -> Index;
auto open(const std::string&, int, int) -> int;
auto close(int) -> void;
auto sync(int) -> void;

} // calico::system

#endif // CALICO_STORAGE_SYSTEM_H
