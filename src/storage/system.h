#ifndef CALICO_STORAGE_SYSTEM_H
#define CALICO_STORAGE_SYSTEM_H

#include "interface.h"
//#include <system_error>

namespace calico::system {

static constexpr int SUCCESS = 0;
static constexpr int FAILURE = -1;

[[nodiscard]] auto error() -> Error;
[[nodiscard]] auto error(std::errc) -> Error;

auto size(int) -> Result<Size>;
auto read(int, Bytes) -> Result<Size>;
auto write(int, BytesView) -> Result<Size>;
auto seek(int, long, int) -> Result<Index>;
auto open(const std::string&, int, int) -> Result<int>;
auto close(int) -> Result<void>;
auto sync(int) -> Result<void>;
auto unlink(const std::string&) -> Result<void>;

} // calico::system

#endif // CALICO_STORAGE_SYSTEM_H
