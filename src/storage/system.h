#ifndef CALICO_STORAGE_SYSTEM_H
#define CALICO_STORAGE_SYSTEM_H

#include "interface.h"
//#include <system_error>

namespace calico::system {

static constexpr int SUCCESS = 0;
static constexpr int FAILURE = -1;

[[nodiscard]] auto error() -> Error;
[[nodiscard]] auto error(std::errc) -> Error;

auto size(int) -> Size;
auto read(int, Bytes) -> Size;
auto write(int, BytesView) -> Size;
auto seek(int, long, int) -> Index;
auto open(const std::string&, int, int) -> int;
auto close(int) -> void;
auto sync(int) -> void;

auto noex_size(int) -> Result<Size>;
auto noex_read(int, Bytes) -> Result<Size>;
auto noex_write(int, BytesView) -> Result<Size>;
auto noex_seek(int, long, int) -> Result<Index>;
auto noex_open(const std::string&, int, int) -> Result<int>;
auto noex_close(int) -> Result<void>;
auto noex_sync(int) -> Result<void>;
auto noex_unlink(const std::string&) -> Result<void>;

} // calico::system

#endif // CALICO_STORAGE_SYSTEM_H
