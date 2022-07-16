#ifndef CCO_STORAGE_SYSTEM_H
#define CCO_STORAGE_SYSTEM_H

#include "interface.h"

namespace cco::system {

static constexpr int SUCCESS = 0;
static constexpr int FAILURE = -1;

[[nodiscard]] auto error() -> Status;
[[nodiscard]] auto error(std::errc) -> Status;
[[nodiscard]] auto exists(const std::string &name) -> Result<bool>;
[[nodiscard]] auto size(int) -> Result<Size>;
[[nodiscard]] auto read(int, Bytes) -> Result<Size>;
[[nodiscard]] auto write(int, BytesView) -> Result<Size>;
[[nodiscard]] auto seek(int, long, int) -> Result<Index>;
[[nodiscard]] auto open(const std::string&, int, int) -> Result<int>;
[[nodiscard]] auto close(int) -> Result<void>;
[[nodiscard]] auto sync(int) -> Result<void>;
[[nodiscard]] auto unlink(const std::string&) -> Result<void>;

} // calico::system

#endif // CCO_STORAGE_SYSTEM_H
