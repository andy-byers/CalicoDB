#ifndef CALICO_STORE_SYSTEM_H
#define CALICO_STORE_SYSTEM_H

#include "calico/storage.h"
#include "utils/result.h"
#include <system_error>

namespace calico::system {

static constexpr int SUCCESS = 0;
static constexpr int FAILURE = -1;

[[nodiscard]] auto error() -> Status;
[[nodiscard]] auto error(std::errc) -> Status;
[[nodiscard]] auto error(const std::string&) -> Status;

[[nodiscard]] auto file_exists(const std::string &) -> Status;
[[nodiscard]] auto file_size(const std::string &) -> Result<Size>;
[[nodiscard]] auto file_read(int, Bytes) -> Result<Size>;
[[nodiscard]] auto file_write(int, BytesView) -> Result<Size>;
[[nodiscard]] auto file_seek(int, long, int) -> Result<Size>;
[[nodiscard]] auto file_open(const std::string &, int, int) -> Result<int>;
[[nodiscard]] auto file_close(int) -> Status;
[[nodiscard]] auto file_sync(int) -> Status;
[[nodiscard]] auto file_remove(const std::string &) -> Status;
[[nodiscard]] auto file_resize(const std::string &, Size) -> Status;
[[nodiscard]] auto dir_create(const std::string &, mode_t) -> Status;
[[nodiscard]] auto dir_remove(const std::string &) -> Status;

} // namespace calico::system

#endif // CALICO_STORE_SYSTEM_H
