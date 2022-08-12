#ifndef CCO_STORAGE_SYSTEM_H
#define CCO_STORAGE_SYSTEM_H

#include "calico/storage.h"

namespace cco::system {

static constexpr int SUCCESS = 0;
static constexpr int FAILURE = -1;

[[nodiscard]] auto error() -> Status;
[[nodiscard]] auto error(std::errc) -> Status;
[[nodiscard]] auto file_exists(const std::string &) -> Status;
[[nodiscard]] auto file_size(const std::string &) -> Result<Size>;
[[nodiscard]] auto file_read(int, Bytes) -> Result<Size>;
[[nodiscard]] auto file_write(int, BytesView) -> Result<Size>;
[[nodiscard]] auto file_seek(int, long, int) -> Result<Index>;
[[nodiscard]] auto file_open(const std::string &, int, int) -> Result<int>;
[[nodiscard]] auto file_close(int) -> Status;
[[nodiscard]] auto file_sync(int) -> Status;
[[nodiscard]] auto file_remove(const std::string &) -> Status;
[[nodiscard]] auto dir_create(const std::string &) -> Status;
[[nodiscard]] auto dir_remove(const std::string &) -> Status;

} // namespace cco::system

#endif // CCO_STORAGE_SYSTEM_H
