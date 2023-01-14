#ifndef CALICO_STORE_SYSTEM_H
#define CALICO_STORE_SYSTEM_H

#include "calico/storage.h"
#include <expected.hpp>
#include <system_error>

namespace Calico::Posix {

static constexpr int SUCCESS = 0;
static constexpr int FAILURE = -1;

[[nodiscard]] auto error() -> Status;
[[nodiscard]] auto error(std::errc) -> Status;

[[nodiscard]] auto file_exists(const std::string &) -> Status;
[[nodiscard]] auto file_size(const std::string &) -> tl::expected<Size, Status>;
[[nodiscard]] auto file_read(int, Byte *, Size) -> tl::expected<Size, Status>;
[[nodiscard]] auto file_write(int, Slice) -> tl::expected<Size, Status>;
[[nodiscard]] auto file_seek(int, long, int) -> tl::expected<Size, Status>;
[[nodiscard]] auto file_open(const std::string &, int, int) -> tl::expected<int, Status>;
[[nodiscard]] auto file_close(int) -> Status;
[[nodiscard]] auto file_sync(int) -> Status;
[[nodiscard]] auto file_remove(const std::string &) -> Status;
[[nodiscard]] auto file_resize(const std::string &, Size) -> Status;
[[nodiscard]] auto dir_create(const std::string &, mode_t) -> Status;
[[nodiscard]] auto dir_remove(const std::string &) -> Status;

} // namespace Calico::system

#endif // CALICO_STORE_SYSTEM_H
