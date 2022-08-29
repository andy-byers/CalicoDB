#ifndef CALICO_STATUS_H
#define CALICO_STATUS_H

#include "bytes.h"

namespace calico {

class Status final {
public:
    [[nodiscard]] static auto invalid_argument(const std::string_view &) -> Status;
    [[nodiscard]] static auto system_error(const std::string_view &) -> Status;
    [[nodiscard]] static auto logic_error(const std::string_view &) -> Status;
    [[nodiscard]] static auto corruption(const std::string_view &) -> Status;
    [[nodiscard]] static auto not_found(const std::string_view &) -> Status;
    [[nodiscard]] static auto ok() -> Status;
    [[nodiscard]] auto is_invalid_argument() const -> bool;
    [[nodiscard]] auto is_system_error() const -> bool;
    [[nodiscard]] auto is_logic_error() const -> bool;
    [[nodiscard]] auto is_corruption() const -> bool;
    [[nodiscard]] auto is_not_found() const -> bool;
    [[nodiscard]] auto is_ok() const -> bool;
    [[nodiscard]] auto what() const -> std::string_view;

    Status(const Status&);
    auto operator=(const Status&) -> Status&;

    Status(Status&&) noexcept;
    auto operator=(Status&&) noexcept -> Status&;

private:
    enum class Code : Byte {
        INVALID_ARGUMENT = 1,
        SYSTEM_ERROR = 2,
        LOGIC_ERROR = 3,
        CORRUPTION = 4,
        NOT_FOUND = 5,
    };

    Status() = default;
    Status(Code, const std::string_view &);
    [[nodiscard]] auto code() const -> Code;

    std::unique_ptr<char[]> m_data;
};

} // namespace calico

#endif // CALICO_STATUS_H
