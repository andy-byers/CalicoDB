#ifndef CALICO_STATUS_H
#define CALICO_STATUS_H

#include <memory>
#include <spdlog/fmt/fmt.h>
#include "bytes.h"

namespace calico {

class Status final {
public:
    [[nodiscard]] static auto ok() -> Status;
    [[nodiscard]] static auto invalid_argument(const std::string_view &) -> Status;
    [[nodiscard]] static auto system_error(const std::string_view &) -> Status;
    [[nodiscard]] static auto logic_error(const std::string_view &) -> Status;
    [[nodiscard]] static auto corruption(const std::string_view &) -> Status;
    [[nodiscard]] static auto not_found(const std::string_view &) -> Status;

    [[nodiscard]] auto is_ok() const -> bool;
    [[nodiscard]] auto is_invalid_argument() const -> bool;
    [[nodiscard]] auto is_system_error() const -> bool;
    [[nodiscard]] auto is_logic_error() const -> bool;
    [[nodiscard]] auto is_corruption() const -> bool;
    [[nodiscard]] auto is_not_found() const -> bool;

    [[nodiscard]] auto what() const -> std::string_view;

    Status(const Status &);
    auto operator=(const Status &) -> Status &;

    Status(Status &&) noexcept;
    auto operator=(Status &&) noexcept -> Status &;

    template<class ...Ts>
    [[nodiscard]]
    static auto invalid_argument(const std::string_view &fmt, Ts &&...ts) -> Status
    {
        return invalid_argument(fmt::format(fmt::runtime(fmt), std::forward<Ts>(ts)...).c_str());
    }

    template<class ...Ts>
    [[nodiscard]]
    static auto system_error(const std::string_view &fmt, Ts &&...ts) -> Status
    {
        return system_error(fmt::format(fmt::runtime(fmt), std::forward<Ts>(ts)...).c_str());
    }

    template<class ...Ts>
    [[nodiscard]]
    static auto logic_error(const std::string_view &fmt, Ts &&...ts) -> Status
    {
        return logic_error(fmt::format(fmt::runtime(fmt), std::forward<Ts>(ts)...).c_str());
    }

    template<class ...Ts>
    [[nodiscard]]
    static auto corruption(const std::string_view &fmt, Ts &&...ts) -> Status
    {
        return corruption(fmt::format(fmt::runtime(fmt), std::forward<Ts>(ts)...).c_str());
    }

    template<class ...Ts>
    [[nodiscard]]
    static auto not_found(const std::string_view &fmt, Ts &&...ts) -> Status
    {
        return not_found(fmt::format(fmt::runtime(fmt), std::forward<Ts>(ts)...).c_str());
    }

private:
    enum class code : Byte {
        INVALID_ARGUMENT = 1,
        SYSTEM_ERROR = 2,
        LOGIC_ERROR = 3,
        CORRUPTION = 4,
        NOT_FOUND = 5,
    };

    // Construct an OK status. No allocation is needed.
    Status() = default;

    // Construct a non-OK status.
    Status(code, const std::string_view &);

    // Storage for a status code and a message.
    std::unique_ptr<char[]> m_data;
};

// status should be the size of a pointer.
static_assert(sizeof(Status) == sizeof(char *));

} // namespace calico

#endif // CALICO_STATUS_H
