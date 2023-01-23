#ifndef CALICO_STATUS_H
#define CALICO_STATUS_H

#include <memory>
#include "slice.h"

namespace Calico {

class Status final {
public:
    /*
     * Create an OK status.
     */
    [[nodiscard]] static auto ok() -> Status;

    /*
     * Create a non-OK status with an error message.
     */
    [[nodiscard]] static auto invalid_argument(const Slice &what) -> Status;
    [[nodiscard]] static auto system_error(const Slice &what) -> Status;
    [[nodiscard]] static auto logic_error(const Slice &what) -> Status;
    [[nodiscard]] static auto corruption(const Slice &what) -> Status;
    [[nodiscard]] static auto not_found(const Slice &what) -> Status;

    /*
     * Check status type.
     */
    [[nodiscard]] auto is_ok() const -> bool;
    [[nodiscard]] auto is_invalid_argument() const -> bool;
    [[nodiscard]] auto is_system_error() const -> bool;
    [[nodiscard]] auto is_logic_error() const -> bool;
    [[nodiscard]] auto is_corruption() const -> bool;
    [[nodiscard]] auto is_not_found() const -> bool;

    /*
     * Get the error message, if it exists.
     */
    [[nodiscard]] auto what() const -> Slice;

    // Status can be copied and moved.
    Status(const Status &rhs);
    auto operator=(const Status &rhs) -> Status &;
    Status(Status &&rhs) noexcept;
    auto operator=(Status &&rhs) noexcept -> Status &;

private:
    enum class Code : Byte {
        INVALID_ARGUMENT = 1,
        SYSTEM_ERROR = 2,
        LOGIC_ERROR = 3,
        CORRUPTION = 4,
        NOT_FOUND = 5,
    };

    // Construct an OK status. No allocation is needed.
    Status() = default;

    // Construct a non-OK status.
    Status(Code code, const Slice &what);

    // Storage for a status code and a message.
    std::unique_ptr<Byte[]> m_data;
};

// Status object should be the size of a pointer.
static_assert(sizeof(Status) == sizeof(void *));

} // namespace Calico

#endif // CALICO_STATUS_H
