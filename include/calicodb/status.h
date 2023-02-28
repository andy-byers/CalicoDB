#ifndef CALICODB_STATUS_H
#define CALICODB_STATUS_H

#include <memory>

namespace calicodb
{

class Slice;

class Status final
{
public:
    explicit Status() = default;

    /*
     * Create/check for an OK status.
     */
    static auto ok() -> Status
    {
        return Status {};
    }

    [[nodiscard]] auto is_ok() const -> bool
    {
        return m_data == nullptr;
    }

    /*
     * Create a non-OK status with an error message.
     */
    [[nodiscard]] static auto invalid_argument(const Slice &what) -> Status;
    [[nodiscard]] static auto system_error(const Slice &what) -> Status;
    [[nodiscard]] static auto logic_error(const Slice &what) -> Status;
    [[nodiscard]] static auto corruption(const Slice &what) -> Status;
    [[nodiscard]] static auto not_found(const Slice &what) -> Status;

    /*
     * Check error status type.
     */
    [[nodiscard]] auto is_invalid_argument() const -> bool;
    [[nodiscard]] auto is_system_error() const -> bool;
    [[nodiscard]] auto is_logic_error() const -> bool;
    [[nodiscard]] auto is_corruption() const -> bool;
    [[nodiscard]] auto is_not_found() const -> bool;

    /*
     * Convert the status to a printable string.
     */
    [[nodiscard]] auto to_string() const -> std::string;

    // Status can be copied and moved.
    Status(const Status &rhs);
    auto operator=(const Status &rhs) -> Status &;
    Status(Status &&rhs) noexcept;
    auto operator=(Status &&rhs) noexcept -> Status &;

private:
    // Construct a non-OK status.
    explicit Status(char code, const Slice &what);

    // Storage for a status code and a null-terminated message.
    std::unique_ptr<char[]> m_data;
};

// Status object should be the size of a pointer.
static_assert(sizeof(Status) == sizeof(void *));

} // namespace calicodb

#endif // CALICODB_STATUS_H
