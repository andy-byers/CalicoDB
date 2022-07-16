#ifndef CCO_ERROR_H
#define CCO_ERROR_H

#include "bytes.h"

namespace cco {

class Status final {
public:
    static auto invalid_argument(const std::string&) -> Status;
    static auto system_error(const std::string&) -> Status;
    static auto logic_error(const std::string&) -> Status;
    static auto corruption(const std::string&) -> Status;
    static auto not_found() -> Status;
    static auto ok() -> Status;
    [[nodiscard]] auto is_invalid_argument() const -> bool;
    [[nodiscard]] auto is_system_error() const -> bool;
    [[nodiscard]] auto is_logic_error() const -> bool;
    [[nodiscard]] auto is_corruption() const -> bool;
    [[nodiscard]] auto is_not_found() const -> bool;
    [[nodiscard]] auto is_ok() const -> bool;
    [[nodiscard]] auto what() const -> std::string;

private:
    enum class Code : Byte {
        OK = 0,
        INVALID_ARGUMENT = 1,
        SYSTEM_ERROR = 2,
        LOGIC_ERROR = 3,
        CORRUPTION = 4,
        NOT_FOUND = 5,
    };

    explicit Status(Code);
    Status(Code, const std::string&);
    [[nodiscard]] auto code() const -> Code;

    std::string m_what;
};

template<class T>
using Result = tl::expected<T, Status>;
using Err = tl::unexpected<Status>;

} // calico

#endif //CCO_ERROR_H
