#ifndef CCO_ERROR_H
#define CCO_ERROR_H

#include "bytes.h"

namespace cco {

class Error final {
public:
    static auto invalid_argument(const std::string&) -> Error;
    static auto system_error(const std::string&) -> Error;
    static auto logic_error(const std::string&) -> Error;
    static auto corruption(const std::string&) -> Error;
    static auto not_found(const std::string&) -> Error;
    [[nodiscard]] auto is_invalid_argument() const -> bool;
    [[nodiscard]] auto is_system_error() const -> bool;
    [[nodiscard]] auto is_logic_error() const -> bool;
    [[nodiscard]] auto is_corruption() const -> bool;
    [[nodiscard]] auto is_not_found() const -> bool;
    [[nodiscard]] auto what() const -> BytesView;

private:
    enum class Code: Byte {
        INVALID_ARGUMENT = 1,
        SYSTEM_ERROR = 2,
        LOGIC_ERROR = 3,
        CORRUPTION = 4,
        NOT_FOUND = 5,
    };

    Error(Code, const std::string&);
    [[nodiscard]] auto code() const -> Code;

    std::string m_what;
};

template<class T>
using Result = tl::expected<T, Error>;
using Err = tl::unexpected<Error>;

} // calico

#endif //CCO_ERROR_H
