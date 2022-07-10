#ifndef CALICO_ERROR_H
#define CALICO_ERROR_H

#include "bytes.h"

namespace calico {

class Error final {
public:
    static auto invalid_argument(const std::string&) -> Error;
    static auto system_error(const std::string&) -> Error;
    static auto logic_error(const std::string&) -> Error;
    static auto corruption(const std::string&) -> Error;

    [[nodiscard]] auto is_invalid_argument() const -> bool;
    [[nodiscard]] auto is_system_error() const -> bool;
    [[nodiscard]] auto is_logic_error() const -> bool;
    [[nodiscard]] auto is_corruption() const -> bool;

    [[nodiscard]] auto what() const -> BytesView;

private:
    enum class Code: Byte {
        INVALID_ARGUMENT = 1,
        SYSTEM_ERROR = 2,
        LOGIC_ERROR = 3,
        CORRUPTION = 4,
    };

    Error(Code, const std::string&);
    [[nodiscard]] auto code() const -> Code;

    std::string m_what;
};

struct None {};

/// Return type for routines that can fail.
template<class T>
using Result = tl::expected<T, Error>;

using ErrorResult = tl::unexpected<Error>;

} // calico

#endif //CALICO_ERROR_H
