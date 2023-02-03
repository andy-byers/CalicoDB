#ifndef CALICO_CURSOR_H
#define CALICO_CURSOR_H

#include <memory>
#include <optional>
#include "status.h"

namespace Calico {

class Cursor {
public:
    Cursor() = default;
    virtual ~Cursor() = default;

    [[nodiscard]] virtual auto is_valid() const -> bool = 0;
    [[nodiscard]] virtual auto status() const -> Status = 0;
    [[nodiscard]] virtual auto key() const -> Slice = 0;
    [[nodiscard]] virtual auto value() const -> Slice = 0;

    virtual auto seek(const Slice &key) -> void = 0;
    virtual auto seek_first() -> void = 0;
    virtual auto seek_last() -> void = 0;
    virtual auto next() -> void = 0;
    virtual auto previous() -> void = 0;

    virtual auto operator==(const Cursor &rhs) const -> bool = 0;
    virtual auto operator!=(const Cursor &rhs) const -> bool = 0;
};

} // namespace Calico

#endif // CALICO_CURSOR_H
