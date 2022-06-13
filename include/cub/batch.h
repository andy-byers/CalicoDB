#ifndef CUB_WRITER_H
#define CUB_WRITER_H

#include <memory>
#include "bytes.h"

namespace cub {

class Batch {
public:
    virtual ~Batch();
    [[nodiscard]] auto transaction_size() const -> Size;
    auto read(BytesView, bool) const -> std::optional<Record>;
    auto read_minimum() const -> std::optional<Record>;
    auto read_maximum() const -> std::optional<Record>;
    auto write(BytesView, BytesView) -> bool;
    auto erase(BytesView) -> bool;
    auto commit() -> void;
    auto abort() -> void;

    Batch(Batch &&) noexcept;
    Batch &operator=(Batch &&) noexcept;

private:
    friend class Database;
    Batch();

    class Impl;
    std::unique_ptr<Impl> m_impl;
};

} // cub

#endif // CUB_WRITER_H
