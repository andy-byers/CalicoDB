#ifndef CUB_BATCH_H
#define CUB_BATCH_H

#include <memory>
#include <shared_mutex>
#include "bytes.h"

namespace cub {

class Token {
public:
    virtual ~Token() = default;

    explicit Token(std::shared_mutex &mutex)
        : m_lock {mutex} {}

private:
    std::unique_lock<std::shared_mutex> m_lock;
};

class Batch {
public:
    virtual ~Batch();
    [[nodiscard]] auto transaction_size() const -> Size;
    [[nodiscard]] auto read(BytesView, Comparison) const -> std::optional<Record>;
    [[nodiscard]] auto read_minimum() const -> std::optional<Record>;
    [[nodiscard]] auto read_maximum() const -> std::optional<Record>;
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

#endif // CUB_BATCH_H
