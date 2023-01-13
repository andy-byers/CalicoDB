#ifndef CALICO_DATABASE_H
#define CALICO_DATABASE_H

#include "options.h"
#include "slice.h"
#include <memory>

namespace Calico {

class Core;
class Cursor;
class Statistics;
class Status;
class Transaction;

class Database final {
public:
    Database() noexcept;

    [[nodiscard]] auto open(Slice path, const Options &options = {}) -> Status;
    [[nodiscard]] auto close() -> Status;
    [[nodiscard]] auto destroy() && -> Status;
    [[nodiscard]] auto find_exact(Slice key) const -> Cursor;
    [[nodiscard]] auto find(Slice key) const -> Cursor;
    [[nodiscard]] auto first() const -> Cursor;
    [[nodiscard]] auto last() const -> Cursor;
    [[nodiscard]] auto insert(Slice key, Slice value) -> Status;
    [[nodiscard]] auto erase(Slice key) -> Status;
    [[nodiscard]] auto erase(const Cursor &cursor) -> Status;
    [[nodiscard]] auto statistics() const -> Statistics;
    [[nodiscard]] auto status() const -> Status;
    [[nodiscard]] auto transaction() -> Transaction;

    ~Database();

    // NOTE: Necessary because we have a non-trivial destructor.
    Database(Database &&rhs) noexcept;
    Database& operator=(Database &&rhs) noexcept;

private:
    std::unique_ptr<Core> m_core;
};

} // namespace Calico

#endif // CALICO_DATABASE_H
