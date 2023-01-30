#ifndef CALICO_DATABASE_H
#define CALICO_DATABASE_H

#include "options.h"
#include "slice.h"
#include "statistics.h"
#include <memory>

namespace Calico {

class Cursor;
class DatabaseImpl;
class Status;
class Transaction;

class Database final {
public:
    Database();
    ~Database();

    [[nodiscard]] auto open(const Slice &path, const Options &options = {}) -> Status;
    [[nodiscard]] auto close() -> Status;
    [[nodiscard]] auto destroy() && -> Status;
    [[nodiscard]] auto cursor() const -> Cursor;
    [[nodiscard]] auto start() -> Transaction;
    [[nodiscard]] auto get(const Slice &key, std::string &out) const -> Status;
    [[nodiscard]] auto put(const Slice &key, const Slice &value) -> Status;
    [[nodiscard]] auto erase(const Slice &key) -> Status;
    [[nodiscard]] auto statistics() const -> Statistics;
    [[nodiscard]] auto status() const -> Status;

private:
    friend class DatabaseImpl;

    const std::unique_ptr<DatabaseImpl> m_impl;
};

} // namespace Calico

#endif // CALICO_DATABASE_H
