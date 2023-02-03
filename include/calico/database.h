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

class Database {
public:
    virtual ~Database();
    [[nodiscard]] static auto open(const Slice &path, const Options &options, Database **db) -> Status;
    [[nodiscard]] static auto destroy(const Slice &path, const Options &options) -> Status;

    [[nodiscard]] virtual auto new_cursor() const -> Cursor * = 0;
    [[nodiscard]] virtual auto transaction() -> Transaction & = 0;

    [[nodiscard]] virtual auto get(const Slice &key, std::string &out) const -> Status = 0;
    [[nodiscard]] virtual auto put(const Slice &key, const Slice &value) -> Status = 0;
    [[nodiscard]] virtual auto erase(const Slice &key) -> Status = 0;
    [[nodiscard]] virtual auto statistics() const -> Statistics = 0;
    [[nodiscard]] virtual auto status() const -> Status = 0;
};

} // namespace Calico

#endif // CALICO_DATABASE_H
