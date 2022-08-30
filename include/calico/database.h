#ifndef CALICO_DATABASE_H
#define CALICO_DATABASE_H

#include "bytes.h"
#include <memory>

namespace calico {

class Core;
class Cursor;
class Info;
class Status;
class Transaction;
struct Options;

class Database final {
public:
    Database() noexcept;

    [[nodiscard]] static auto destroy(Database) -> Status;
    [[nodiscard]] auto open(const std::string &, const Options & = {}) -> Status;
    [[nodiscard]] auto close() -> Status;
    [[nodiscard]] auto find_exact(BytesView key) const -> Cursor;
    [[nodiscard]] auto find_exact(const std::string &key) const -> Cursor;
    [[nodiscard]] auto find(BytesView key) const -> Cursor;
    [[nodiscard]] auto find(const std::string &key) const -> Cursor;
    [[nodiscard]] auto first() const -> Cursor;
    [[nodiscard]] auto last() const -> Cursor;
    [[nodiscard]] auto insert(BytesView key, BytesView value) -> Status;
    [[nodiscard]] auto insert(const std::string &key, const std::string &value) -> Status;
    [[nodiscard]] auto erase(BytesView key) -> Status;
    [[nodiscard]] auto erase(const std::string &key) -> Status;
    [[nodiscard]] auto erase(const Cursor &cursor) -> Status;
    [[nodiscard]] auto info() const -> Info;
    [[nodiscard]] auto status() const -> Status;
    [[nodiscard]] auto transaction() -> Transaction;

    ~Database();

    // NOTE: Necessary because we have a non-trivial destructor.
    Database(Database&&) noexcept;
    Database& operator=(Database&&) noexcept;

private:
    std::unique_ptr<Core> m_core;
};

} // namespace calico

#endif // CALICO_DATABASE_H
