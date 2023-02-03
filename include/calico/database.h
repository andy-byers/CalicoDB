#ifndef CALICO_DATABASE_H
#define CALICO_DATABASE_H

namespace Calico {

struct Options;
class Cursor;
class Slice;
class Status;

class Database {
public:
    virtual ~Database();
    [[nodiscard]] static auto open(const Slice &path, const Options &options, Database **db) -> Status;
    [[nodiscard]] static auto destroy(const Slice &path, const Options &options) -> Status;

    [[nodiscard]] virtual auto get_property(const Slice &name) const -> std::string = 0;
    [[nodiscard]] virtual auto new_cursor() const -> Cursor * = 0;

    [[nodiscard]] virtual auto status() const -> Status = 0;
    [[nodiscard]] virtual auto commit() -> Status = 0;
    [[nodiscard]] virtual auto abort() -> Status = 0;

    [[nodiscard]] virtual auto get(const Slice &key, std::string &out) const -> Status = 0;
    [[nodiscard]] virtual auto put(const Slice &key, const Slice &value) -> Status = 0;
    [[nodiscard]] virtual auto erase(const Slice &key) -> Status = 0;
};

} // namespace Calico

#endif // CALICO_DATABASE_H
