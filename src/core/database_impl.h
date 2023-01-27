#ifndef CALICO_DB_DATABASE_IMPL_H
#define CALICO_DB_DATABASE_IMPL_H

#include "calico/database.h"
#include "spdlog/sinks/basic_file_sink.h"
#include "spdlog/spdlog.h"
#include "tree/header.h"
#include "utils/expected.hpp"
#include "wal/helpers.h"
#include <unordered_set>

namespace Calico {

class Cursor;
class Pager;
class Recovery;
class Storage;
class BPlusTree;
class WriteAheadLog;

struct InitialState {
    FileHeader state;
    bool is_new {};
};

class DatabaseImpl final {
public:
    friend class Database;

    DatabaseImpl() = default;
    ~DatabaseImpl();

    [[nodiscard]] auto open(const Slice &path, const Options &options) -> Status;
    [[nodiscard]] auto close() -> Status;
    [[nodiscard]] auto destroy() -> Status;
    [[nodiscard]] auto start() -> Transaction;
    [[nodiscard]] auto status() const -> Status;
    [[nodiscard]] auto path() const -> std::string;
    [[nodiscard]] auto put(const Slice &key, const Slice &value) -> Status;
    [[nodiscard]] auto erase(const Slice &key) -> Status;
    [[nodiscard]] auto commit() -> Status;
    [[nodiscard]] auto abort() -> Status;
    [[nodiscard]] auto get(const Slice &key, std::string &out) const -> Status;
    [[nodiscard]] auto cursor() const -> Cursor;
    [[nodiscard]] auto statistics() -> Statistics;

    std::unique_ptr<System> system;
    std::unique_ptr<WriteAheadLog> wal;
    std::unique_ptr<Pager> pager;
    std::unique_ptr<BPlusTree> tree;

    Size bytes_written {};
    Size record_count {};
    Size maximum_key_size {};

private:
    [[nodiscard]] auto check_key(const Slice &key, const char *message) const -> Status;
    [[nodiscard]] auto do_open(Options sanitized) -> Status;
    [[nodiscard]] auto ensure_consistency_on_startup() -> Status;
    [[nodiscard]] auto atomic_insert(const Slice &key, const Slice &value) -> Status;
    [[nodiscard]] auto atomic_erase(const Slice &key) -> Status;
    [[nodiscard]] auto save_state() -> Status;
    [[nodiscard]] auto load_state() -> Status;
    [[nodiscard]] auto do_commit() -> Status;
    [[nodiscard]] auto do_abort() -> Status;

    std::string m_prefix;
    LogPtr m_log;
    std::unique_ptr<Recovery> m_recovery;
    std::unique_ptr<LogScratchManager> m_scratch;
    Storage *m_store {};
    bool m_owns_store {};
};

auto setup(const std::string &, Storage &, const Options &) -> tl::expected<InitialState, Status>;

} // namespace Calico

#endif // CALICO_DB_DATABASE_IMPL_H
