#ifndef CALICO_CORE_DATABASE_IMPL_H
#define CALICO_CORE_DATABASE_IMPL_H

#include "calico/database.h"
#include "recovery.h"
#include <unordered_set>
#include <spdlog/sinks/basic_file_sink.h>
#include <spdlog/spdlog.h>
#include "tree/header.h"
#include "utils/expected.hpp"
#include "wal/wal.h"

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

class DatabaseImpl: public Database {
public:
    friend class Database;

    DatabaseImpl() = default;
    virtual ~DatabaseImpl() = default;

    [[nodiscard]] static auto destroy(const std::string &path, const Options &options) -> Status;
    [[nodiscard]] auto open(const Slice &path, const Options &options) -> Status;
    [[nodiscard]] auto close() -> Status;

    [[nodiscard]] auto status() const -> Status override;
    [[nodiscard]] auto put(const Slice &key, const Slice &value) -> Status override;
    [[nodiscard]] auto erase(const Slice &key) -> Status override;
    [[nodiscard]] auto commit() -> Status override;
    [[nodiscard]] auto abort() -> Status override;
    [[nodiscard]] auto get(const Slice &key, std::string &out) const -> Status override;
    [[nodiscard]] auto new_cursor() const -> Cursor * override;
    [[nodiscard]] auto get_property(const Slice &name) const -> std::string override;

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

    mutable Status m_status {ok()};
    std::string m_prefix;
    LogPtr m_log;
    std::unique_ptr<Recovery> m_recovery;
    std::unique_ptr<LogScratchManager> m_scratch;
    Storage *m_storage {};
    Size m_txn_number {1};
    bool m_owns_storage {};
};

auto setup(const std::string &, Storage &, const Options &) -> tl::expected<InitialState, Status>;

} // namespace Calico

#endif // CALICO_CORE_DATABASE_IMPL_H
