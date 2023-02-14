#ifndef CALICO_DATABASE_IMPL_H
#define CALICO_DATABASE_IMPL_H

#include "calico/database.h"

#include "recovery.h"
#include "pager/pager.h"
#include "tree/header.h"
#include "tree/tree.h"
#include "utils/expected.hpp"
#include "wal/wal.h"
#include "wal/cleanup.h"
#include "wal/writer.h"

namespace Calico {

class Cursor;
class Recovery;
class Storage;
class WriteAheadLog;

struct InitialState {
    FileHeader state;
    bool is_new {};
};

class DatabaseImpl: public Database {
public:
    friend class Database;

    DatabaseImpl() = default;
    ~DatabaseImpl() override;

    [[nodiscard]] static auto destroy(const std::string &path, const Options &options) -> Status;
    [[nodiscard]] static auto repair(const std::string &path, const Options &options) -> Status;
    [[nodiscard]] auto open(const Slice &path, const Options &options) -> Status;

    [[nodiscard]] auto new_cursor() const -> Cursor * override;
    [[nodiscard]] auto get_property(const Slice &name, std::string &out) const -> bool override;
    [[nodiscard]] auto status() const -> Status override;
    [[nodiscard]] auto vacuum() -> Status override;
    [[nodiscard]] auto commit() -> Status override;
    [[nodiscard]] auto abort() -> Status override;
    [[nodiscard]] auto get(const Slice &key, std::string &out) const -> Status override;
    [[nodiscard]] auto put(const Slice &key, const Slice &value) -> Status override;
    [[nodiscard]] auto erase(const Slice &key) -> Status override;

    auto TEST_validate() const -> void;

    std::unique_ptr<WriteAheadLog> wal;
    std::unique_ptr<Pager> pager;
    std::unique_ptr<BPlusTree> tree;

    Size bytes_written {};
    Size record_count {};
    Size max_key_length {};

private:
    [[nodiscard]] auto check_key(const Slice &key) const -> Status;
    [[nodiscard]] auto do_open(Options sanitized) -> Status;
    [[nodiscard]] auto ensure_consistency_on_startup() -> Status;
    [[nodiscard]] auto save_state() const -> Status;
    [[nodiscard]] auto load_state() -> Status;
    [[nodiscard]] auto do_commit() -> Status;
    [[nodiscard]] auto do_abort() -> Status;

    mutable Status m_status;
    std::string m_db_prefix;
    std::string m_wal_prefix;
    std::unique_ptr<Recovery> m_recovery;
    std::string m_scratch;
    Storage *m_storage {};
    Logger *m_info_log {};
    Size m_txn_size {};
    Lsn m_commit_lsn;
    bool m_in_txn {true};
    bool m_owns_storage {};
    bool m_owns_info_log {};
    bool m_sync {};
};

auto setup(const std::string &, Storage &, const Options &) -> tl::expected<InitialState, Status>;

} // namespace Calico

#endif // CALICO_DATABASE_IMPL_H
