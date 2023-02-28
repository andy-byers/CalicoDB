#ifndef CALICO_DATABASE_IMPL_H
#define CALICO_DATABASE_IMPL_H

#include "calico/db.h"

#include "pager/pager.h"
#include "recovery.h"
#include "tree/header.h"
#include "tree/tree.h"
#include "wal/wal.h"
#include "wal/writer.h"

namespace calicodb
{

class Cursor;
class Recovery;
class Storage;
class WriteAheadLog;

class DBImpl : public DB
{
public:
    friend class DB;

    DBImpl() = default;
    ~DBImpl() override;

    [[nodiscard]] static auto destroy(const std::string &path, const Options &options) -> Status;
    [[nodiscard]] static auto repair(const std::string &path, const Options &options) -> Status;
    [[nodiscard]] auto open(const Slice &path, const Options &options) -> Status;

    [[nodiscard]] auto new_cursor() const -> Cursor * override;
    [[nodiscard]] auto get_property(const Slice &name, std::string &out) const -> bool override;
    [[nodiscard]] auto status() const -> Status override;
    [[nodiscard]] auto vacuum() -> Status override;
    [[nodiscard]] auto commit() -> Status override;
    [[nodiscard]] auto get(const Slice &key, std::string &out) const -> Status override;
    [[nodiscard]] auto put(const Slice &key, const Slice &value) -> Status override;
    [[nodiscard]] auto erase(const Slice &key) -> Status override;

    [[nodiscard]] auto record_count() const -> Size
    {
        return m_record_count;
    }

    auto TEST_validate() const -> void;

    WriteAheadLog *wal {};
    BPlusTree *tree {};
    Pager *pager {};

private:
    [[nodiscard]] auto do_open(Options sanitized) -> Status;
    [[nodiscard]] auto ensure_consistency() -> Status;
    [[nodiscard]] auto save_state(Page root, Lsn commit_lsn) const -> Status;
    [[nodiscard]] auto load_state() -> Status;
    [[nodiscard]] auto do_commit() -> Status;
    [[nodiscard]] auto do_vacuum() -> Status;

    mutable Status m_status;
    std::string m_db_prefix;
    std::string m_wal_prefix;
    std::string m_scratch;
    Storage *m_storage {};
    InfoLogger *m_info_log {};
    Size m_txn_size {};
    Size m_record_count {};
    Size m_bytes_written {};
    Lsn m_commit_lsn;
    bool m_in_txn {true};
    bool m_owns_storage {};
    bool m_owns_info_log {};
    bool m_is_setup {};
};

auto setup(const std::string &, Storage &, const Options &, FileHeader &state) -> Status;

} // namespace calicodb

#endif // CALICO_DATABASE_IMPL_H
