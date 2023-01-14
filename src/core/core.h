#ifndef CALICO_DB_DATABASE_IMPL_H
#define CALICO_DB_DATABASE_IMPL_H

#include "calico/database.h"
#include "spdlog/sinks/basic_file_sink.h"
#include "spdlog/spdlog.h"
#include "utils/header.h"
#include <expected.hpp>
#include "wal/helpers.h"
#include <unordered_set>

namespace Calico {

class Cursor;
class Pager;
class Recovery;
class Storage;
class Tree;
class WriteAheadLog;

struct InitialState {
    FileHeader state {};
    bool is_new {};
};

class Core final {
public:
    friend class Database;

    Core() = default;
    ~Core();

    [[nodiscard]] auto open(Slice path, const Options &options) -> Status;
    [[nodiscard]] auto close() -> Status;
    [[nodiscard]] auto destroy() -> Status;
    [[nodiscard]] auto transaction() -> Transaction;
    [[nodiscard]] auto status() const -> Status;
    [[nodiscard]] auto path() const -> std::string;
    [[nodiscard]] auto insert(Slice, Slice) -> Status;
    [[nodiscard]] auto erase(Slice) -> Status;
    [[nodiscard]] auto erase(const Cursor &) -> Status;
    [[nodiscard]] auto commit() -> Status;
    [[nodiscard]] auto abort() -> Status;
    [[nodiscard]] auto find(Slice) -> Cursor;
    [[nodiscard]] auto find_exact(Slice) -> Cursor;
    [[nodiscard]] auto first() -> Cursor;
    [[nodiscard]] auto last() -> Cursor;
    [[nodiscard]] auto statistics() -> Statistics;

    [[nodiscard]]
    auto store() -> Storage&
    {
        return *m_store;
    }

    [[nodiscard]]
    auto store() const -> const Storage&
    {
        return *m_store;
    }

    [[nodiscard]]
    auto wal() -> WriteAheadLog&
    {
        return *m_wal;
    }

    [[nodiscard]]
    auto wal() const -> const WriteAheadLog&
    {
        return *m_wal;
    }

    [[nodiscard]]
    auto tree() -> Tree&
    {
        return *m_tree;
    }

    [[nodiscard]]
    auto tree() const -> const Tree&
    {
        return *m_tree;
    }

    [[nodiscard]]
    auto pager() -> Pager&
    {
        return *m_pager;
    }

    [[nodiscard]]
    auto pager() const -> const Pager&
    {
        return *m_pager;
    }

private:
    auto handle_errors() -> Status;
    [[nodiscard]] auto do_open(Options sanitized) -> Status;
    [[nodiscard]] auto ensure_consistency_on_startup() -> Status;
    [[nodiscard]] auto atomic_insert(Slice, Slice) -> Status;
    [[nodiscard]] auto atomic_erase(const Cursor &) -> Status;
    [[nodiscard]] auto save_state() -> Status;
    [[nodiscard]] auto load_state() -> Status;
    [[nodiscard]] auto do_commit() -> Status;
    [[nodiscard]] auto do_abort() -> Status;

    std::string m_prefix;
    LogPtr m_log;
    std::unique_ptr<System> m_system;
    std::unique_ptr<WriteAheadLog> m_wal;
    std::unique_ptr<Pager> m_pager;
    std::unique_ptr<Tree> m_tree;
    std::unique_ptr<Recovery> m_recovery;
    std::unique_ptr<LogScratchManager> m_scratch;
    std::unordered_set<Id, Id::Hash> m_images;
    Storage *m_store {};
    bool m_owns_store {};
};

auto setup(const std::string &, Storage &, const Options &) -> tl::expected<InitialState, Status>;

} // namespace Calico

#endif // CALICO_DB_DATABASE_IMPL_H
