#ifndef CALICO_DB_DATABASE_IMPL_H
#define CALICO_DB_DATABASE_IMPL_H

#include "calico/database.h"
#include "spdlog/sinks/basic_file_sink.h"
#include "spdlog/spdlog.h"
#include "utils/header.h"
#include "utils/result.h"
#include "wal/helpers.h"
#include <unordered_set>

namespace calico {

class Cursor;
class Pager;
class Recovery;
class Storage;
class Tree;
class WriteAheadLog;

struct InitialState {
    FileHeader state {};
    bool is_new {}; // TODO: state.page_count == 0?
};

class Core final {
public:
    friend class Database;

    Core() = default;
    ~Core();

    [[nodiscard]] auto open(const std::string &path, const Options &options) -> Status;
    [[nodiscard]] auto close() -> Status;
    [[nodiscard]] auto destroy() -> Status;
    [[nodiscard]] auto transaction() -> Transaction;
    [[nodiscard]] auto status() const -> Status;
    [[nodiscard]] auto path() const -> std::string;
    [[nodiscard]] auto insert(BytesView, BytesView) -> Status;
    [[nodiscard]] auto erase(BytesView) -> Status;
    [[nodiscard]] auto erase(Cursor) -> Status;
    [[nodiscard]] auto commit() -> Status;
    [[nodiscard]] auto abort() -> Status;
    [[nodiscard]] auto find(BytesView) -> Cursor;
    [[nodiscard]] auto find_exact(BytesView) -> Cursor;
    [[nodiscard]] auto first() -> Cursor;
    [[nodiscard]] auto last() -> Cursor;
    [[nodiscard]] auto info() -> Info;

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
    auto forward_status(Status, const std::string &) -> Status;
    auto save_and_forward_status(Status, const std::string &) -> Status;
    [[nodiscard]] auto ensure_consistent_state() -> Status;
    [[nodiscard]] auto atomic_insert(BytesView, BytesView) -> Status;
    [[nodiscard]] auto atomic_erase(Cursor) -> Status;
    [[nodiscard]] auto save_state() -> Status;
    [[nodiscard]] auto load_state() -> Status;

    std::string m_prefix;
    spdlog::sink_ptr m_sink;
    Status m_status {Status::ok()};
    std::shared_ptr<spdlog::logger> m_logger;
    std::unique_ptr<WriteAheadLog> m_wal;
    std::unique_ptr<Pager> m_pager;
    std::unique_ptr<Tree> m_tree;
    std::unique_ptr<Recovery> m_recovery;
    std::unique_ptr<LogScratchManager> m_scratch;
    std::unordered_set<identifier, identifier::hash> m_images;
    identifier m_commit_lsn;
    Storage *m_store {};
    bool m_has_xact {};
    bool m_owns_store {};
};

auto setup(const std::string &, Storage &, const Options &, spdlog::logger &) -> tl::expected<InitialState, Status>;

} // namespace calico

#endif // CALICO_DB_DATABASE_IMPL_H
