#ifndef CALICO_DB_DATABASE_IMPL_H
#define CALICO_DB_DATABASE_IMPL_H

#include "calico/database.h"
#include "spdlog/sinks/basic_file_sink.h"
#include "spdlog/spdlog.h"
#include "utils/header.h"
#include "utils/expected.hpp"
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
    [[nodiscard]] auto bytes_written() const -> Size;
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

    std::unique_ptr<WriteAheadLog> wal;
    std::unique_ptr<Pager> pager;
    std::unique_ptr<Tree> tree;

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
    std::unique_ptr<Recovery> m_recovery;
    std::unique_ptr<LogScratchManager> m_scratch;
    Storage *m_store {};
    Size m_bytes_written {};
    bool m_owns_store {};
};

auto setup(const std::string &, Storage &, const Options &) -> tl::expected<InitialState, Status>;

} // namespace Calico

#endif // CALICO_DB_DATABASE_IMPL_H
