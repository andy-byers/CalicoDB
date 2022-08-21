#ifndef CALICO_DB_DATABASE_IMPL_H
#define CALICO_DB_DATABASE_IMPL_H

#include "calico/database.h"
#include "utils/result.h"
#include <shared_mutex>
#include <spdlog/sinks/basic_file_sink.h>
#include <spdlog/spdlog.h>

#ifdef CALICO_BUILD_TESTS
#  include <gtest/gtest_prod.h>
#endif

namespace calico {

class Cursor;
class Pager;
class Storage;
class Tree;

struct InitialState {
    FileHeader state;
    bool is_new {};
};

class Core final {
public:
    friend class Database;

    Core(const std::string &path, const Options &options);
    ~Core();

    [[nodiscard]] auto open() -> Status;
    [[nodiscard]] auto status() const -> Status;
    [[nodiscard]] auto path() const -> std::string;
    [[nodiscard]] auto insert(BytesView, BytesView) -> Status;
    [[nodiscard]] auto erase(BytesView) -> Status;
    [[nodiscard]] auto erase(Cursor) -> Status;
    [[nodiscard]] auto commit() -> Status;
    [[nodiscard]] auto abort() -> Status;
    [[nodiscard]] auto close() -> Status;
    [[nodiscard]] auto find(BytesView) -> Cursor;
    [[nodiscard]] auto find_exact(BytesView) -> Cursor;
    [[nodiscard]] auto find_minimum() -> Cursor;
    [[nodiscard]] auto find_maximum() -> Cursor;
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

    [[nodiscard]]
    auto options() const -> const Options&
    {
        return m_options;
    }

private:
    [[nodiscard]] auto forward_status(Status, const std::string &) -> Status;
    [[nodiscard]] auto recover() -> Status;
    [[nodiscard]] auto save_state() -> Status;
    [[nodiscard]] auto load_state() -> Status;

    std::string m_path;
    Options m_options;
    spdlog::sink_ptr m_sink;
    std::shared_ptr<spdlog::logger> m_logger;
    std::unique_ptr<Pager> m_pager;
    std::unique_ptr<Tree> m_tree;
    Storage *m_store {};
    WriteAheadLog *m_wal {};
    bool m_has_update {};
    bool m_owns_store {};
    bool m_owns_wal {};
};

auto setup(const std::string &, Storage &, const Options &, spdlog::logger &) -> Result<InitialState>;

} // namespace cco

#endif // CALICO_DB_DATABASE_IMPL_H
