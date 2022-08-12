#ifndef CCO_DB_DATABASE_IMPL_H
#define CCO_DB_DATABASE_IMPL_H

#include "calico/database.h"
#include "utils/result.h"
#include <shared_mutex>
#include <spdlog/sinks/basic_file_sink.h>
#include <spdlog/spdlog.h>

namespace cco {

class Cursor;
class Pager;
class Storage;
class Tree;

struct InitialState {
    FileHeader state;
    Options options;
    bool is_new {};
    bool owns_store {};
    bool owns_wal {};
};

class Core final {
public:
    friend class Database;

    Core(const std::string &path, const Options &options);
    ~Core();

    [[nodiscard]] auto open() -> Status;
    [[nodiscard]] auto can_commit() const -> bool;
    [[nodiscard]] auto status() const -> Status;
    [[nodiscard]] auto path() const -> std::string;
    [[nodiscard]] auto cache_hit_ratio() const -> double;
    [[nodiscard]] auto record_count() const -> Size;
    [[nodiscard]] auto page_count() const -> Size;
    [[nodiscard]] auto page_size() const -> Size;
    [[nodiscard]] auto is_temp() const -> bool;
    [[nodiscard]] auto uses_xact() const -> bool;
    [[nodiscard]] auto insert(BytesView, BytesView) -> Result<bool>;
    [[nodiscard]] auto erase(BytesView) -> Result<bool>;
    [[nodiscard]] auto erase(Cursor) -> Result<bool>;
    [[nodiscard]] auto commit() -> Result<void>;
    [[nodiscard]] auto abort() -> Result<void>;
    [[nodiscard]] auto close() -> Result<void>;
    [[nodiscard]] auto find(BytesView) -> Cursor;
    [[nodiscard]] auto find_exact(BytesView) -> Cursor;
    [[nodiscard]] auto find_minimum() -> Cursor;
    [[nodiscard]] auto find_maximum() -> Cursor;
    [[nodiscard]] auto info() -> Info;

private:
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
    bool m_owns_store {};
    bool m_owns_wal {};
};

auto setup(Storage &, const Options &, spdlog::logger &) -> Result<SanitizedOptions>;

} // namespace cco

#endif // CCO_DB_DATABASE_IMPL_H
