#ifndef CCO_DB_DATABASE_IMPL_H
#define CCO_DB_DATABASE_IMPL_H

#include "calico/database.h"
#include "page/file_header.h"
#include "utils/result.h"
#include <shared_mutex>
#include <spdlog/sinks/basic_file_sink.h>
#include <spdlog/spdlog.h>

namespace cco {

class Cursor;
class BufferPool;
class Storage;
class IFile;
class Tree;

struct InitialState {
    std::string state;
    Options revised;
    bool is_new {};
};

class Database::Impl final {
public:
    struct Parameters {
        spdlog::sink_ptr sink;
        Options options;
    };

    friend class Database;

    [[nodiscard]] static auto open(Parameters, std::unique_ptr<Storage>) -> Result<std::unique_ptr<Impl>>;
    [[nodiscard]] static auto open(Parameters) -> Result<std::unique_ptr<Impl>>;
    Impl() = default;
    ~Impl();
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

    [[nodiscard]] auto home() -> Storage &
    {
        return *m_home;
    }

    [[nodiscard]] auto home() const -> const Storage &
    {
        return *m_home;
    }

private:
    [[nodiscard]] auto save_header() -> Result<void>;
    [[nodiscard]] auto load_header() -> Result<void>;

    spdlog::sink_ptr m_sink;
    std::shared_ptr<spdlog::logger> m_logger;
    std::unique_ptr<Storage> m_home;
    std::unique_ptr<BufferPool> m_pool;
    std::unique_ptr<Tree> m_tree;
    Status m_status {Status::ok()};
    bool m_is_temp {};
};

auto setup(Storage &, const Options &, spdlog::logger &) -> Result<InitialState>;

} // namespace cco

#endif // CCO_DB_DATABASE_IMPL_H
