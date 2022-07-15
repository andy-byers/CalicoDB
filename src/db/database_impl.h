#ifndef CCO_DB_DATABASE_IMPL_H
#define CCO_DB_DATABASE_IMPL_H

#include <shared_mutex>
#include <spdlog/spdlog.h>
#include <spdlog/sinks/basic_file_sink.h>
#include "calico/database.h"
#include "page/file_header.h"

namespace cco {

class Cursor;
class IBufferPool;
class IDirectory;
class IFile;
class ITree;

struct InitialState {
    page::FileHeader state;
    Options revised;
    bool is_new {};
};

class Database::Impl final {
public:
    struct Parameters {
        std::unique_ptr<IDirectory> home;
        spdlog::sink_ptr sink;
        page::FileHeader state;
        Options options;
    };

    friend class Database;

    /**
     * Tag for in-memory database constructor overload.
     */
    struct InMemoryTag {};

    explicit Impl(Parameters);
    Impl(Parameters, InMemoryTag);
    ~Impl();
    [[nodiscard]] auto path() const -> std::string;
    [[nodiscard]] auto cache_hit_ratio() const -> double;
    [[nodiscard]] auto record_count() const -> Size;
    [[nodiscard]] auto page_count() const -> Size;
    [[nodiscard]] auto page_size() const -> Size;
    [[nodiscard]] auto is_temp() const -> bool;
    [[nodiscard]] auto insert(BytesView, BytesView) -> Result<bool>;
    [[nodiscard]] auto erase(BytesView) -> Result<bool>;
    [[nodiscard]] auto erase(Cursor) -> Result<bool>;
    [[nodiscard]] auto commit() -> Result<void>;
    [[nodiscard]] auto close() -> Result<void>;
    auto find(BytesView) -> Cursor;
    auto find_exact(BytesView) -> Cursor;
    auto find_minimum() -> Cursor;
    auto find_maximum() -> Cursor;
    auto info() -> Info;

private:
    [[nodiscard]] auto save_header() -> Result<void>;
    [[nodiscard]] auto load_header() -> Result<void>;

    spdlog::sink_ptr m_sink;
    std::shared_ptr<spdlog::logger> m_logger;
    std::unique_ptr<IDirectory> m_home;
    std::unique_ptr<IBufferPool> m_pool;
    std::unique_ptr<ITree> m_tree;
    bool m_is_temp {};
};

auto setup(IDirectory&, const Options&, spdlog::logger&) -> Result<InitialState>;

} // calico

#endif // CCO_DB_DATABASE_IMPL_H
