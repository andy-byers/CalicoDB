#ifndef CALICO_DB_DATABASE_IMPL_H
#define CALICO_DB_DATABASE_IMPL_H

#include <shared_mutex>
#include <spdlog/spdlog.h>
#include <spdlog/sinks/basic_file_sink.h>
#include "calico/database.h"
#include "page/file_header.h"

namespace calico {

class Cursor;
class IBufferPool;
class IDirectory;
class IFile;
class ITree;

class Database::Impl final {
public:
    struct Parameters {
        std::unique_ptr<IDirectory> directory;
        Options options;
    };

    struct InMemoryTag {};

    explicit Impl(Parameters);
    Impl(Parameters, InMemoryTag);
    ~Impl();
    [[nodiscard]] auto path() const -> std::string;
    [[nodiscard]] auto cache_hit_ratio() const -> double;
    [[nodiscard]] auto record_count() const -> Size;
    [[nodiscard]] auto page_count() const -> Size;
    [[nodiscard]] auto page_size() const -> Size;
    [[nodiscard]] auto uses_transactions() const -> Size;
    [[nodiscard]] auto is_temp() const -> bool;
    auto find(BytesView) -> Cursor;
    auto find_exact(BytesView) -> Cursor;
    auto find_minimum() -> Cursor;
    auto find_maximum() -> Cursor;
    auto insert(BytesView, BytesView) -> bool;
    auto erase(BytesView) -> bool;
    auto erase(Cursor) -> bool;
    auto commit() -> bool;
    auto abort() -> bool;

    auto info() -> Info;

private:
    auto save_header() -> void;
    auto load_header() -> void;
    auto recover() -> void;

    spdlog::sink_ptr m_sink;
    std::shared_ptr<spdlog::logger> m_logger;
    std::unique_ptr<IDirectory> m_directory;
    std::unique_ptr<IBufferPool> m_pool;
    std::unique_ptr<ITree> m_tree;
    bool m_is_temp {};
};

struct InitialState {
    FileHeader state;
    Options revised;
    bool is_new {};
};

auto setup(const std::string&, const Options&, spdlog::logger&) -> InitialState;

} // calico

#endif // CALICO_DB_DATABASE_IMPL_H
