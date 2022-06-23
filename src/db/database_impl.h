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
class ILogFile;
class IReadOnlyFile;
class IReadWriteFile;
class ITree;

class Database::Impl final {
public:
    struct Parameters {
        std::string path;
        std::unique_ptr<IReadWriteFile> database_file;
        std::unique_ptr<IReadOnlyFile> wal_reader_file;
        std::unique_ptr<ILogFile> wal_writer_file;
        FileHeader header;
        Options options;
    };

    struct InMemoryTag {};

    explicit Impl(Parameters);
    Impl(Parameters, InMemoryTag);
    ~Impl();
    [[nodiscard]] auto cache_hit_ratio() const -> double;
    [[nodiscard]] auto record_count() const -> Size;
    [[nodiscard]] auto page_count() const -> Size;
    [[nodiscard]] auto page_size() const -> Size;
    [[nodiscard]] auto uses_transactions() const -> Size;
    [[nodiscard]] auto is_temp() const -> bool;
    auto read(BytesView, Ordering) -> std::optional<Record>;
    auto read_minimum() -> std::optional<Record>;
    auto read_maximum() -> std::optional<Record>;
    auto write(BytesView, BytesView) -> bool;
    auto erase(BytesView) -> bool;
    auto commit() -> bool;
    auto abort() -> bool;
    auto get_cursor() -> Cursor;
    auto get_info() -> Info;

    [[nodiscard]] auto path() const -> const std::string&
    {
        return m_path;
    }

private:
    auto save_header() -> void;
    auto load_header() -> void;
    auto recover() -> void;

    spdlog::sink_ptr m_sink;
    std::shared_ptr<spdlog::logger> m_logger;
    std::string m_path;
    std::unique_ptr<IBufferPool> m_pool;
    std::unique_ptr<ITree> m_tree;
    bool m_is_temp {};
};

struct InitialState {
    FileHeader header;
    bool uses_transactions {};
};

struct OpenFiles {
    std::unique_ptr<IReadWriteFile> tree_file;
    std::unique_ptr<IReadOnlyFile> wal_reader_file;
    std::unique_ptr<ILogFile> wal_writer_file;
};

auto get_initial_state(const std::string&, const Options&) -> InitialState;
auto get_open_files(const std::string&, const Options&) -> OpenFiles;

} // calico

#endif // CALICO_DB_DATABASE_IMPL_H
