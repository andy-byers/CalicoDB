#ifndef CUB_DB_DATABASE_IMPL_H
#define CUB_DB_DATABASE_IMPL_H

#include <shared_mutex>
#include "cub/database.h"

namespace cub {

class Batch;
class Cursor;
class FileHeader;
class Iterator;
class IBufferPool;
class ILogFile;
class IReadOnlyFile;
class IReadWriteFile;
class ITree;
class Cursor;
class Batch;
class Lock;

class Database::Impl final {
public:
    struct Parameters {
        std::string path;
        std::unique_ptr<IReadWriteFile> database_file;
        std::unique_ptr<IReadOnlyFile> wal_reader_file;
        std::unique_ptr<ILogFile> wal_writer_file;
        const FileHeader &file_header;
        Size frame_count{};
    };

    explicit Impl(Size);
    explicit Impl(Parameters);
    ~Impl();
    [[nodiscard]] auto cache_hit_ratio() const -> double;
    [[nodiscard]] auto record_count() const -> Size;
    [[nodiscard]] auto page_count() const -> Size;
    [[nodiscard]] auto page_size() const -> Size;

    [[nodiscard]] auto path() const -> const std::string&
    {
        return m_path;
    }

    auto read(BytesView, Ordering) -> std::optional<Record>;
    auto read_minimum() -> std::optional<Record>;
    auto read_maximum() -> std::optional<Record>;
    auto write(BytesView, BytesView) -> bool;
    auto erase(BytesView) -> bool;
    auto commit() -> bool;
    auto abort() -> bool;
    auto get_cursor() -> Cursor;
    auto get_info() -> Info;

private:
    auto save_header() -> void;
    auto load_header() -> void;
    auto recover() -> void;

    std::string m_path;
    std::unique_ptr<IBufferPool> m_pool;
    std::unique_ptr<ITree> m_tree;
};

} // cub

#endif // CUB_DB_DATABASE_IMPL_H
