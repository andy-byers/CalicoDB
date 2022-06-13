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
class LogSink;

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
    auto read(BytesView, bool) -> std::optional<Record>;
    auto read_minimum() -> std::optional<Record>;
    auto read_maximum() -> std::optional<Record>;
    auto write(BytesView, BytesView) -> bool;
    auto erase(BytesView) -> bool;
    auto commit() -> void;
    auto abort() -> void;
    auto get_iterator() -> Iterator;
    auto get_cursor() -> Cursor;
    auto get_batch() -> Batch;
    auto get_info() -> Info;

    auto cache_hit_ratio() const -> double;
    auto record_count() const -> Size;
    auto page_count() const -> Size;

    auto save_header() -> void;
    auto load_header() -> void;

    auto unlocked_read(BytesView, bool) -> std::optional<Record>; // TODO: Make const
    auto unlocked_read_minimum() -> std::optional<Record>;
    auto unlocked_read_maximum() -> std::optional<Record>;
    auto unlocked_write(BytesView, BytesView) -> bool;
    auto unlocked_erase(BytesView) -> bool;
    auto unlocked_commit() -> bool;
    auto unlocked_abort() -> bool;

private:
    auto recover() -> void;

    std::string m_path;
    mutable std::shared_mutex m_mutex;
    std::unique_ptr<IBufferPool> m_pool;
    std::unique_ptr<ITree> m_tree;
    std::unique_ptr<LogSink> m_log;
};

} // cub

#endif // CUB_DB_DATABASE_IMPL_H
