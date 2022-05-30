#ifndef CUB_DB_DATABASE_IMPL_H
#define CUB_DB_DATABASE_IMPL_H

#include <shared_mutex>
#include "bytes.h"
#include "database.h"

namespace cub {

class FileHeader;
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
        const FileHeader &file_header;
        Size frame_count{};
    };

    explicit Impl(Parameters);
    ~Impl();
    auto lookup(BytesView, bool) -> std::optional<std::string>;
    auto lookup_minimum() -> std::optional<std::string>;
    auto lookup_maximum() -> std::optional<std::string>;
    auto insert(BytesView, BytesView) -> void;
    auto remove(BytesView) -> bool;
    auto commit() -> void;
    auto abort() -> void;
    auto get_cursor() -> Cursor;
    auto get_info() -> Info;

    auto cache_hit_ratio() const -> double;
    auto record_count() const -> Size;
    auto page_count() const -> Size;
    auto transaction_size() const -> Size;

private:
    std::string m_path;
    mutable std::shared_mutex m_mutex;
    std::unique_ptr<IBufferPool> m_pool;
    std::unique_ptr<ITree> m_tree;
    Size m_transaction_size {};
};

} // cub

#endif // CUB_DB_DATABASE_IMPL_H
