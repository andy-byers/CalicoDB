#ifndef CUB_DB_DATABASE_IMPL_H
#define CUB_DB_DATABASE_IMPL_H

#include "bytes.h"
#include "database.h"

namespace cub {

class FileHeader;
class IBufferPool;
class ILogFile;
class IReadOnlyFile;
class IReadWriteFile;
class ITree;

class Info {
public:

private:

};

class Database::Impl final {
public:
    struct Parameters {
        std::unique_ptr<IReadWriteFile> database_file;
        std::unique_ptr<IReadOnlyFile> wal_reader_file;
        std::unique_ptr<ILogFile> wal_writer_file;
        const FileHeader &file_header;
        Size frame_count{};
    };

    explicit Impl(Parameters);
    ~Impl() = default;
    auto lookup(BytesView, std::string&) -> bool;
    auto insert(BytesView, BytesView) -> void;
    auto remove(BytesView) -> bool;
    auto get_cursor() -> Cursor;
    auto get_info() -> Info;

private:
    std::unique_ptr<IBufferPool> m_pool;
    std::unique_ptr<ITree> m_tree;
};

} // cub

#endif //CUB_DB_DATABASE_IMPL_H
