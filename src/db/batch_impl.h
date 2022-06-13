#ifndef CUB_DB_WRITER_IMPL_H
#define CUB_DB_WRITER_IMPL_H

#include "database_impl.h"
#include "cub/batch.h"
#include <optional>
#include <shared_mutex>
#include <vector>
#include "page/node.h"
#include "utils/types.h"

namespace cub {

class IBufferPool;
class ITree;
struct PID;

class Batch::Impl {
public:
    Impl(Database::Impl*, std::shared_mutex&);
    ~Impl();
    auto read(BytesView, bool) const -> std::optional<Record>;
    auto read_minimum() const -> std::optional<Record>;
    auto read_maximum() const -> std::optional<Record>;
    auto write(BytesView, BytesView) -> bool;
    auto erase(BytesView) -> bool;
    auto commit() -> void;
    auto abort() -> void;

    Impl(Impl&&) = default;
    Impl &operator=(Impl&&) = default;

private:
    std::unique_lock<std::shared_mutex> m_lock;
    Unique<Database::Impl*> m_db;
    Size m_transaction_size {};
};

} // cub

#endif // CUB_DB_WRITER_IMPL_H
