#ifndef CUB_DB_WRITER_IMPL_H
#define CUB_DB_WRITER_IMPL_H

#include "database_impl.h"
#include "cub/lock.h"
#include <optional>
#include <shared_mutex>
#include <vector>
#include "page/node.h"
#include "utils/types.h"

namespace cub {

class IBufferPool;
class ITree;
struct PID;

class Lock::Impl final {
public:
    explicit Impl(Database::Impl*);
    ~Impl();
    Impl(Impl&&) noexcept = default;
    Impl &operator=(Impl&&) noexcept = default;

private:
    std::unique_lock<std::shared_mutex> m_lock;
    Unique<Database::Impl*> m_db;
};

} // cub

#endif // CUB_DB_WRITER_IMPL_H
