#ifndef CUB_LOCK_H
#define CUB_LOCK_H

#include <memory>

namespace cub {

class Lock {
public:
    virtual ~Lock();
    Lock(Lock &&) noexcept = default;
    Lock &operator=(Lock &&) noexcept = default;

private:
    friend class Database;

    Lock();
    class Impl;
    std::unique_ptr<Impl> m_impl;
};

} // cub

#endif // CUB_LOCK_H
