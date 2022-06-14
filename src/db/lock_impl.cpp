#include "lock_impl.h"
#include "page/file_header.h"
#include "pool/interface.h"
#include "tree/interface.h"

namespace cub {

Lock::Impl::~Impl()
{
    if (m_db.value)
        m_db.value->unlock();
}

Lock::Impl::Impl(Database::Impl *db)
    : m_lock {db->lock()}
    , m_db {db}
{
    db->lock();
}

Lock::Lock() = default;

Lock::~Lock() = default;

} // cub