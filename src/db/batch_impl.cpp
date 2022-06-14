#include "batch_impl.h"
#include "page/file_header.h"
#include "pool/interface.h"
#include "tree/interface.h"

namespace cub {

Batch::Impl::Impl(Database::Impl *db, std::shared_mutex &mutex)
    : m_lock {mutex}
    , m_db {db} {}

Batch::Impl::~Impl()
{
    if (m_db.value) {
        try {
            m_db.value->unlocked_commit();
        } catch (...) {
            // TODO: Try to abort here?
        }
    }
}

auto Batch::Impl::read(BytesView key, Comparison comparison) const -> std::optional<Record>
{
    return m_db.value->unlocked_read(key, comparison);
}

auto Batch::Impl::read_minimum() const -> std::optional<Record>
{
    return m_db.value->unlocked_read_minimum();
}

auto Batch::Impl::read_maximum() const -> std::optional<Record>
{
    return m_db.value->unlocked_read_maximum();
}

auto Batch::Impl::write(BytesView key, BytesView value) -> bool
{
    return m_db.value->unlocked_write(key, value);
}

auto Batch::Impl::erase(BytesView key) -> bool
{
    return m_db.value->unlocked_erase(key);
}

auto Batch::Impl::commit() -> void
{
    if (m_db.value->unlocked_commit())
        m_transaction_size = 0;
}

auto Batch::Impl::abort() -> void
{
    if (m_db.value->unlocked_abort())
        m_transaction_size = 0;
}

Batch::Batch() = default;
Batch::~Batch() = default;
Batch::Batch(Batch &&) noexcept = default;
auto Batch::operator=(Batch &&) noexcept -> Batch & = default;

auto Batch::read(BytesView key, Comparison comparison) const -> std::optional<Record>
{
    return m_impl->read(key, comparison);
}

auto Batch::read_minimum() const -> std::optional<Record>
{
    return m_impl->read_minimum();
}

auto Batch::read_maximum() const -> std::optional<Record>
{
    return m_impl->read_maximum();
}

auto Batch::write(BytesView key, BytesView value) -> bool
{
    return m_impl->write(key, value);
}

auto Batch::erase(BytesView key) -> bool
{
    return m_impl->erase(key);
}

auto Batch::commit() -> void
{
    m_impl->commit();
}

auto Batch::abort() -> void
{
    m_impl->abort();
}

} // cub