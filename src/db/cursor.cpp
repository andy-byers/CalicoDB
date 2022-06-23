#include "cursor_impl.h"

namespace calico {

Cursor::Cursor() = default;

Cursor::~Cursor() = default;

Cursor::Cursor(Cursor &&) noexcept = default;

auto Cursor::operator=(Cursor &&) noexcept -> Cursor & = default;

auto Cursor::has_record() const -> bool
{
    return m_impl->has_record();
}

auto Cursor::is_minimum() const -> bool
{
    return m_impl->is_minimum();
}

auto Cursor::is_maximum() const -> bool
{
    return m_impl->is_maximum();
}

auto Cursor::key() const -> BytesView
{
    return m_impl->key();
}

auto Cursor::value() const -> std::string
{
    return m_impl->value();
}

auto Cursor::record() const -> Record
{
    return {btos(m_impl->key()), m_impl->value()};
}

auto Cursor::reset() -> void
{
    m_impl->reset();
}

auto Cursor::increment() -> bool
{
    return m_impl->increment();
}

auto Cursor::increment(Size n) -> Size
{
    Size count {};
    while (count < n && m_impl->increment())
        count++;
    return count;
}

auto Cursor::decrement() -> bool
{
    return m_impl->decrement();
}

auto Cursor::decrement(Size n) -> Size
{
    Size count {};
    while (count < n && m_impl->decrement())
        count++;
    return count;
}

auto Cursor::find(BytesView key) -> bool
{
    return m_impl->find(key);
}

auto Cursor::find_minimum() -> void
{
    m_impl->find_minimum();
}

auto Cursor::find_maximum() -> void
{
    m_impl->find_maximum();
}

} // calico
