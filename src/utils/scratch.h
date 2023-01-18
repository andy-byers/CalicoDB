
#ifndef CALICO_UTILS_SCRATCH_H
#define CALICO_UTILS_SCRATCH_H

#include "calico/slice.h"
#include "types.h"
#include <list>
#include <string>
#include <unordered_map>

namespace Calico {


class Scratch final {
public:
    explicit Scratch(Byte *data, Size size)
        : m_data {data},
          m_size {size},
          m_remove_me {data, size}
    {}

    [[nodiscard]]
    auto size() const -> Size
    {
        return m_size;
    }

    [[nodiscard]]
    auto data() -> Byte *
    {
        return m_data;
    }

    [[nodiscard]]
    auto data() const -> const Byte *
    {
        return m_data;
    }



    explicit Scratch(Span data)
        : m_data {data.data()},
          m_size {data.size()},
          m_remove_me {data}
    {}

    auto operator*() -> Span &
    {
        return m_remove_me;
    }

    auto operator*() const -> const Span &
    {
        return m_remove_me;
    }

    auto operator->() -> Span *
    {
        return &m_remove_me;
    }

private:
    Byte *m_data {};
    Size m_size {};

    Span m_remove_me;
};

class StaticScratch final {
public:
    explicit StaticScratch(Size size)
        : m_data(size, '\x00'),
          m_view {m_data}
    {}

    [[nodiscard]]
    auto size() const -> Size
    {
        return m_data.size();
    }

    [[nodiscard]]
    auto data() -> Byte *
    {
        return m_data.data();
    }

    [[nodiscard]]
    auto data() const -> const Byte *
    {
        return m_data.data();
    }

    [[nodiscard]]
    auto operator*() -> Span
    {
        return m_view;
    }

    [[nodiscard]]
    auto operator*() const -> Slice
    {
        return m_view;
    }

    [[nodiscard]]
    auto operator->() -> Span *
    {
        return &m_view;
    }

private:
    std::string m_data {};
    Span m_view;
};

class MonotonicScratchManager final {
public:
    explicit MonotonicScratchManager(Size scratch_size, Size scratch_count)
        : m_scratch(scratch_size * scratch_count, '\x00'),
          m_scratch_size {scratch_size}
    {}

    [[nodiscard]] auto get() -> Scratch
    {
        auto *data = m_scratch.data() + m_scratch_size*m_counter++;
        m_counter %= m_scratch.size() / m_scratch_size;
        return Scratch {data, m_scratch_size};
    }

private:
    std::string m_scratch;
    Size m_scratch_size;
    Size m_counter {};
};

} // namespace Calico

#endif // CALICO_UTILS_SCRATCH_H
