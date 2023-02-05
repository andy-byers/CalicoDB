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
    explicit Scratch(Span data)
        : m_data {data}
    {}

    explicit Scratch(Byte *data, Size size)
        : m_data {data, size}
    {}

    auto operator*() -> Span &
    {
        return m_data;
    }

    auto operator->() -> Span *
    {
        return &m_data;
    }

private:
    Span m_data;
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
    auto operator*() -> Span
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
    explicit MonotonicScratchManager(Size chunk_size, Size chunk_count)
        : m_scratch_data(chunk_size * chunk_count, '\x00'),
          m_chunk_size {chunk_size}
    {}

    [[nodiscard]] auto get() -> Scratch
    {
        auto *data = m_scratch_data.data() + m_chunk_size*m_counter++;
        m_counter %= m_scratch_data.size() / m_chunk_size;
        return Scratch {data, m_chunk_size};
    }

private:
    std::string m_scratch_data;
    Size m_chunk_size;
    Size m_counter {};
};

} // namespace Calico

#endif // CALICO_UTILS_SCRATCH_H
