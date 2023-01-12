
#ifndef CALICO_UTILS_SCRATCH_H
#define CALICO_UTILS_SCRATCH_H

#include "calico/slice.h"
#include "types.h"
#include <list>
#include <string>
#include <unordered_map>

namespace Calico {

class StaticScratch {
public:
    explicit StaticScratch(Size size)
        : m_data(size, '\x00'),
          m_view {m_data}
    {}

    [[nodiscard]]
    auto operator*() -> Bytes
    {
        return m_view;
    }

    [[nodiscard]]
    auto operator*() const -> Slice
    {
        return m_view;
    }

    [[nodiscard]]
    auto operator->() -> Bytes *
    {
        return &m_view;
    }

private:
    std::string m_data {};
    Bytes m_view;
};

class Scratch {
public:
    ~Scratch() = default;

    explicit Scratch(Bytes data)
        : m_data {data}
    {}

    auto operator*() -> Bytes&
    {
        return m_data;
    }

    auto operator*() const -> const Bytes&
    {
        return m_data;
    }

    auto operator->() -> Bytes*
    {
        return &m_data;
    }

private:
    Bytes m_data;
};

class MonotonicScratchManager final {
public:
    explicit MonotonicScratchManager(Size scratch_size, Size scratch_count)
        : m_scratch(scratch_size * scratch_count, '\x00'),
          m_scratch_size {scratch_size}
    {}

    [[nodiscard]] auto get() -> Scratch
    {
        auto data = Bytes {m_scratch}.range(m_counter++ * m_scratch_size, m_scratch_size);
        m_counter %= m_scratch.size() / m_scratch_size;
        return Scratch {data};
    }

private:
    std::string m_scratch;
    Size m_scratch_size;
    Size m_counter {};
};

} // namespace Calico

#endif // CALICO_UTILS_SCRATCH_H
