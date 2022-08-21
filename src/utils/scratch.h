
#ifndef CALICO_UTILS_SCRATCH_H
#define CALICO_UTILS_SCRATCH_H

#include "calico/bytes.h"
#include "types.h"
#include <list>
#include <string>
#include <unordered_map>

namespace calico {

class Scratch {
public:
    ~Scratch() = default;

    explicit Scratch(Bytes data)
        : m_data {data}
    {}

    [[nodiscard]] auto size() const -> Size
    {
        return m_data.size();
    }

    [[nodiscard]] auto data() const -> BytesView
    {
        return m_data;
    }

    [[nodiscard]] auto data() -> Bytes
    {
        return m_data;
    }

private:
    Bytes m_data;
};

template<Size N>
class MonotonicScratchManager final {
public:
    static constexpr auto ScratchCount = N;

    explicit MonotonicScratchManager(Size scratch_size)
        : m_scratch(scratch_size * ScratchCount, '\x00'),
          m_scratch_size {scratch_size}
    {}

    [[nodiscard]] auto get() -> Scratch
    {
        auto data = stob(m_scratch).range(m_counter++ * m_scratch_size, m_scratch_size);
        m_counter %= ScratchCount;
        return Scratch {data};
    }

private:
    std::string m_scratch;
    Size m_scratch_size;
    Size m_counter {};
};

class NamedScratch : public Scratch {
public:
    NamedScratch(Size id, Bytes data)
        : Scratch {data},
          m_id {id}
    {}

    [[nodiscard]] auto id() const -> Size
    {
        return m_id;
    }
    
private:
    Size m_id {};
};

class NamedScratchManager final {
public:
    explicit NamedScratchManager(Size scratch_size)
        : m_scratch_size {scratch_size} {}

    [[nodiscard]] auto get() -> NamedScratch;
    auto put(NamedScratch) -> void;

private:
    std::unordered_map<Size, std::string> m_occupied;
    std::list<std::string> m_available;
    Size m_next_id {};
    Size m_scratch_size;
};

} // namespace cco

#endif // CALICO_UTILS_SCRATCH_H
