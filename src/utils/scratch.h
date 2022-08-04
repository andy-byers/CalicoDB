
#ifndef CCO_UTILS_SCRATCH_H
#define CCO_UTILS_SCRATCH_H

#include "calico/bytes.h"
#include "types.h"
#include <list>
#include <string>
#include <unordered_map>

namespace cco {

class RollingScratchManager;

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

class RollingScratchManager final {
public:
    static constexpr Size MAXIMUM_SCRATCHES {16};

    explicit RollingScratchManager(Size scratch_size)
        : m_scratches(MAXIMUM_SCRATCHES),
          m_scratch_size {scratch_size}
    {
        for (auto &scratch: m_scratches)
            scratch.resize(scratch_size);
    }

    [[nodiscard]] auto get() -> Scratch;
    auto reset() -> void;

private:
    std::vector<std::string> m_scratches;
    std::list<std::string> m_emergency;
    Size m_scratch_size;
    Index m_counter {};
};

class ManualScratch: public Scratch {
public:
    
    ManualScratch(Index id, Bytes data)
        : Scratch {data},
          m_id {id}
    {}

    [[nodiscard]] auto id() const -> Index
    {
        return m_id;
    }
    
private:
    Index m_id {};
};

class ManualScratchManager final {
public:
    
    explicit ManualScratchManager(Size scratch_size)
        : m_scratch_size {scratch_size} {}

    [[nodiscard]] auto get() -> ManualScratch;
    auto put(ManualScratch) -> void;

private:
    std::unordered_map<Index, std::string> m_occupied;
    std::list<std::string> m_available;
    Index m_next_id {};
    Size m_scratch_size;
};

} // namespace cco

#endif // CCO_UTILS_SCRATCH_H
