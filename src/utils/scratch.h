
#ifndef CCO_UTILS_SCRATCH_H
#define CCO_UTILS_SCRATCH_H

#include <list>
#include <string>
#include <unordered_map>
#include "types.h"
#include "calico/bytes.h"

namespace cco::utils {

class ScratchManager;

class Scratch final {
public:
    ~Scratch() = default;

    explicit Scratch(Bytes data):
          m_data {data} {}

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

class ScratchManager final {
public:
    explicit ScratchManager(Size scratch_size):
          m_scratch_size {scratch_size} {}

    [[nodiscard]] auto get() -> Scratch;
    auto reset() -> void;

private:
    static constexpr Size MIN_SCRATCH_ID {1};

    std::list<std::string> m_occupied;
    std::list<std::string> m_available;
    Size m_scratch_size;
};

} // cco::utils

#endif // CCO_UTILS_SCRATCH_H
