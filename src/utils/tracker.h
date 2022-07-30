#ifndef CCO_UTILS_TRACKER_H
#define CCO_UTILS_TRACKER_H

#include "identifier.h"
#include "page/update.h"
#include "scratch.h"

namespace cco {

class Tracker final {
public:
    ~Tracker() = default;

    explicit Tracker(Size page_size)
        : m_scratch {page_size}
    {}

    [[nodiscard]] auto collect(Page &, LSN) -> PageUpdate;
    auto reset() -> void;
    auto discard(Page &) -> void;
    auto track(Page &) -> void;

private:
    std::unordered_map<PID, UpdateManager, PID::Hash> m_registry;
    ScratchManager m_scratch;
};

} // namespace cco

#endif // CCO_UTILS_TRACKER_H
